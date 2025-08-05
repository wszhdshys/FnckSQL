use crate::errors::DatabaseError;
use crate::execution::{build_read, Executor, ReadExecutor};
use crate::planner::operator::sort::SortField;
use crate::planner::operator::top_k::TopKOperator;
use crate::planner::LogicalPlan;
use crate::storage::table_codec::BumpBytes;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::{Schema, Tuple};
use ahash::{HashMap, HashMapExt};
use bumpalo::Bump;
use std::collections::BinaryHeap;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

fn top_sort<'a>(
    arena: &'a Bump,
    schema: &Schema,
    sort_fields: &[SortField],
    indices: &mut BinaryHeap<(Vec<u8>, usize)>,
    tuples: &mut HashMap<usize, Tuple>,
    tuple: Tuple,
    keep_count: usize,
    index: usize,
) -> Result<(), DatabaseError> {
    let mut full_key = vec![];
    for SortField {
        expr,
        nulls_first,
        asc,
    } in sort_fields
    {
        let mut key = BumpBytes::new_in(arena);
        expr.eval(Some((&tuple, &**schema)))?
            .memcomparable_encode(&mut key)?;
        if !asc {
            for byte in key.iter_mut() {
                *byte ^= 0xFF;
            }
        }
        key.push(if *nulls_first { u8::MIN } else { u8::MAX });
        full_key.extend(key);
    }

    if indices.len() < keep_count {
        indices.push((full_key, index));
        tuples.insert(index, tuple);
    } else if let Some((min_key, i)) = indices.peek() {
        let pop_index = *i;
        if full_key.as_slice() < min_key.as_slice() {
            indices.pop();
            indices.push((full_key, index));
            tuples.remove(&pop_index);
            tuples.insert(index, tuple);
        }
    }
    Ok(())
}

fn final_sort(
    indices: BinaryHeap<(Vec<u8>, usize)>,
    mut tuples: HashMap<usize, Tuple>,
) -> Vec<Tuple> {
    let mut topk: Vec<(Vec<u8>, usize)> = indices.into_iter().map(|(key, i)| (key, i)).collect();
    topk.sort_by(|(k1, i1), (k2, i2)| k2.cmp(k1).then_with(|| i1.cmp(i2).reverse()));
    topk.reverse();
    Vec::from(
        topk.into_iter()
            .map(|(_, index)| tuples.remove(&index).unwrap())
            .collect::<Vec<_>>(),
    )
}

pub struct TopK {
    arena: Bump,
    sort_fields: Vec<SortField>,
    limit: usize,
    offset: Option<usize>,
    input: LogicalPlan,
}

impl From<(TopKOperator, LogicalPlan)> for TopK {
    fn from(
        (
            TopKOperator {
                sort_fields,
                limit,
                offset,
            },
            input,
        ): (TopKOperator, LogicalPlan),
    ) -> Self {
        TopK {
            arena: Default::default(),
            sort_fields,
            limit,
            offset,
            input,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for TopK {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let TopK {
                    arena,
                    sort_fields,
                    limit,
                    offset,
                    mut input,
                } = self;

                let arena: *const Bump = &arena;

                let schema = input.output_schema().clone();
                let keep_count = offset.unwrap_or(0) + limit;
                let mut indices: BinaryHeap<(Vec<u8>, usize)> =
                    BinaryHeap::with_capacity(keep_count);
                let mut tuples: HashMap<usize, Tuple> = HashMap::with_capacity(keep_count);

                let mut coroutine = build_read(input, cache, transaction);

                let mut i: usize = 0;
                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    throw!(top_sort(
                        unsafe { &*arena },
                        &schema,
                        &sort_fields,
                        &mut indices,
                        &mut tuples,
                        throw!(tuple),
                        keep_count,
                        i
                    ));
                    i += 1;
                }

                i = 0;
                for tuple in final_sort(indices, tuples) {
                    i += 1;
                    if i - 1 < offset.unwrap_or(0) {
                        continue;
                    }
                    yield Ok(tuple);
                }
            },
        )
    }
}
