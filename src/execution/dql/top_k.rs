use crate::errors::DatabaseError;
use crate::execution::dql::sort::{BumpVec, NullableVec, RemappingIterator};
use crate::execution::{build_read, Executor, ReadExecutor};
use crate::planner::operator::sort::SortField;
use crate::planner::operator::top_k::TopKOperator;
use crate::planner::LogicalPlan;
use crate::storage::table_codec::BumpBytes;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::{Schema, Tuple};
use bumpalo::Bump;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

fn top_sort<'a>(
    arena: &'a Bump,
    schema: &Schema,
    sort_fields: &[SortField],
    tuples: NullableVec<'a, (usize, Tuple)>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<Box<dyn Iterator<Item = Tuple> + 'a>, DatabaseError> {
    let mut sort_keys = BumpVec::with_capacity_in(tuples.len(), arena);
    for (i, tuple) in tuples.0.iter().enumerate() {
        let mut full_key = BumpVec::new_in(arena);
        for SortField {
            expr,
            nulls_first,
            asc,
        } in sort_fields
        {
            let mut key = BumpBytes::new_in(arena);
            let tuple = tuple.as_ref().map(|(_, tuple)| tuple).unwrap();
            expr.eval(Some((tuple, &**schema)))?
                .memcomparable_encode(&mut key)?;
            if *asc {
                for byte in key.iter_mut() {
                    *byte ^= 0xFF;
                }
            }
            key.push(if *nulls_first { u8::MIN } else { u8::MAX });
            full_key.extend(key);
        }
        //full_key.extend_from_slice(&(i as u64).to_be_bytes());
        sort_keys.push((i, full_key))
    }

    let keep_count = offset.unwrap_or(0) + limit.unwrap_or(sort_keys.len());

    let mut heap: BinaryHeap<Reverse<(&[u8], usize)>> = BinaryHeap::with_capacity(keep_count);
    for (i, key) in sort_keys.iter() {
        let key = key.as_slice();
        if heap.len() < keep_count {
            heap.push(Reverse((key, *i)));
        } else if let Some(&Reverse((min_key, _))) = heap.peek() {
            if key > min_key {
                heap.pop();
                heap.push(Reverse((key, *i)));
            }
        }
    }

    let mut topk: Vec<(Vec<u8>, usize)> = heap
        .into_iter()
        .map(|Reverse((key, i))| (key.to_vec(), i))
        .collect();
    topk.sort_by(|(k1, i1), (k2, i2)| k1.cmp(k2).then_with(|| i1.cmp(i2).reverse()));
    topk.reverse();

    let mut bumped_indices =
        BumpVec::with_capacity_in(topk.len().saturating_sub(offset.unwrap_or(0)), arena);
    for (_, idx) in topk.into_iter().skip(offset.unwrap_or(0)) {
        bumped_indices.push(idx);
    }
    Ok(Box::new(RemappingIterator::new(0, tuples, bumped_indices)))
}

pub struct TopK {
    arena: Bump,
    sort_fields: Vec<SortField>,
    limit: Option<usize>,
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

                let mut tuples = NullableVec::new(unsafe { &*arena });
                let schema = input.output_schema().clone();
                let mut tuple_offset = 0;

                let mut coroutine = build_read(input, cache, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    tuples.put((tuple_offset, throw!(tuple)));
                    tuple_offset += 1;
                }

                for tuple in throw!(top_sort(
                    unsafe { &*arena },
                    &schema,
                    &sort_fields,
                    tuples,
                    limit,
                    offset
                )) {
                    yield Ok(tuple)
                }
            },
        )
    }
}
