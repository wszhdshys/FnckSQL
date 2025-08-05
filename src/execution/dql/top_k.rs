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
    mut indices: BinaryHeap<(Vec<u8>, usize)>,
    mut tuples: HashMap<usize, Tuple>,
) -> Vec<Tuple> {
    let mut result = Vec::with_capacity(indices.len());
    while let Some((_, index)) = indices.pop() {
        result.push(tuples.remove(&index).unwrap());
    }
    result.reverse();
    result
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

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::errors::DatabaseError;
    use crate::execution::dql::top_k::{final_sort, top_sort};
    use crate::expression::ScalarExpression;
    use crate::planner::operator::sort::SortField;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use ahash::{HashMap, HashMapExt};
    use bumpalo::Bump;
    use std::collections::BinaryHeap;
    use std::sync::Arc;

    #[test]
    fn test_top_k_sort() -> Result<(), DatabaseError> {
        let fn_sort_fields = |asc: bool, nulls_first: bool| {
            vec![SortField {
                expr: ScalarExpression::Reference {
                    expr: Box::new(ScalarExpression::Empty),
                    pos: 0,
                },
                asc,
                nulls_first,
            }]
        };
        let schema = Arc::new(vec![ColumnRef::from(ColumnCatalog::new(
            "c1".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        ))]);

        let arena = Bump::new();

        let fn_asc_and_nulls_last_eq = |mut iter: Box<dyn Iterator<Item = Tuple>>| {
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Int32(0)])
            } else {
                unreachable!()
            }
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Int32(1)])
            } else {
                unreachable!()
            }
        };
        let fn_desc_and_nulls_last_eq = |mut iter: Box<dyn Iterator<Item = Tuple>>| {
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Int32(1)])
            } else {
                unreachable!()
            }
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Int32(0)])
            } else {
                unreachable!()
            }
        };
        let fn_asc_and_nulls_first_eq = |mut iter: Box<dyn Iterator<Item = Tuple>>| {
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Null])
            } else {
                unreachable!()
            }
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Int32(0)])
            } else {
                unreachable!()
            }
        };
        let fn_desc_and_nulls_first_eq = |mut iter: Box<dyn Iterator<Item = Tuple>>| {
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Null])
            } else {
                unreachable!()
            }
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Int32(1)])
            } else {
                unreachable!()
            }
        };

        let mut indices: BinaryHeap<(Vec<u8>, usize)> = BinaryHeap::with_capacity(2);
        let mut tuples: HashMap<usize, Tuple> = HashMap::with_capacity(2);
        top_sort(
            &arena,
            &schema,
            &*fn_sort_fields(true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Null]),
            2,
            0,
        )?;
        top_sort(
            &arena,
            &schema,
            &*fn_sort_fields(true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(0)]),
            2,
            1,
        )?;
        top_sort(
            &arena,
            &schema,
            &*fn_sort_fields(true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(1)]),
            2,
            2,
        )?;
        fn_asc_and_nulls_first_eq(Box::new(final_sort(indices, tuples).into_iter()));

        let mut indices: BinaryHeap<(Vec<u8>, usize)> = BinaryHeap::with_capacity(2);
        let mut tuples: HashMap<usize, Tuple> = HashMap::with_capacity(2);
        top_sort(
            &arena,
            &schema,
            &*fn_sort_fields(true, false),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Null]),
            2,
            0,
        )?;
        top_sort(
            &arena,
            &schema,
            &*fn_sort_fields(true, false),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(0)]),
            2,
            1,
        )?;
        top_sort(
            &arena,
            &schema,
            &*fn_sort_fields(true, false),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(1)]),
            2,
            2,
        )?;
        fn_asc_and_nulls_last_eq(Box::new(final_sort(indices, tuples).into_iter()));

        let mut indices: BinaryHeap<(Vec<u8>, usize)> = BinaryHeap::with_capacity(2);
        let mut tuples: HashMap<usize, Tuple> = HashMap::with_capacity(2);
        top_sort(
            &arena,
            &schema,
            &*fn_sort_fields(false, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Null]),
            2,
            0,
        )?;
        top_sort(
            &arena,
            &schema,
            &*fn_sort_fields(false, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(0)]),
            2,
            1,
        )?;
        top_sort(
            &arena,
            &schema,
            &*fn_sort_fields(false, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(1)]),
            2,
            2,
        )?;
        fn_desc_and_nulls_first_eq(Box::new(final_sort(indices, tuples).into_iter()));

        let mut indices: BinaryHeap<(Vec<u8>, usize)> = BinaryHeap::with_capacity(2);
        let mut tuples: HashMap<usize, Tuple> = HashMap::with_capacity(2);
        top_sort(
            &arena,
            &schema,
            &*fn_sort_fields(false, false),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Null]),
            2,
            0,
        )?;
        top_sort(
            &arena,
            &schema,
            &*fn_sort_fields(false, false),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(0)]),
            2,
            1,
        )?;
        top_sort(
            &arena,
            &schema,
            &*fn_sort_fields(false, false),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(1)]),
            2,
            2,
        )?;
        fn_desc_and_nulls_last_eq(Box::new(final_sort(indices, tuples).into_iter()));

        Ok(())
    }

    #[test]
    fn test_top_k_sort_mix_values() -> Result<(), DatabaseError> {
        let fn_sort_fields =
            |asc_1: bool, nulls_first_1: bool, asc_2: bool, nulls_first_2: bool| {
                vec![
                    SortField {
                        expr: ScalarExpression::Reference {
                            expr: Box::new(ScalarExpression::Empty),
                            pos: 0,
                        },
                        asc: asc_1,
                        nulls_first: nulls_first_1,
                    },
                    SortField {
                        expr: ScalarExpression::Reference {
                            expr: Box::new(ScalarExpression::Empty),
                            pos: 0,
                        },
                        asc: asc_2,
                        nulls_first: nulls_first_2,
                    },
                ]
            };
        let schema = Arc::new(vec![
            ColumnRef::from(ColumnCatalog::new(
                "c1".to_string(),
                true,
                ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c2".to_string(),
                true,
                ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
            )),
        ]);
        let arena = Bump::new();

        let fn_asc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq =
            |mut iter: Box<dyn Iterator<Item = Tuple>>| {
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Null, DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Null, DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(0), DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(0), DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
            };
        let fn_asc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq =
            |mut iter: Box<dyn Iterator<Item = Tuple>>| {
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(0), DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(0), DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(1), DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(1), DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
            };
        let fn_desc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq =
            |mut iter: Box<dyn Iterator<Item = Tuple>>| {
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Null, DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Null, DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(1), DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(1), DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
            };
        let fn_desc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq =
            |mut iter: Box<dyn Iterator<Item = Tuple>>| {
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(1), DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(1), DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(0), DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(0), DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
            };

        let mut indices: BinaryHeap<(Vec<u8>, usize)> = BinaryHeap::with_capacity(4);
        let mut tuples: HashMap<usize, Tuple> = HashMap::with_capacity(4);
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            4,
            0,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            4,
            1,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            4,
            2,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            4,
            3,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            4,
            5,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
            4,
            6,
        )?;
        fn_asc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(Box::new(
            final_sort(indices, tuples).into_iter(),
        ));

        let mut indices: BinaryHeap<(Vec<u8>, usize)> = BinaryHeap::with_capacity(4);
        let mut tuples: HashMap<usize, Tuple> = HashMap::with_capacity(4);
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            4,
            0,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            4,
            1,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            4,
            2,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            4,
            3,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            4,
            5,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
            4,
            6,
        )?;
        fn_asc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(Box::new(
            final_sort(indices, tuples).into_iter(),
        ));

        let mut indices: BinaryHeap<(Vec<u8>, usize)> = BinaryHeap::with_capacity(4);
        let mut tuples: HashMap<usize, Tuple> = HashMap::with_capacity(4);
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            4,
            0,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            4,
            1,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            4,
            2,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            4,
            3,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            4,
            5,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
            4,
            6,
        )?;
        fn_desc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(Box::new(
            final_sort(indices, tuples).into_iter(),
        ));

        let mut indices: BinaryHeap<(Vec<u8>, usize)> = BinaryHeap::with_capacity(4);
        let mut tuples: HashMap<usize, Tuple> = HashMap::with_capacity(4);
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            4,
            0,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            4,
            1,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            4,
            2,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            4,
            3,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            4,
            5,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            &mut tuples,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
            4,
            6,
        )?;
        fn_desc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(Box::new(
            final_sort(indices, tuples).into_iter(),
        ));

        Ok(())
    }
}
