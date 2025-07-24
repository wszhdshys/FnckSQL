use crate::execution::{build_read, Executor, ReadExecutor};
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use ahash::{HashSet, HashSetExt};
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

pub struct Except {
    left_input: LogicalPlan,
    right_input: LogicalPlan,
}

impl From<(LogicalPlan, LogicalPlan)> for Except {
    fn from((left_input, right_input): (LogicalPlan, LogicalPlan)) -> Self {
        Except {
            left_input,
            right_input,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Except {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let Except {
                    left_input,
                    right_input,
                } = self;

                let mut coroutine = build_read(right_input, cache, transaction);

                let mut except_col = HashSet::new();

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let tuple = throw!(tuple);
                    except_col.insert(tuple);
                }

                let mut coroutine = build_read(left_input, cache, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let tuple = throw!(tuple);
                    if !except_col.contains(&tuple) {
                        yield Ok(tuple);
                    }
                }
            },
        )
    }
}
