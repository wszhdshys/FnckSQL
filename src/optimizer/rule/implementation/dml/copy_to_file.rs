use crate::errors::DatabaseError;
use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::single_mapping;
use crate::storage::Transaction;
use std::sync::LazyLock;

static COPY_TO_FILE_PATTERN: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::CopyToFile(_)),
    children: PatternChildrenPredicate::None,
});

#[derive(Clone)]
pub struct CopyToFileImplementation;

single_mapping!(
    CopyToFileImplementation,
    COPY_TO_FILE_PATTERN,
    PhysicalOption::CopyToFile
);
