use crate::catalog::{ColumnRef, TableName};
use crate::errors::DatabaseError;
use crate::expression::visitor::Visitor;
use crate::expression::HasCountStar;
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::Operator;
use crate::planner::operator::Operator::{Join, TableScan};
use crate::types::index::IndexInfo;
use crate::types::ColumnId;
use itertools::Itertools;
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

static CORRELATED_SUBQUERY_RULE: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Join(_)),
    children: PatternChildrenPredicate::None,
});

#[derive(Clone)]
pub struct CorrelatedSubquery;

macro_rules! trans_references {
    ($columns:expr) => {{
        let mut column_references = HashSet::with_capacity($columns.len());
        for column in $columns {
            column_references.insert(column);
        }
        column_references
    }};
}

impl CorrelatedSubquery {
    fn _apply(
        column_references: HashSet<&ColumnRef>,
        scan_columns: HashMap<TableName, (Vec<ColumnId>, HashMap<ColumnId, usize>, Vec<IndexInfo>)>,
        node_id: HepNodeId,
        graph: &mut HepGraph,
    ) -> Result<
        HashMap<TableName, (Vec<ColumnId>, HashMap<ColumnId, usize>, Vec<IndexInfo>)>,
        DatabaseError,
    > {
        let operator = &graph.operator(node_id).clone();

        match operator {
            Operator::Aggregate(op) => {
                let is_distinct = op.is_distinct;
                let referenced_columns = operator.referenced_columns(false);
                let mut new_column_references = trans_references!(&referenced_columns);
                // on distinct
                if is_distinct {
                    for summary in column_references {
                        new_column_references.insert(summary);
                    }
                }

                Self::recollect_apply(new_column_references, scan_columns, node_id, graph)
            }
            Operator::Project(op) => {
                let mut has_count_star = HasCountStar::default();
                for expr in &op.exprs {
                    has_count_star.visit(expr)?;
                }
                let referenced_columns = operator.referenced_columns(false);
                let new_column_references = trans_references!(&referenced_columns);

                Self::recollect_apply(new_column_references, scan_columns, node_id, graph)
            }
            Operator::TableScan(op) => {
                let table_column: HashSet<&ColumnRef> = op.columns.values().collect();
                let mut new_scan_columns = scan_columns.clone();
                new_scan_columns.insert(
                    op.table_name.clone(),
                    (
                        op.primary_keys.clone(),
                        op.columns
                            .iter()
                            .map(|(num, col)| (col.id().unwrap(), *num))
                            .collect(),
                        op.index_infos.clone(),
                    ),
                );
                let mut parent_col = HashMap::new();
                for col in column_references {
                    match (
                        table_column.contains(col),
                        scan_columns.get(col.table_name().unwrap_or(&Arc::new("".to_string()))),
                    ) {
                        (false, Some(..)) => {
                            parent_col
                                .entry(col.table_name().unwrap())
                                .or_insert(HashSet::new())
                                .insert(col);
                        }
                        _ => continue,
                    }
                }
                for (table_name, table_columns) in parent_col {
                    let table_columns = table_columns.into_iter().collect_vec();
                    let (primary_keys, columns, index_infos) =
                        scan_columns.get(table_name).unwrap();
                    let map: BTreeMap<usize, ColumnRef> = table_columns
                        .into_iter()
                        .map(|col| (*columns.get(&col.id().unwrap()).unwrap(), col.clone()))
                        .collect();
                    let left_operator = graph.operator(node_id).clone();
                    let right_operator = TableScan(TableScanOperator {
                        table_name: table_name.clone(),
                        primary_keys: primary_keys.clone(),
                        columns: map,
                        limit: (None, None),
                        index_infos: index_infos.clone(),
                        with_pk: false,
                    });
                    let join_operator = Join(JoinOperator {
                        on: JoinCondition::None,
                        join_type: JoinType::Cross,
                    });

                    match &left_operator {
                        TableScan(_) => {
                            graph.replace_node(node_id, join_operator);
                            graph.add_node(node_id, None, left_operator);
                            graph.add_node(node_id, None, right_operator);
                        }
                        Join(_) => {
                            let left_id = graph.eldest_child_at(node_id).unwrap();
                            let left_id = graph.add_node(node_id, Some(left_id), join_operator);
                            graph.add_node(left_id, None, right_operator);
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(new_scan_columns)
            }
            Operator::Sort(_) | Operator::Limit(_) | Operator::Filter(_) | Operator::Union(_) => {
                let mut new_scan_columns = scan_columns.clone();
                let temp_columns = operator.referenced_columns(false);
                // why?
                let mut column_references = column_references;
                for column in temp_columns.iter() {
                    column_references.insert(column);
                }
                for child_id in graph.children_at(node_id).collect_vec() {
                    let copy_references = column_references.clone();
                    let copy_scan = scan_columns.clone();
                    if let Ok(scan) = Self::_apply(copy_references, copy_scan, child_id, graph) {
                        new_scan_columns.extend(scan);
                    };
                }
                Ok(new_scan_columns)
            }
            Operator::Join(_) => {
                let mut new_scan_columns = scan_columns.clone();
                for child_id in graph.children_at(node_id).collect_vec() {
                    let copy_references = column_references.clone();
                    let copy_scan = new_scan_columns.clone();
                    if let Ok(scan) = Self::_apply(copy_references, copy_scan, child_id, graph) {
                        new_scan_columns.extend(scan);
                    };
                }
                Ok(new_scan_columns)
            }
            // Last Operator
            Operator::Dummy | Operator::Values(_) | Operator::FunctionScan(_) => Ok(scan_columns),
            Operator::Explain => {
                if let Some(child_id) = graph.eldest_child_at(node_id) {
                    Self::_apply(column_references, scan_columns, child_id, graph)
                } else {
                    unreachable!()
                }
            }
            // DDL Based on Other Plan
            Operator::Insert(_)
            | Operator::Update(_)
            | Operator::Delete(_)
            | Operator::Analyze(_) => {
                let referenced_columns = operator.referenced_columns(false);
                let new_column_references = trans_references!(&referenced_columns);

                if let Some(child_id) = graph.eldest_child_at(node_id) {
                    Self::recollect_apply(new_column_references, scan_columns, child_id, graph)
                } else {
                    unreachable!();
                }
            }
            // DDL Single Plan
            Operator::CreateTable(_)
            | Operator::CreateIndex(_)
            | Operator::CreateView(_)
            | Operator::DropTable(_)
            | Operator::DropView(_)
            | Operator::Truncate(_)
            | Operator::ShowTable
            | Operator::ShowView
            | Operator::CopyFromFile(_)
            | Operator::CopyToFile(_)
            | Operator::AddColumn(_)
            | Operator::DropColumn(_)
            | Operator::Describe(_) => Ok(scan_columns),
        }
    }

    fn recollect_apply(
        referenced_columns: HashSet<&ColumnRef>,
        scan_columns: HashMap<TableName, (Vec<ColumnId>, HashMap<ColumnId, usize>, Vec<IndexInfo>)>,
        node_id: HepNodeId,
        graph: &mut HepGraph,
    ) -> Result<
        HashMap<TableName, (Vec<ColumnId>, HashMap<ColumnId, usize>, Vec<IndexInfo>)>,
        DatabaseError,
    > {
        let mut new_scan_columns = scan_columns.clone();
        for child_id in graph.children_at(node_id).collect_vec() {
            let copy_references = referenced_columns.clone();
            let copy_scan = scan_columns.clone();

            if let Ok(scan) = Self::_apply(copy_references, copy_scan, child_id, graph) {
                new_scan_columns.extend(scan);
            };
        }
        Ok(new_scan_columns)
    }
}

impl MatchPattern for CorrelatedSubquery {
    fn pattern(&self) -> &Pattern {
        &CORRELATED_SUBQUERY_RULE
    }
}

impl NormalizationRule for CorrelatedSubquery {
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError> {
        Self::_apply(HashSet::new(), HashMap::new(), node_id, graph)?;
        // mark changed to skip this rule batch
        graph.version += 1;

        Ok(())
    }
}
