use crate::errors::DatabaseError;
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::pattern::PatternChildrenPredicate;
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::planner::operator::top_k::TopKOperator;
use crate::planner::operator::Operator;
use std::sync::LazyLock;

static TOP_K_RULE: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::Limit(_)),
    children: PatternChildrenPredicate::Predicate(vec![Pattern {
        predicate: |op| matches!(op, Operator::Sort(_)),
        children: PatternChildrenPredicate::None,
    }]),
});

pub struct TopK;

impl MatchPattern for TopK {
    fn pattern(&self) -> &Pattern {
        &TOP_K_RULE
    }
}

impl NormalizationRule for TopK {
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError> {
        if let Operator::Limit(op) = graph.operator(node_id) {
            if let Some(limit) = op.limit {
                let sort_id = graph.eldest_child_at(node_id).unwrap();
                if let Operator::Sort(sort_op) = graph.operator(sort_id) {
                    graph.replace_node(
                        node_id,
                        Operator::TopK(TopKOperator {
                            sort_fields: sort_op.sort_fields.clone(),
                            limit,
                            offset: op.offset,
                        }),
                    );
                    graph.remove_node(sort_id, false);
                }
            }
        }
        Ok(())
    }
}
