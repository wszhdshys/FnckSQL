use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::function::scala::FuncMonotonicity;
use crate::expression::function::scala::ScalarFunctionImpl;
use crate::expression::function::FunctionSummary;
use crate::expression::ScalarExpression;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use chrono::{Local, Offset, Utc};
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CurrentTimeStamp {
    summary: FunctionSummary,
}

impl CurrentTimeStamp {
    #[allow(unused_mut)]
    pub(crate) fn new(function_name: String) -> Arc<Self> {
        Arc::new(Self {
            summary: FunctionSummary {
                name: function_name,
                arg_types: Vec::new(),
            },
        })
    }
}

#[typetag::serde]
impl ScalarFunctionImpl for CurrentTimeStamp {
    #[allow(unused_variables, clippy::redundant_closure_call)]
    fn eval(
        &self,
        _: &[ScalarExpression],
        _: Option<(&Tuple, &[ColumnRef])>,
    ) -> Result<DataValue, DatabaseError> {
        if self.summary.name == "current_timestamp" {
            Ok(DataValue::Time64(Utc::now().with_timezone(&Local::now().offset().fix()).timestamp(), 0, false))
        } else if self.summary.name == "local_timestamp" {
            Ok(DataValue::Time64(Utc::now().timestamp(), 0, false))
        } else {
            unreachable!()
        }
    }

    fn monotonicity(&self) -> Option<FuncMonotonicity> {
        todo!()
    }

    fn return_type(&self) -> &LogicalType {
        &LogicalType::TimeStamp(None, false)
    }

    fn summary(&self) -> &FunctionSummary {
        &self.summary
    }
}
