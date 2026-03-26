use crate::logical_plan::Expr;

#[derive(Debug, PartialEq)]
pub enum LogicalPlan {
    Scan {
        table_name: String,
    },
    Projection {
        columns: Vec<String>,
        child: Box<LogicalPlan>,
    },
    Filter{
        predicate: Expr,
        child: Box<LogicalPlan>
    },
}
