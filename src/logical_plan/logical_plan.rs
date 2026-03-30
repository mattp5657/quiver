use crate::logical_plan::Expr;

#[derive(Debug, PartialEq)]
pub enum LogicalPlan {
    Scan {
        table_name: String,
        parent: Box<LogicalPlan>,
    },
    Projection {
        columns: Vec<String>,
        parent: Option<Box<LogicalPlan>>,
    },
    Filter{
        predicate: Expr,
        parent: Box<LogicalPlan>,
    },
}
