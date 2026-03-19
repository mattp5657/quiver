#[derive(Debug)]
pub enum LogicalPlan {
    Scan {
        table_name: String,
    },
    Projection {
        columns: Vec<String>,
        child: Box<LogicalPlan>,
    },
}
