use arrow::record_batch::RecordBatch;

pub trait PhysicalOperator {
    fn execute(&mut self) -> Option<Result<RecordBatch, Box<dyn std::error::Error>>>;
}