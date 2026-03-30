use arrow::record_batch::RecordBatch;

pub trait PhysicalOperator {
    fn execute(&mut self, batch: RecordBatch) -> Result<(), Box<dyn std::error::Error>>;
}