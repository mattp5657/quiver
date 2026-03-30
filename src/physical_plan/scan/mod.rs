pub mod memory;
pub mod parquet;
pub mod csv;

use arrow::record_batch::RecordBatch;
use crate::physical_plan::PhysicalOperator;

pub use memory::MemoryScanExec;
pub use parquet::ParquetScanExec;
pub use csv::CsvScanExec;

pub enum ScanExec {
    Memory(MemoryScanExec),
    Parquet(ParquetScanExec),
    Csv(CsvScanExec),
}

impl ScanExec {
    pub fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            ScanExec::Memory(s) => s.run(),
            ScanExec::Parquet(s) => s.run(),
            ScanExec::Csv(s) => s.run(),
        }
    }
}