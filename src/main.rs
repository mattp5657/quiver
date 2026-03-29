use std::collections::HashMap;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use quiver::logical_plan::LogicalPlan;
use quiver::physical_plan::filter::FilterExec;
use quiver::physical_plan::projection::{OutputProjectionExec, ProjectionExec};
use quiver::physical_plan::scan::{ParquetScanExec, ScanExec};
use quiver::physical_plan::PhysicalOperator;
use quiver::sql::parser::QueryParser;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sql = "SELECT Volatility_30d, Month FROM data";

    let stmts = Parser::parse_sql(&GenericDialect {}, sql)?;
    let logical_plan = QueryParser::new().to_logical_plan(stmts)?;
    println!("Logical plan:\n{:#?}\n", logical_plan);

    let mut catalog = HashMap::new();
    catalog.insert("data".to_string(), "tests/data/data.parquet".to_string());

    let mut exec = plan_to_exec(logical_plan, &catalog)?;

    while let Some(result) = exec.execute() {
        let batch = result?;
        println!("{:#?}", batch);
    }

    Ok(())
}

fn plan_to_exec(
    plan: LogicalPlan,
    catalog: &HashMap<String, String>,
) -> Result<Box<dyn PhysicalOperator>, Box<dyn std::error::Error>> {
    match plan {
        LogicalPlan::Scan { table_name } => {
            let path = catalog
                .get(&table_name)
                .ok_or(format!("table '{}' not in catalog", table_name))?;
            Ok(Box::new(ScanExec::Parquet(ParquetScanExec::new(path, 1024)?)))
        }
        LogicalPlan::Filter { predicate, child } => {
            let child_exec = plan_to_exec(*child, catalog)?;
            Ok(Box::new(FilterExec::new(predicate, child_exec)))
        }
        LogicalPlan::Projection { columns, child } => {
            let child_exec = plan_to_exec(*child, catalog)?;
            Ok(Box::new(ProjectionExec::Output(OutputProjectionExec::new(
                columns,
                child_exec,
            ))))
        }
    }
}