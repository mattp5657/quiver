use std::collections::HashMap;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use quiver::logical_plan::LogicalPlan;
use quiver::physical_plan::filter::FilterExec;
use quiver::physical_plan::projection::{OutputProjectionExec, ProjectionExec, IntermediateProjectionExec};
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

    let mut scan = plan_to_scan(logical_plan, &catalog)?;
    scan.run()?;

    // TODO: access results from OutputProjectionExec

    Ok(())
}

fn plan_to_scan(
    plan: LogicalPlan,
    catalog: &HashMap<String, String>,
) -> Result<ScanExec, Box<dyn std::error::Error>> {
    match plan {
        LogicalPlan::Scan { table_name, parent } => {
            let path = catalog
                .get(&table_name)
                .ok_or(format!("table '{}' not in catalog", table_name))?;
            let parent_exec = plan_node_to_exec(*parent)?;
            Ok(ScanExec::Parquet(ParquetScanExec::new(path, 1024, parent_exec)?))
        }
        _ => Err("expected Scan at root of logical plan".into()),
    }
}

fn plan_node_to_exec(
    plan: LogicalPlan,
) -> Result<Box<dyn PhysicalOperator>, Box<dyn std::error::Error>> {
    match plan {
        LogicalPlan::Filter { predicate, parent } => {
            let parent_exec = plan_node_to_exec(*parent)?;
            Ok(Box::new(FilterExec::new(predicate, parent_exec)))
        }
        LogicalPlan::Projection { columns, parent } => {
            match parent {
                Some(p) => {
                    let parent_exec = plan_node_to_exec(*p)?;
                    Ok(Box::new(ProjectionExec::Intermediate(
                        IntermediateProjectionExec::new(columns, parent_exec),
                    )))
                }
                None => {
                    Ok(Box::new(ProjectionExec::Output(
                        OutputProjectionExec::new(columns),
                    )))
                }
            }
        }
        _ => Err("unexpected Scan node in middle of plan".into()),
    }
}
