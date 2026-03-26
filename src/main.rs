use quiver::sql::parser::QueryParser;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use quiver::physical_plan::scan::ParquetScanExec;
use quiver::physical_plan::PhysicalOperator;


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sql = "SELECT A, B from employees where A > B and B < A";

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;

    let parser = QueryParser::new();

    let plan = parser.to_logical_plan(statements)?;

    println!("{:#?}", plan);

    let mut parquet_scan = ParquetScanExec::new("tests/data/data.parquet", 1024)?;

    while let Some(batch) = parquet_scan.execute() {
        let batch = batch?;
        println!("{:?}", batch);
    }

    // match statement {
    //     Statement::Query(query) => {
    //     // now you have the Query
    //         match query.body.as_ref() {
    //             SetExpr::Select(select) => {
    //                 match &select.from[0].relation {
    //                     TableFactor::Table { name, .. } => {
    //                         println!("{:#?}", name);
    //                 }
    //                 _ => {println!("Hello, world!");}
    //         }
    //             }
    //             _ => {println!("Hello, world!");}
    //         }
    //     }
    //     _ => {println!("Hello, world!");}
    // }

    // println!("{:#?}", statement);

    Ok(())
}
