use quiver::sql::parser::QueryParser;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sql = "SELECT A, B from employees";

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;

    let parser = QueryParser::new();

    let plan = parser.to_logical_plan(statements)?;

    println!("{:#?}", plan);

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
