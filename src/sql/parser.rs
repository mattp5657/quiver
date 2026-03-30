use crate::logical_plan::LogicalPlan;
use crate::logical_plan::expr::Expr;
use crate::shared::Value;
use crate::shared::BinaryOp;
use sqlparser::ast::Expr as SqlExpr;
use sqlparser::ast::Value as SqlValue;
use sqlparser::ast::BinaryOperator;
use sqlparser::ast::Query;
use sqlparser::ast::Select;
use sqlparser::ast::SelectItem;
use sqlparser::ast::SetExpr;
use sqlparser::ast::Statement;
use sqlparser::ast::TableFactor;
use std::collections::HashMap;

pub struct QueryParser {
    catalog: HashMap<String, String>,
}

impl QueryParser {
    pub fn new() -> Self {
        QueryParser {
            catalog: HashMap::new(),
        }
    }

    pub fn to_logical_plan(
        &self,
        statements: Vec<Statement>,
    ) -> Result<LogicalPlan, Box<dyn std::error::Error>> {
        let statement = &statements[0];

        match statement {
            Statement::Query(query) => self.query_to_plan(query),
            _ => Err("unsupported statement".into()),
        }
    }

    fn query_to_plan(&self, query: &Query) -> Result<LogicalPlan, Box<dyn std::error::Error>> {
        match query.body.as_ref() {
            SetExpr::Select(select) => self.select_to_plan(select),
            _ => Err("unsupported statement".into()),
        }
    }

    fn select_to_plan(&self, select: &Select) -> Result<LogicalPlan, Box<dyn std::error::Error>> {
        let table_name = self.extract_table_name(select)?;
        let columns = self.extract_columns(select)?;

        let projection = LogicalPlan::Projection {
            columns,
            parent: None,
        };

        let scan_parent = match &select.selection {
            Some(expr) => LogicalPlan::Filter {
                predicate: self.extract_filter(expr)?,
                parent: Box::new(projection),
            },
            None => projection,
        };

        Ok(LogicalPlan::Scan {
            table_name,
            parent: Box::new(scan_parent),
        })
    }

    fn extract_table_name(&self, select: &Select) -> Result<String, Box<dyn std::error::Error>> {
        match &select.from[0].relation {
            TableFactor::Table { name, .. } => Ok(name.to_string()),
            _ => Err("unsupported statement".into()),
        }
    }

    fn extract_columns(&self, select: &Select) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut columns = vec![];

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(SqlExpr::Identifier(ident)) => {
                    columns.push(ident.value.clone());
                }
                SelectItem::Wildcard(_) => {
                    return Ok(vec!["*".to_string()]);
                }
                _ => return Err("unsupported column type".into()),
            }
        }

        Ok(columns)
    }

    fn extract_filter(&self, expr: &SqlExpr) -> Result<Expr, Box<dyn std::error::Error>> {
        match expr {
            SqlExpr::BinaryOp { left, op, right } => {
                let left_expr = self.extract_filter(left)?;
                let right_expr = self.extract_filter(right)?;
                let binary_op = self.extract_op(op)?;
                
                Ok(Expr::BinaryExpr {
                    left: Box::new(left_expr),
                    op: binary_op,
                    right: Box::new(right_expr)
                })
            }
            SqlExpr::Identifier(ident) => {
                Ok(Expr::Column(ident.value.clone()))
            }
            SqlExpr::Value(v) => {
                Ok(Expr::Literal(self.extract_value(&v.value)?))
            }
            _ => Err("unsupported expression".into())
        }
    }

    fn extract_op(&self, op: &BinaryOperator) -> Result<BinaryOp, Box<dyn std::error::Error>> {
        match op {
            BinaryOperator::Eq => Ok(BinaryOp::Eq),
            BinaryOperator::NotEq => Ok(BinaryOp::NotEq),
            BinaryOperator::Gt => Ok(BinaryOp::Gt),
            BinaryOperator::GtEq => Ok(BinaryOp::GtEq),
            BinaryOperator::Lt => Ok(BinaryOp::Lt),
            BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
            BinaryOperator::And => Ok(BinaryOp::And),
            BinaryOperator::Or => Ok(BinaryOp::Or),
            _ => Err("unsupported operator".into())
        }
    }

    fn extract_value(&self, val: &SqlValue) -> Result<Value, Box<dyn std::error::Error>> {
        match val {
            SqlValue::Number(n, _) => Ok(Value::Int64(n.parse::<i64>()?)),
            SqlValue::SingleQuotedString(s) => Ok(Value::Utf8(s.clone())),
            SqlValue::Boolean(b) => Ok(Value::Boolean(*b)),
            _ => Err("unsupported value".into())
        }
    }
}

mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse(sql: &str) -> LogicalPlan {
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        QueryParser::new().to_logical_plan(stmts).unwrap()
    }

    // Verifies SELECT * builds a push chain: Scan -> Projection (terminal).
    // Projection has no parent (None) since it is the output node.
    // No Filter should be present — the query has no WHERE clause.
    #[test]
    fn test_select_star() {
        let plan = parse("SELECT * FROM employees");
        assert_eq!(
            plan,
            LogicalPlan::Scan {
                table_name: "employees".to_string(),
                parent: Box::new(LogicalPlan::Projection {
                    columns: vec!["*".to_string()],
                    parent: None,
                }),
            }
        );
    }

    // Verifies named columns are extracted correctly into the Projection node.
    // Push chain: Scan -> Projection (terminal, parent=None).
    // No Filter should be present — the query has no WHERE clause.
    #[test]
    fn test_select_named_columns() {
        let plan = parse("SELECT a, b FROM employees");
        assert_eq!(
            plan,
            LogicalPlan::Scan {
                table_name: "employees".to_string(),
                parent: Box::new(LogicalPlan::Projection {
                    columns: vec!["a".to_string(), "b".to_string()],
                    parent: None,
                }),
            }
        );
    }

    // Verifies a WHERE clause inserts a Filter between Scan and Projection.
    // Push chain: Scan -> Filter(a > 5) -> Projection (terminal, parent=None).
    // Predicate is a BinaryExpr with a Column on the left and Int64 literal on the right.
    #[test]
    fn test_select_with_filter() {
        let plan = parse("SELECT a FROM employees WHERE a > 5");
        assert_eq!(
            plan,
            LogicalPlan::Scan {
                table_name: "employees".to_string(),
                parent: Box::new(LogicalPlan::Filter {
                    predicate: Expr::BinaryExpr {
                        left: Box::new(Expr::Column("a".to_string())),
                        op: BinaryOp::Gt,
                        right: Box::new(Expr::Literal(Value::Int64(5))),
                    },
                    parent: Box::new(LogicalPlan::Projection {
                        columns: vec!["a".to_string()],
                        parent: None,
                    }),
                }),
            }
        );
    }

    // Verifies unsupported SQL statements return an error rather than panic.
    // INSERT is not a query — the parser should reject it gracefully.
    #[test]
    fn test_unsupported_statement_returns_err() {
        let stmts = Parser::parse_sql(&GenericDialect {}, "INSERT INTO foo VALUES (1)").unwrap();
        let result = QueryParser::new().to_logical_plan(stmts);
        assert!(result.is_err());
    }
}
