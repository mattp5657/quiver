use crate::logical_plan::LogicalPlan;
use sqlparser::ast::Expr;
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

        let scan = LogicalPlan::Scan { table_name };

        let projection = LogicalPlan::Projection {
            columns,
            child: Box::new(scan),
        };

        Ok(projection)
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
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
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
}
