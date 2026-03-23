use crate::shared::values::Value;
use crate::shared::operators::BinaryOp;

#[derive(Debug)]
pub enum Expr {
    Column(String),
    
    Literal(Value),
    
    BinaryExpr {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>
    }
}