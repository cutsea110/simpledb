use anyhow::Result;
use core::fmt;

use super::{constant::Constant, scan::Scan};
use crate::record::schema::Schema;

#[derive(Debug)]
pub enum ExpressionError {
    InvalidExpression,
}

impl std::error::Error for ExpressionError {}
impl fmt::Display for ExpressionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExpressionError::InvalidExpression => {
                write!(f, "invalid expression")
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Expression {
    Val(Constant),
    Fldname(String),
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expression::Val(val) => write!(f, "{}", val.to_string()),
            Expression::Fldname(fldname) => write!(f, "{}", fldname),
        }
    }
}

impl Expression {
    pub fn new_val(val: Constant) -> Self {
        Expression::Val(val)
    }
    pub fn new_fldname(fldname: String) -> Self {
        Expression::Fldname(fldname)
    }
    pub fn is_fldname(&self) -> bool {
        match self {
            Expression::Val(_) => false,
            Expression::Fldname(_) => true,
        }
    }
    pub fn as_constant(&self) -> Option<&Constant> {
        match self {
            Expression::Val(c) => Some(&c),
            Expression::Fldname(_) => None,
        }
    }
    pub fn field_name(&self) -> Result<&str> {
        match self {
            Expression::Val(_) => Err(From::from(ExpressionError::InvalidExpression)),
            Expression::Fldname(s) => Ok(&s),
        }
    }
    pub fn evaluate(&self, s: &mut dyn Scan) -> Constant {
        match self {
            Expression::Val(val) => val.clone(),
            Expression::Fldname(fldname) => s.get_val(fldname),
        }
    }
    pub fn applies_to(&self, sch: Schema) -> bool {
        match self {
            Expression::Val(_) => true,
            Expression::Fldname(fldname) => sch.has_field(fldname),
        }
    }
}
