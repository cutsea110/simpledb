use crate::{plan::plan::Plan, record::schema::Schema};

use super::{constant::Constant, expression::Expression, scan::Scan};

pub struct Term {
    lhs: Expression,
    rhs: Expression,
}

impl Term {
    pub fn new(lhs: Expression, rhs: Expression) -> Self {
        Self { lhs, rhs }
    }
    pub fn is_satisfied(&self, s: &mut dyn Scan) -> bool {
        panic!("TODO")
    }
    pub fn applies_to(&self, sch: Schema) -> bool {
        panic!("TODO")
    }
    pub fn reduction_factor(&self, p: &mut dyn Plan) -> i32 {
        panic!("TODO")
    }
    pub fn equates_with_constant(&self, fldname: &str) -> Constant {
        panic!("TODO")
    }
    pub fn equates_with_field(&self, fldname: &str) -> String {
        panic!("TODO")
    }
}
