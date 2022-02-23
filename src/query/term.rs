use anyhow::Result;
use std::cmp::*;

use super::{constant::Constant, expression::Expression, scan::Scan};
use crate::{plan::plan::Plan, record::schema::Schema};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Term {
    lhs: Expression,
    rhs: Expression,
}

impl Term {
    pub fn new(lhs: Expression, rhs: Expression) -> Self {
        Self { lhs, rhs }
    }
    pub fn is_satisfied(&self, s: &mut dyn Scan) -> bool {
        let lhsval = self.lhs.evaluate(s);
        let rhsval = self.rhs.evaluate(s);
        lhsval == rhsval
    }
    pub fn applies_to(&self, sch: &Schema) -> bool {
        self.lhs.applies_to(sch) && self.rhs.applies_to(sch)
    }
    pub fn reduction_factor(&self, p: &mut dyn Plan) -> Result<i32> {
        match (&self.lhs, &self.rhs) {
            (Expression::Fldname(lhs_name), Expression::Fldname(rhs_name)) => {
                return Ok(max(
                    p.distinct_values(&lhs_name),
                    p.distinct_values(&rhs_name),
                ));
            }
            (Expression::Fldname(lhs_name), Expression::Val(_)) => {
                return Ok(p.distinct_values(&lhs_name));
            }
            (Expression::Val(_), Expression::Fldname(rhs_name)) => {
                return Ok(p.distinct_values(&rhs_name));
            }
            (Expression::Val(lhs_val), Expression::Val(rhs_val)) => {
                if lhs_val == rhs_val {
                    return Ok(1);
                } else {
                    return Ok(i32::MAX);
                }
            }
        }
    }
    pub fn equates_with_constant(&self, fldname: &str) -> Option<&Constant> {
        match (&self.lhs, &self.rhs) {
            (Expression::Fldname(lhs_name), Expression::Val(_)) => {
                if lhs_name == fldname {
                    return self.rhs.as_constant();
                }
                None
            }
            (Expression::Val(_), Expression::Fldname(rhs_name)) => {
                if rhs_name == fldname {
                    return self.lhs.as_constant();
                }
                None
            }
            _ => return None,
        }
    }
    pub fn equates_with_field(&self, fldname: &str) -> Option<&str> {
        match (&self.lhs, &self.rhs) {
            (Expression::Fldname(lhs_name), Expression::Fldname(rhs_name)) => {
                if lhs_name == fldname {
                    return Some(rhs_name);
                } else if rhs_name == fldname {
                    return Some(lhs_name);
                } else {
                    return None;
                }
            }
            _ => None,
        }
    }
}
