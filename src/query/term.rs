use core::fmt;
use std::{
    cmp::*,
    sync::{Arc, Mutex},
};

use super::{constant::Constant, expression::Expression, scan::Scan};
use crate::{plan::plan::Plan, record::schema::Schema};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Term {
    lhs: Expression,
    rhs: Expression,
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}={}", self.lhs.to_string(), self.rhs.to_string())
    }
}

impl Term {
    pub fn new(lhs: Expression, rhs: Expression) -> Self {
        Self { lhs, rhs }
    }
    pub fn is_satisfied(&self, s: Arc<Mutex<dyn Scan>>) -> bool {
        let lhsval = self.lhs.evaluate(Arc::clone(&s));
        let rhsval = self.rhs.evaluate(Arc::clone(&s));
        lhsval.unwrap() == rhsval.unwrap()
    }
    pub fn applies_to(&self, sch: Arc<Schema>) -> bool {
        self.lhs.applies_to(Arc::clone(&sch)) && self.rhs.applies_to(Arc::clone(&sch))
    }
    pub fn reduction_factor(&self, p: Arc<dyn Plan>) -> i32 {
        match (&self.lhs, &self.rhs) {
            (Expression::Fldname(lhs_name), Expression::Fldname(rhs_name)) => {
                return max(p.distinct_values(&lhs_name), p.distinct_values(&rhs_name));
            }
            (Expression::Fldname(lhs_name), Expression::Val(_)) => {
                return p.distinct_values(&lhs_name);
            }
            (Expression::Val(_), Expression::Fldname(rhs_name)) => {
                return p.distinct_values(&rhs_name);
            }
            (Expression::Val(lhs_val), Expression::Val(rhs_val)) => {
                if lhs_val == rhs_val {
                    return 1;
                } else {
                    return i32::MAX;
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
    // my own extends
    pub fn lhs(&self) -> &Expression {
        &self.lhs
    }
    pub fn rhs(&self) -> &Expression {
        &self.rhs
    }
}
