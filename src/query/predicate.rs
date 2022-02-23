use anyhow::Result;
use core::fmt;

use super::{scan::Scan, term::Term};
use crate::{plan::plan::Plan, query::constant::Constant, record::schema::Schema};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Predicate {
    terms: Vec<Term>,
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut result = vec![];
        for t in self.terms.iter() {
            result.push(t.to_string());
        }
        write!(f, "{}", result.join(" and "))
    }
}

impl Predicate {
    pub fn new_empty() -> Self {
        Self { terms: vec![] }
    }
    pub fn new(t: Term) -> Self {
        Self { terms: vec![t] }
    }
    pub fn conjoin_with(&mut self, pred: &mut Predicate) {
        self.terms.append(&mut pred.terms)
    }
    pub fn is_satisfied(&self, s: &mut dyn Scan) -> bool {
        for t in self.terms.iter() {
            if !t.is_satisfied(s) {
                return false;
            }
        }
        true
    }
    pub fn reduction_factor(&self, p: &mut dyn Plan) -> Result<i32> {
        let mut factor = 1;
        for t in self.terms.iter() {
            factor *= t.reduction_factor(p)?;
        }
        Ok(factor)
    }
    pub fn select_sub_pred(&self, sch: &Schema) -> Option<Predicate> {
        let mut result = Predicate::new_empty();
        for t in self.terms.iter() {
            if t.applies_to(sch) {
                result.terms.push(t.clone());
            }
        }
        if result.terms.is_empty() {
            return None;
        } else {
            return Some(result);
        }
    }
    pub fn join_sub_pred(&self, sch1: &Schema, sch2: &Schema) -> Option<Predicate> {
        let mut result = Predicate::new_empty();
        let mut newsch = Schema::new();
        newsch.add_all(sch1);
        newsch.add_all(sch2);
        for t in self.terms.iter() {
            if !t.applies_to(sch1) && !t.applies_to(sch2) && t.applies_to(&newsch) {
                result.terms.push(t.clone());
            }
        }
        if result.terms.is_empty() {
            return None;
        } else {
            return Some(result);
        }
    }
    pub fn equates_with_constant(&self, fldname: &str) -> Option<&Constant> {
        for t in self.terms.iter() {
            if let Some(c) = t.equates_with_constant(fldname) {
                return Some(c);
            }
        }
        None
    }
    pub fn equates_with_field(&self, fldname: &str) -> Option<&str> {
        for t in self.terms.iter() {
            if let Some(s) = t.equates_with_field(fldname) {
                return Some(s);
            }
        }
        None
    }
}
