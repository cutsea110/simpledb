use core::fmt;

use super::{scan::Scan, term::Term};
use crate::{plan::plan::Plan, query::constant::Constant, record::schema::Schema};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Predicate {
    terms: Vec<Term>,
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        panic!("TODO")
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
        panic!("TODO")
    }
    pub fn reduction_factor(&self, p: &mut dyn Plan) -> i32 {
        panic!("TODO")
    }
    pub fn select_sub_pred(&self, sch: Schema) -> Predicate {
        panic!("TODO")
    }
    pub fn join_sub_pred(&self, sch1: Schema, sch2: Schema) -> Predicate {
        panic!("TODO")
    }
    pub fn equates_with_constant(&self, fldname: &str) -> Constant {
        panic!("TODO")
    }
    pub fn equates_with_field(&self, fldname: &str) -> String {
        panic!("TODO")
    }
}
