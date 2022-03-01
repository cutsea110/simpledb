use crate::query::{expression::Expression, predicate::Predicate};

pub struct ModifyData {
    tblname: String,
    fldname: String,
    newval: Expression,
    pred: Predicate,
}

impl ModifyData {
    pub fn new(tblname: String, fldname: String, newval: Expression, pred: Predicate) -> Self {
        Self {
            tblname,
            fldname,
            newval,
            pred,
        }
    }
    pub fn table_name(&self) -> &str {
        &self.tblname
    }
    pub fn target_field(&self) -> &str {
        &self.fldname
    }
    pub fn new_value(&self) -> &Expression {
        &self.newval
    }
    pub fn pred(&self) -> &Predicate {
        &self.pred
    }
}
