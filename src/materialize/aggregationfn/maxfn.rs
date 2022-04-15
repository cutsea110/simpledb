use std::{
    cell::RefCell,
    sync::{Arc, Mutex},
};

use super::AggregationFn;
use crate::query::{constant::Constant, scan::Scan};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct MaxFn {
    fldname: String,
    val: RefCell<Option<Constant>>,
}

impl MaxFn {
    pub fn new(fldname: &str) -> Self {
        Self {
            fldname: fldname.to_string(),
            val: RefCell::new(None),
        }
    }
}

impl AggregationFn for MaxFn {
    fn process_first(&self, scan: Arc<Mutex<dyn Scan>>) {
        *self.val.borrow_mut() = scan.lock().unwrap().get_val(&self.fldname).ok()
    }
    fn process_next(&self, scan: Arc<Mutex<dyn Scan>>) {
        let newval = scan.lock().unwrap().get_val(&self.fldname).ok();
        if newval > *self.val.borrow() {
            *self.val.borrow_mut() = newval;
        }
    }
    fn field_name(&self) -> String {
        format!("maxof{}", self.fldname)
    }
    fn value(&self) -> Constant {
        (*self.val.borrow()).as_ref().unwrap().clone()
    }
}
