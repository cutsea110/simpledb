use std::sync::{Arc, Mutex};

use super::AggregationFn;
use crate::query::{constant::Constant, scan::Scan};

pub struct MaxFn {
    fldname: String,
    val: Option<Constant>,
}

impl MaxFn {
    pub fn new(fldname: &str) -> Self {
        Self {
            fldname: fldname.to_string(),
            val: None,
        }
    }
}

impl AggregationFn for MaxFn {
    fn process_first(&mut self, scan: Arc<Mutex<dyn Scan>>) {
        self.val = scan.lock().unwrap().get_val(&self.fldname).ok()
    }
    fn process_next(&mut self, scan: Arc<Mutex<dyn Scan>>) {
        let newval = scan.lock().unwrap().get_val(&self.fldname).ok();
        if newval > self.val {
            self.val = newval;
        }
    }
    fn field_name(&self) -> String {
        format!("maxof{}", self.fldname)
    }
    fn value(&self) -> Constant {
        self.val.clone().expect("access value")
    }
}
