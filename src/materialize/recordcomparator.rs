use std::{
    cmp::Ordering,
    sync::{Arc, Mutex},
};

use crate::query::scan::Scan;

#[derive(Debug, Clone)]
pub struct RecordComparator {
    fields: Vec<String>,
}

impl RecordComparator {
    pub fn new(fields: Vec<String>) -> Self {
        Self { fields }
    }
    pub fn compare(&self, s1: Arc<Mutex<dyn Scan>>, s2: Arc<Mutex<dyn Scan>>) -> Ordering {
        for fldname in self.fields.iter() {
            let val1 = s1.lock().unwrap().get_val(fldname).unwrap();
            let val2 = s2.lock().unwrap().get_val(fldname).unwrap();
            let result = val1.cmp(&val2);
            if result.is_ne() {
                return result;
            }
        }

        Ordering::Equal
    }
    // my own extends
    pub fn fields(&self) -> Vec<String> {
        self.fields.clone()
    }
}
