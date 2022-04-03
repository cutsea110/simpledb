use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::query::{constant::Constant, scan::Scan};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GroupValue {
    vals: HashMap<String, Constant>,
}

impl GroupValue {
    pub fn new(s: Arc<Mutex<dyn Scan>>, fields: Vec<String>) -> Self {
        let mut vals = HashMap::<String, Constant>::new();
        for fldname in fields.iter() {
            vals.insert(
                fldname.to_string(),
                s.lock().unwrap().get_val(fldname).unwrap(),
            );
        }

        Self { vals }
    }
    pub fn get_val(&self, fldname: &str) -> Option<&Constant> {
        self.vals.get(fldname)
    }
}
