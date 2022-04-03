use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
};

use crate::query::{constant::Constant, scan::Scan};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GroupValue {
    vals: HashMap<String, Constant>,
}

impl Hash for GroupValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        panic!("TODO")
    }
    fn hash_slice<H: Hasher>(data: &[Self], state: &mut H)
    where
        Self: Sized,
    {
        panic!("TODO")
    }
}

impl GroupValue {
    pub fn new(s: Arc<Mutex<dyn Scan>>, fields: Vec<String>) -> Self {
        panic!("TODO")
    }
    pub fn get_val(&self, fldname: &str) -> Constant {
        panic!("TODO")
    }
}
