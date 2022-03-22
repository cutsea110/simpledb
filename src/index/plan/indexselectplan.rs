use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::{
    metadata::indexmanager::IndexInfo,
    plan::plan::Plan,
    query::{constant::Constant, scan::Scan},
    record::schema::Schema,
};

pub struct IndexSelectPlan {
    p: Arc<dyn Plan>,
    ii: IndexInfo,
    val: Constant,
}

impl IndexSelectPlan {
    pub fn new(p: Arc<dyn Plan>, ii: IndexInfo, val: Constant) -> Self {
        panic!("TODO")
    }
}

impl Plan for IndexSelectPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        panic!("TODO")
    }
    fn blocks_accessed(&self) -> i32 {
        panic!("TODO")
    }
    fn records_output(&self) -> i32 {
        panic!("TODO")
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        panic!("TODO")
    }
    fn schema(&self) -> Arc<Schema> {
        panic!("TODO")
    }
}
