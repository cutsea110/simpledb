use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::{plan::Plan, selectplan::SelectPlan};
use crate::{
    query::{scan::Scan, selectscan::SelectScan},
    record::schema::Schema,
};

pub struct ProjectPlan {
    p: Arc<dyn Plan>,
    schema: Schema,
}

impl Plan for ProjectPlan {
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

impl ProjectPlan {
    pub fn new(p: Arc<dyn Plan>, fieldlist: Vec<String>) -> Self {
        panic!("TODO")
    }
}
