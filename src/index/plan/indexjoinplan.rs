use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::{
    metadata::indexmanager::IndexInfo, plan::plan::Plan, query::scan::Scan, record::schema::Schema,
};

pub struct IndexJoinPlan {
    p1: Arc<dyn Plan>,
    p2: Arc<dyn Plan>,
    ii: IndexInfo,
    joinfield: String,
    sch: Schema,
}

impl IndexJoinPlan {
    pub fn new(p1: Arc<dyn Plan>, p2: Arc<dyn Plan>, ii: IndexInfo, joinfield: &str) -> Self {
        panic!("TODO")
    }
}

impl Plan for IndexJoinPlan {
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
