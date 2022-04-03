use anyhow::Result;
use std::sync::{Arc, Mutex};

use crate::{
    plan::plan::Plan, query::scan::Scan, record::schema::Schema, tx::transaction::Transaction,
};

pub struct MergeJoinPlan {
    p1: Arc<dyn Plan>,
    p2: Arc<dyn Plan>,
    fldname1: String,
    fldname2: String,
    sch: Arc<Schema>,
}

impl MergeJoinPlan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        p1: Arc<dyn Plan>,
        p2: Arc<dyn Plan>,
        fldname1: &str,
        fldname2: &str,
    ) -> Self {
        panic!("TODO")
    }
}

impl Plan for MergeJoinPlan {
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
