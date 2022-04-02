use std::sync::{Arc, Mutex};

use anyhow::Result;

use super::aggregationfn::AggregationFn;
use crate::{
    plan::plan::Plan, query::scan::Scan, record::schema::Schema, tx::transaction::Transaction,
};

pub struct GroupByPlan {
    p: Arc<dyn Plan>,
    groupfields: Vec<String>,
    aggfns: Vec<Arc<dyn AggregationFn>>,
    sch: Arc<Schema>,
}

impl GroupByPlan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        p: Arc<dyn Plan>,
        groupfields: Vec<String>,
        aggfns: Vec<Arc<dyn AggregationFn>>,
    ) -> Self {
        panic!("TODO")
    }
}

impl Plan for GroupByPlan {
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
