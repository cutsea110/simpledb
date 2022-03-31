use anyhow::Result;
use std::sync::{Arc, Mutex};

use crate::{
    plan::plan::Plan,
    query::{scan::Scan, updatescan::UpdateScan},
    record::schema::Schema,
    tx::transaction::Transaction,
};

use super::temptable::TempTable;

pub struct SortPlan {
    p: Arc<dyn Plan>,
    tx: Arc<Mutex<Transaction>>,
    sch: Arc<Schema>,
    // TODO: comp and RecordComparator
}

impl SortPlan {
    pub fn new(p: Arc<dyn Plan>, sortfields: Vec<String>, tx: Arc<Mutex<Transaction>>) -> Self {
        panic!("TODO")
    }

    fn split_into_runs(src: Arc<Mutex<dyn Scan>>) -> Vec<TempTable> {
        panic!("TODO")
    }
    fn do_a_merge_iteration(runs: Vec<TempTable>) -> Vec<TempTable> {
        panic!("TODO")
    }
    fn merge_two_runs(p1: TempTable, p2: TempTable) -> TempTable {
        panic!("TODO")
    }
    fn copy(src: Arc<Mutex<dyn Scan>>, dest: Arc<Mutex<dyn UpdateScan>>) -> bool {
        panic!("TODO")
    }
}

impl Plan for SortPlan {
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
