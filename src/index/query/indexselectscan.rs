use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::{
    index::Index,
    query::{constant::Constant, scan::Scan, updatescan::UpdateScan},
    record::tablescan::TableScan,
};

pub struct IndexSelectScan {
    ts: TableScan,
    idx: Arc<Mutex<dyn Index>>,
    val: Constant,
}

impl IndexSelectScan {
    pub fn new(ts: Arc<Mutex<TableScan>>, idx: Arc<Mutex<dyn Index>>, val: Constant) -> Self {
        panic!("TODO")
    }
}

impl Scan for IndexSelectScan {
    fn before_first(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn next(&mut self) -> bool {
        panic!("TODO")
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        panic!("TODO")
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        panic!("TODO")
    }
    fn get_val(&mut self, fldname: &str) -> Result<Constant> {
        panic!("TODO")
    }
    fn has_field(&self, fldname: &str) -> bool {
        panic!("TODO")
    }
    fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }

    fn to_update_scan(&mut self) -> Result<&mut dyn UpdateScan> {
        panic!("TODO")
    }
}
