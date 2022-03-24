use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::{
    index::Index,
    query::{constant::Constant, scan::Scan, updatescan::UpdateScan},
    record::tablescan::TableScan,
};

pub struct IndexJoinScan {
    lhs: Arc<Mutex<dyn Scan>>,
    idx: Arc<Mutex<dyn Index>>,
    joinfield: String,
    rhs: Arc<Mutex<TableScan>>,
}

impl IndexJoinScan {
    pub fn new(
        lhs: Arc<Mutex<dyn Scan>>,
        idx: Arc<Mutex<dyn Index>>,
        joinfld: &str,
        rhs: Arc<Mutex<TableScan>>,
    ) -> Self {
        panic!("TODO")
    }
}

impl Scan for IndexJoinScan {
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
    fn as_table_scan(&self) -> Result<&TableScan> {
        panic!("TODO")
    }
}
