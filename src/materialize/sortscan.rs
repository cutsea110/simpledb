use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::temptable::TempTable;
use crate::{
    query::{scan::Scan, updatescan::UpdateScan},
    record::rid::RID,
};

pub struct SortScan {
    s1: Arc<Mutex<dyn UpdateScan>>,
    s2: Option<Arc<Mutex<dyn UpdateScan>>>,
    // TODO: comp and RecordComparator
    hasmore1: bool,
    hasmore2: bool,
    savedposition: Vec<RID>,
}

impl SortScan {
    // TODO: add comp argument
    pub fn new(runs: Vec<TempTable>) -> Self {
        panic!("TODO")
    }
    pub fn save_position(&mut self) -> Result<()> {
        panic!("TODO")
    }
    pub fn restore_position(&mut self) -> Result<()> {
        panic!("TODO")
    }
}

impl Scan for SortScan {
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
    fn get_val(&mut self, fldname: &str) -> Result<crate::query::constant::Constant> {
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
    fn as_table_scan(&mut self) -> Result<&mut crate::record::tablescan::TableScan> {
        panic!("TODO")
    }
}
