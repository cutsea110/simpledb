use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::sortscan::SortScan;
use crate::query::{constant::Constant, scan::Scan};

pub struct MergeJoinScan {
    s1: Arc<Mutex<dyn Scan>>,
    s2: Arc<Mutex<SortScan>>,
    fldname1: String,
    fldname2: String,
    joinval: Option<Constant>,
}

impl MergeJoinScan {
    pub fn new(
        s1: Arc<Mutex<dyn Scan>>,
        s2: Arc<Mutex<SortScan>>,
        fldname1: &str,
        fldname2: &str,
    ) -> Self {
        panic!("TODO")
    }
}

impl Scan for MergeJoinScan {
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
    fn to_update_scan(&mut self) -> Result<&mut dyn crate::query::updatescan::UpdateScan> {
        panic!("TODO")
    }
    fn as_table_scan(&mut self) -> Result<&mut crate::record::tablescan::TableScan> {
        panic!("TODO")
    }
}
