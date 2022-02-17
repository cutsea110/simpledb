use anyhow::Result;
use std::alloc::Layout;

use super::recordpage::RecordPage;
use crate::tx::transaction::Transaction;

pub struct TableScan {
    tx: Transaction,
    layout: Layout,
    rp: RecordPage,
    filename: String,
    currentslot: i32,
}

impl TableScan {
    pub fn new(tx: Transaction, tblname: String, layout: Layout) -> Self {
        panic!("TODO")
    }
    // TODO: Methods that implement Scan trait
    pub fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
    pub fn before_first(&mut self) -> Result<()> {
        panic!("TODO")
    }
    pub fn next(&self) -> bool {
        panic!("TODO")
    }
    pub fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        panic!("TODO")
    }
    pub fn get_string(&mut self, fldname: &str) -> Result<String> {
        panic!("TODO")
    }
}
