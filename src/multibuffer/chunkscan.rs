use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::{
    query::{constant::Constant, scan::Scan},
    record::{layout::Layout, recordpage::RecordPage},
    tx::transaction::Transaction,
};

pub struct ChunkScan {
    buffs: Vec<RecordPage>,
    tx: Arc<Mutex<Transaction>>,
    filename: String,
    layout: Arc<Layout>,
    startbnum: i32,
    endbnum: i32,
    currentbnum: i32,
    rp: RecordPage,
    currentslot: i32,
}

impl ChunkScan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        filename: String,
        layout: Arc<Layout>,
        startbnum: i32,
        endbnum: i32,
    ) -> Self {
        panic!("TODO")
    }
}

impl Scan for ChunkScan {
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
    fn as_sort_scan(&mut self) -> Result<&mut crate::materialize::sortscan::SortScan> {
        panic!("TODO")
    }
}
