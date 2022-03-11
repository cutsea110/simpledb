use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::Index;
use crate::{
    query::constant::Constant,
    record::{layout::Layout, rid::RID, tablescan::TableScan},
    tx::transaction::Transaction,
};

pub const NUM_BUCKETS: i32 = 100;

pub struct HashIndex {
    _tx: Arc<Mutex<Transaction>>,
    _idxname: String,
    _layout: Arc<Layout>,
    _searchkey: Option<Constant>,
    _ts: Option<TableScan>,
}

impl HashIndex {
    pub fn new(_tx: Arc<Mutex<Transaction>>, _idxname: String, _layout: Arc<Layout>) -> Self {
        panic!("TODO")
    }
    pub fn search_cost(numblocks: i32, _rpb: i32) -> i32 {
        numblocks / NUM_BUCKETS
    }
}

impl Index for HashIndex {
    fn before_first(&self, _searchkey: Constant) -> Result<()> {
        panic!("TODO")
    }
    fn next(&self) -> bool {
        panic!("TODO")
    }
    fn get_data_rid(&self) -> Result<RID> {
        panic!("TODO")
    }
    fn insert(&mut self, _dataval: Constant, _datarid: RID) -> Result<()> {
        panic!("TODO")
    }
    fn delete(&mut self, _dataval: Constant, _datarid: RID) -> Result<()> {
        panic!("TODO")
    }
    fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
}
