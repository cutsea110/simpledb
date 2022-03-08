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
    tx: Arc<Mutex<Transaction>>,
    idxname: String,
    layout: Arc<Layout>,
    searchkey: Option<Constant>,
    ts: Option<TableScan>,
}

impl HashIndex {
    pub fn new(tx: Arc<Mutex<Transaction>>, idxname: String, layout: Arc<Layout>) -> Self {
        panic!("TODO")
    }
    pub fn search_cost(numblocks: i32, rpb: i32) -> i32 {
        numblocks / NUM_BUCKETS
    }
}

impl Index for HashIndex {
    fn before_first(&self, searchkey: Constant) -> Result<()> {
        panic!("TODO")
    }
    fn next(&self) -> bool {
        panic!("TODO")
    }
    fn get_data_rid(&self) -> Result<RID> {
        panic!("TODO")
    }
    fn insert(&mut self, dataval: Constant, datarid: RID) -> Result<()> {
        panic!("TODO")
    }
    fn delete(&mut self, dataval: Constant, datarid: RID) -> Result<()> {
        panic!("TODO")
    }
    fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
}
