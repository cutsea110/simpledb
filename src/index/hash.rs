use std::sync::{Arc, Mutex};

use super::Index;
use crate::{
    query::constant::Constant,
    record::{layout::Layout, tablescan::TableScan},
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
    fn before_first(&self, searchkey: Constant) -> anyhow::Result<()> {
        panic!("TODO")
    }
    fn next(&self) -> bool {
        panic!("TODO")
    }
    fn get_data_rid(&self) -> anyhow::Result<crate::record::rid::RID> {
        panic!("TODO")
    }
    fn insert(
        &mut self,
        dataval: Constant,
        datarid: crate::record::rid::RID,
    ) -> anyhow::Result<()> {
        panic!("TODO")
    }
    fn delete(
        &mut self,
        dataval: Constant,
        datarid: crate::record::rid::RID,
    ) -> anyhow::Result<()> {
        panic!("TODO")
    }
    fn close(&mut self) -> anyhow::Result<()> {
        panic!("TODO")
    }
}
