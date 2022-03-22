use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::btreeleaf::BTreeLeaf;
use crate::{
    file::block_id::BlockId,
    index::Index,
    query::constant::Constant,
    record::{layout::Layout, rid::RID},
    tx::transaction::Transaction,
};

pub struct BTreeIndex {
    tx: Arc<Mutex<Transaction>>,
    dir_layout: Arc<Layout>,
    leaf_layout: Arc<Layout>,
    leaftbl: String,
    leaf: Option<BTreeLeaf>,
    rootblk: BlockId,
}

impl BTreeIndex {
    pub fn new(tx: Arc<Mutex<Transaction>>, idxname: String, leaf_layout: Arc<Layout>) -> Self {
        panic!("TODO")
    }
    pub fn search_cost(numblocks: i32, rpb: i32) -> i32 {
        panic!("TODO")
    }
}

impl Index for BTreeIndex {
    fn before_first(&mut self, searchkey: Constant) -> Result<()> {
        panic!("TODO")
    }
    fn next(&mut self) -> bool {
        panic!("TODO")
    }
    fn get_data_rid(&mut self) -> Result<RID> {
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
