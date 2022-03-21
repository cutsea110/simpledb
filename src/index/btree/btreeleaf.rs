use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::{btpage::BTPage, direntry::DirEntry};
use crate::{
    file::block_id::BlockId,
    query::constant::Constant,
    record::{layout::Layout, rid::RID},
    tx::transaction::Transaction,
};

pub struct BTreeLeaf {
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Layout>,
    searchkey: Constant,
    constants: BTPage,
    currentslot: i32,
    filename: String,
}

impl BTreeLeaf {
    pub fn new(tx: Arc<Mutex<Transaction>>, blk: BlockId, layout: Arc<Layout>) -> Self {
        panic!("TODO")
    }
    pub fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
    pub fn next(&self) -> bool {
        panic!("TODO")
    }
    pub fn get_data_rid(&self) -> Result<RID> {
        panic!("TODO")
    }
    pub fn delete(&mut self, datarid: RID) -> Result<()> {
        panic!("TODO")
    }
    pub fn insert(&mut self, datarid: RID) -> Result<DirEntry> {
        panic!("TODO")
    }
    pub fn try_overflow(&self) -> bool {
        panic!("TODO")
    }
}
