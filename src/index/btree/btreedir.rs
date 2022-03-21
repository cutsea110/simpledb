use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::{btpage::BTPage, direntry::DirEntry};
use crate::{
    file::block_id::BlockId, query::constant::Constant, record::layout::Layout,
    tx::transaction::Transaction,
};

pub struct BTreeDir {
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Layout>,
    contents: BTPage,
    filename: String,
}

impl BTreeDir {
    pub fn new(tx: Arc<Mutex<Transaction>>, blk: BlockId, layout: Arc<Layout>) -> Self {
        panic!("TODO")
    }
    pub fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
    pub fn search(&self, searchkey: Constant) -> Result<i32> {
        panic!("TODO")
    }
    pub fn make_new_root(&mut self, e: DirEntry) -> Result<()> {
        panic!("TODO")
    }
    pub fn insert(&mut self, e: DirEntry) -> Result<DirEntry> {
        panic!("TODO")
    }
    pub fn insert_entry(&mut self, e: DirEntry) -> Result<DirEntry> {
        panic!("TODO")
    }
    pub fn find_child_block(&self, searchkey: Constant) -> Result<BlockId> {
        panic!("TODO")
    }
}
