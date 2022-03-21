use anyhow::Result;
use core::panic;
use std::sync::{Arc, Mutex};

use crate::{
    file::block_id::BlockId,
    query::constant::Constant,
    record::{layout::Layout, rid::RID},
    tx::transaction::Transaction,
};

pub struct BTPage {
    tx: Arc<Mutex<Transaction>>,
    currentblk: BlockId,
    layout: Arc<Layout>,
}

impl BTPage {
    pub fn new(tx: Arc<Mutex<Transaction>>, currentblk: BlockId, layout: Arc<Layout>) -> Self {
        Self {
            tx,
            currentblk,
            layout,
        }
    }
    pub fn find_slot_before(&self, searchkey: Constant) -> i32 {
        panic!("TODO")
    }
    pub fn close(&mut self) -> Result<()> {
        panic!("TODO")
    }
    pub fn is_full(&self) -> bool {
        panic!("TODO")
    }
    pub fn split(&mut self, splitpos: i32, flag: i32) -> BlockId {
        panic!("TODO")
    }
    pub fn get_data_val(&self, slot: i32) -> Constant {
        panic!("TODO")
    }
    pub fn get_flag(&self) -> i32 {
        panic!("TODO")
    }
    pub fn append_new(&self, flag: i32) -> BlockId {
        panic!("TODO")
    }
    pub fn format(&mut self, blk: BlockId, flag: i32) -> Result<()> {
        panic!("TODO")
    }
    pub fn make_default_record(&self, blk: BlockId, pos: i32) -> Result<()> {
        panic!("TODO")
    }
    // Methods called only by BTreeDir
    pub fn get_child_num(&self, slot: i32) -> i32 {
        panic!("TODO")
    }
    pub fn insert_dir(&mut self, slot: i32, val: Constant, blknum: i32) -> Result<()> {
        panic!("TODO")
    }
    // Methods called only by BTreeLeaf
    pub fn get_data_rid(&self, slot: i32) -> RID {
        panic!("TODO")
    }
    pub fn insert_leaf(&mut self, slot: i32, val: Constant, rid: RID) -> Result<()> {
        panic!("TODO")
    }

    pub fn delete(&mut self, slot: i32) -> Result<()> {
        panic!("TODO")
    }
    pub fn get_num_recs(&self) -> i32 {
        panic!("TODO")
    }
    // Private methods
    fn get_i32(&self, slot: i32, fldname: &str) -> Result<i32> {
        panic!("TODO")
    }
    fn get_string(&self, slot: i32, fldname: &str) -> Result<String> {
        panic!("TODO")
    }
    fn get_val(&self, slot: i32, fldname: &str) -> Result<Constant> {
        panic!("TODO")
    }
    fn set_i32(&mut self, slot: i32, fldname: &str, val: i32) -> Result<()> {
        panic!("TODO")
    }
    fn set_string(&mut self, slot: i32, fldname: &str, val: String) -> Result<()> {
        panic!("TODO")
    }
    fn set_val(&mut self, slot: i32, fldname: &str, val: Constant) -> Result<()> {
        panic!("TODO")
    }
    fn set_num_recs(&mut self, n: i32) -> Result<()> {
        panic!("TODO")
    }
    fn insert(&self, slot: i32) -> Result<()> {
        panic!("TODO")
    }
    fn copy_record(&self, from: i32, to: i32) -> Result<()> {
        panic!("TODO")
    }
    fn transfer_recs(&self, slot: i32, dest: BTPage) -> Result<()> {
        panic!("TODO")
    }
    fn fldpos(&self, slot: i32, fldname: &str) -> i32 {
        panic!("TODO")
    }
    fn slotpos(&self, slot: i32) -> i32 {
        panic!("TODO")
    }
}
