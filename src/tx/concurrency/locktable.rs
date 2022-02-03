use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::file::block_id::BlockId;

const MAX_TIME: i64 = 10_000;

pub struct LockTable {
    locks: HashMap<BlockId, i32>,
    l: Arc<Mutex<()>>,
}

impl LockTable {
    pub fn s_lock(&mut self, blk: BlockId) -> Result<()> {
        panic!("TODO")
    }
    pub fn x_lock(&mut self, blk: BlockId) -> Result<()> {
        panic!("TODO")
    }
    pub fn unlock(&mut self, blk: BlockId) -> Result<()> {
        panic!("TODO")
    }
    fn has_x_lock(&self, blk: BlockId) -> Result<bool> {
        panic!("TODO")
    }
    fn has_other_locks(&self, blk: BlockId) -> Result<bool> {
        panic!("TODO")
    }
    fn waiting_too_long(&self, starttime: i64) -> bool {
        panic!("TODO")
    }
    fn get_lock_val(&self, blk: BlockId) -> Result<i64> {
        panic!("TODO")
    }
}
