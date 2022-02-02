use anyhow::Result;
use std::collections::HashMap;

use crate::file::block_id::BlockId;

const MAX_TIME: i64 = 10_000;

pub struct LockTable {
    locks: HashMap<BlockId, i32>,
}

impl LockTable {
    pub fn s_lock(blk: BlockId) -> Result<()> {
        panic!("TODO")
    }
    pub fn x_lock(blk: BlockId) -> Result<()> {
        panic!("TODO")
    }
    pub fn unlock(blk: BlockId) -> Result<()> {
        panic!("TODO")
    }
    fn has_x_lock(blk: BlockId) -> Result<bool> {
        panic!("TODO")
    }
    fn has_other_locks(blk: BlockId) -> Result<bool> {
        panic!("TODO")
    }
    fn waiting_too_long(starttime: i64) -> bool {
        panic!("TODO")
    }
    fn get_lock_val(blk: BlockId) -> Result<i64> {
        panic!("TODO")
    }
}
