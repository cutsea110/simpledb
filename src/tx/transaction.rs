use std::usize;

use anyhow::Result;

use crate::{
    buffer::manager::BufferMgr,
    file::{block_id::BlockId, manager::FileMgr},
    log::manager::LogMgr,
};

pub struct Transaction {
    fm: FileMgr,
    lm: LogMgr,
    bm: BufferMgr,
}

impl Transaction {
    pub fn new(fm: FileMgr, lm: LogMgr, bm: BufferMgr) -> Self {
        Self { fm, lm, bm }
    }
    pub fn commit(&mut self) -> Result<()> {
        panic!("TODO")
    }
    pub fn rollback(&mut self) -> Result<()> {
        panic!("TODO")
    }
    pub fn recover(&mut self) -> Result<()> {
        panic!("TODO")
    }
    pub fn pin(&mut self, blk: &BlockId) -> Result<()> {
        panic!("TODO")
    }
    pub fn unpin(&mut self, blk: &BlockId) -> Result<()> {
        panic!("TODO")
    }
    pub fn get_i32(&self, blk: &BlockId, offset: i32) -> Result<i32> {
        panic!("TODO")
    }
    pub fn get_string(&self, blk: &BlockId, offset: i32) -> Result<String> {
        panic!("TODO")
    }
    pub fn set_i32(&mut self, blk: &BlockId, offset: i32, val: i32, ok_to_log: bool) -> Result<()> {
        panic!("TODO")
    }
    pub fn set_string(
        &mut self,
        blk: &BlockId,
        offset: i32,
        val: &str,
        ok_to_log: bool,
    ) -> Result<()> {
        panic!("TODO")
    }
    pub fn size(&self, filename: &str) -> u64 {
        panic!("TODO")
    }
    pub fn append(&mut self, filename: &str) -> Result<BlockId> {
        panic!("TODO")
    }
    pub fn block_size(&self) -> u64 {
        panic!("TODO")
    }
    pub fn available_buffs(&self) -> Result<usize> {
        panic!("TODO")
    }
    fn next_tx_number(&mut self) -> Result<i32> {
        panic!("TODO")
    }
}
