use std::{
    sync::{Arc, Mutex, Once},
    usize,
};

use anyhow::Result;

use crate::{
    buffer::manager::BufferMgr,
    file::{block_id::BlockId, manager::FileMgr},
    log::manager::LogMgr,
};

use super::{bufferlist::BufferList, concurrency::manager::ConcurrencyMgr};

static END_OF_FILE: i64 = -1;

pub struct Transaction {
    // static member (shared by all Transaction)
    next_tx_num: Arc<Mutex<u64>>,

    concur_mgr: ConcurrencyMgr,
    fm: Arc<Mutex<FileMgr>>,
    bm: Arc<Mutex<BufferMgr>>,
    mybuffers: BufferList,
}

impl Transaction {
    pub fn new(fm: Arc<Mutex<FileMgr>>, lm: Arc<Mutex<LogMgr>>, bm: Arc<Mutex<BufferMgr>>) -> Self {
        static mut NEXT_TX_NUM: Option<Arc<Mutex<u64>>> = None;
        static ONCE: Once = Once::new();

        unsafe {
            ONCE.call_once(|| {
                let next_tx_num = Arc::new(Mutex::new(1));
                NEXT_TX_NUM = Some(next_tx_num);
            });

            Self {
                next_tx_num: NEXT_TX_NUM.clone().unwrap(),
                concur_mgr: ConcurrencyMgr::new(),
                fm,
                bm: Arc::clone(&bm),
                mybuffers: BufferList::new(bm),
            }
        }
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
    fn next_tx_number(&mut self) -> u64 {
        let mut next_tx_num = self.next_tx_num.lock().unwrap();
        *next_tx_num += 1;

        *next_tx_num
    }
}
