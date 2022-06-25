use anyhow::Result;
use core::fmt;
use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use super::buffer::Buffer;
use crate::file::block_id::BlockId;

// implements of BufferMgr trait
pub mod naive;

const MAX_TIME: i64 = 10_000; // 10 seconds

#[derive(Debug)]
enum BufferMgrError {
    BufferAbort,
}

impl std::error::Error for BufferMgrError {}
impl fmt::Display for BufferMgrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BufferMgrError::BufferAbort => {
                write!(f, "buffer abort")
            }
        }
    }
}

pub trait BufferMgr {
    fn available(&self) -> usize;
    fn flush_all(&mut self, txnum: i32) -> Result<()>;
    fn unpin(&mut self, buff: Arc<Mutex<Buffer>>) -> Result<()>;
    fn pin(&mut self, blk: &BlockId) -> Result<Arc<Mutex<Buffer>>>;
}

impl Debug for dyn BufferMgr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BufferMgr")
    }
}
