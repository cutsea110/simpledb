use anyhow::Result;
use core::fmt;
use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use super::buffer::Buffer;
use crate::file::block_id::BlockId;

// implements of BufferMgr trait
pub mod clock; // by exercise 4.14
pub mod fifo; // by exercise 4.14
pub mod lru; // by exercise 4.14
pub mod naive;
pub mod naive_up; // with unmodified preferd
pub mod naivebis; // by exercise 4.17
pub mod naivebis_up; // with unmodified preferd

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
    // extends for statistics by exercise 4.18
    fn nums_total_pinned_unpinned(&self) -> (u32, u32);
    fn buffer_cache_hit_assigned(&self) -> (u32, u32);
}

impl Debug for dyn BufferMgr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BufferMgr")
    }
}
