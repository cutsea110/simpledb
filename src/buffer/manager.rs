use anyhow::Result;
use core::fmt;
use std::{
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime},
};

use super::buffer::Buffer;
use crate::{
    file::{block_id::BlockId, manager::FileMgr},
    log::manager::LogMgr,
};

const MAX_TIME: i64 = 10_000; // 10 seconds

#[derive(Debug)]
enum BufferMgrError {
    LockFailed(String),
    BufferAbort,
}

impl std::error::Error for BufferMgrError {}
impl fmt::Display for BufferMgrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BufferMgrError::LockFailed(s) => {
                write!(f, "lock failed: {}", s)
            }
            BufferMgrError::BufferAbort => {
                write!(f, "buffer abort")
            }
        }
    }
}

pub struct BufferMgr {
    bufferpool: Vec<Arc<Mutex<Buffer>>>,
    num_available: Arc<Mutex<usize>>,
}

impl BufferMgr {
    pub fn new(fm: Arc<Mutex<FileMgr>>, lm: Arc<Mutex<LogMgr>>, numbuffs: usize) -> Self {
        let bufferpool = (0..numbuffs)
            .map(|_| Arc::new(Mutex::new(Buffer::new(Arc::clone(&fm), Arc::clone(&lm)))))
            .collect();

        Self {
            bufferpool,
            num_available: Arc::new(Mutex::new(numbuffs)),
        }
    }
    // synchronized
    pub fn available(&self) -> Result<usize> {
        let num = self.num_available.lock().unwrap();
        return Ok(*num);
    }
    // synchronized
    pub fn flush_all(&mut self, txnum: i32) -> Result<()> {
        for i in 0..self.bufferpool.len() {
            let mut buff = self.bufferpool[i].lock().unwrap();
            if buff.modifying_tx() == txnum {
                buff.flush()?;
            }
        }

        Ok(())
    }
    // synchronized
    pub fn unpin(&mut self, buff: Arc<Mutex<Buffer>>) -> Result<()> {
        let mut b = buff.lock().unwrap();

        b.unpin();

        if !b.is_pinned() {
            *(self.num_available.lock().unwrap()) += 1;
        }

        Ok(())
    }
    // synchronized
    pub fn pin(&mut self, blk: &BlockId) -> Result<Arc<Mutex<Buffer>>> {
        let timestamp = SystemTime::now();

        while !waiting_too_long(timestamp) {
            if let Ok(buff) = self.try_to_pin(blk) {
                return Ok(buff);
            }
            thread::sleep(Duration::new(1, 0))
        }

        return Err(From::from(BufferMgrError::BufferAbort));
    }
    fn try_to_pin(&mut self, blk: &BlockId) -> Result<Arc<Mutex<Buffer>>> {
        if let Some(buff) = self.pickup_pinnable_buffer(blk) {
            let mut b = buff.lock().unwrap();

            b.assign_to_block(blk.clone())?;

            if !b.is_pinned() {
                *(self.num_available.lock().unwrap()) -= 1;
            }
            b.pin();

            drop(b); // release lock
            return Ok(buff);
        }

        Err(From::from(BufferMgrError::BufferAbort))
    }
    fn pickup_pinnable_buffer(&mut self, blk: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
        if let Some(buff) = self.find_existing_buffer(blk) {
            return Some(buff);
        }

        if let Some(buff) = self.choose_unpinned_buffer() {
            let mut b = buff.lock().unwrap();

            if let Err(e) = b.assign_to_block(blk.clone()) {
                eprintln!("failed to assign to block: {}", e);
                return None;
            }

            drop(b);
            return Some(buff);
        }

        None
    }
    fn find_existing_buffer(&self, blk: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
        for i in 0..self.bufferpool.len() {
            let buff = self.bufferpool[i].lock().unwrap();

            if let Some(b) = buff.block() {
                if *b == *blk {
                    return Some(Arc::clone(&self.bufferpool[i]));
                }
            }
        }

        None
    }
    // The Naive Strategy
    fn choose_unpinned_buffer(&mut self) -> Option<Arc<Mutex<Buffer>>> {
        for i in 0..self.bufferpool.len() {
            let buff = self.bufferpool[i].lock().unwrap();

            if !buff.is_pinned() {
                return Some(Arc::clone(&self.bufferpool[i]));
            }
        }

        None
    }
}

fn waiting_too_long(starttime: SystemTime) -> bool {
    let now = SystemTime::now();
    let diff = now.duration_since(starttime).unwrap();

    diff.as_millis() as i64 > MAX_TIME
}
