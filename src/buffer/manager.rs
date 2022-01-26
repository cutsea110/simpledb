use anyhow::Result;
use core::fmt;
use std::{
    cell::RefCell,
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
    bufferpool: Vec<Arc<RefCell<Buffer>>>,
    num_available: usize,
    l: Arc<Mutex<()>>,
}

impl BufferMgr {
    pub fn new(fm: Arc<RefCell<FileMgr>>, lm: Arc<RefCell<LogMgr>>, numbuffs: usize) -> Self {
        let bufferpool = (0..numbuffs)
            .map(|_| Arc::new(RefCell::new(Buffer::new(Arc::clone(&fm), Arc::clone(&lm)))))
            .collect();

        Self {
            bufferpool,
            num_available: numbuffs,
            l: Arc::new(Mutex::default()),
        }
    }
    pub fn available(&self) -> Result<usize> {
        if self.l.lock().is_ok() {
            return Ok(self.num_available);
        }

        Err(From::from(BufferMgrError::LockFailed(
            "available".to_string(),
        )))
    }
    pub fn flush_all(&mut self, txnum: i32) -> Result<()> {
        if self.l.lock().is_ok() {
            for i in 0..self.bufferpool.len() {
                if self.bufferpool[i].borrow().modifying_tx() == txnum {
                    self.bufferpool[i].borrow_mut().flush()?;
                }
            }
        }

        Err(From::from(BufferMgrError::LockFailed(
            "flush_all".to_string(),
        )))
    }
    pub fn unpin(&mut self, buff: Arc<RefCell<Buffer>>) -> Result<()> {
        if self.l.lock().is_ok() {
            buff.borrow_mut().unpin();

            if !buff.borrow().is_pinned() {
                self.num_available += 1;
            }

            return Ok(());
        }

        Err(From::from(BufferMgrError::LockFailed("unpin".to_string())))
    }
    pub fn pin(&mut self, blk: &BlockId) -> Result<Arc<RefCell<Buffer>>> {
        if self.l.lock().is_ok() {
            let timestamp = SystemTime::now();

            while !waiting_too_long(timestamp) {
                if let Ok(buff) = self.try_to_pin(blk) {
                    return Ok(buff);
                }
                thread::sleep(Duration::new(1, 0))
            }

            return Err(From::from(BufferMgrError::BufferAbort));
        }

        Err(From::from(BufferMgrError::LockFailed("pin".to_string())))
    }
    fn try_to_pin(&mut self, blk: &BlockId) -> Result<Arc<RefCell<Buffer>>> {
        if let Some(buff) = self.pickup_pinnable_buffer(blk) {
            if !buff.borrow_mut().is_pinned() {
                self.num_available -= 1;
            }
            buff.borrow_mut().pin();

            return Ok(buff);
        }

        Err(From::from(BufferMgrError::BufferAbort))
    }
    fn pickup_pinnable_buffer(&mut self, blk: &BlockId) -> Option<Arc<RefCell<Buffer>>> {
        self.find_existing_buffer(blk).or_else(|| {
            self.choose_unpinned_buffer().and_then(|buff| {
                if buff.borrow_mut().assign_to_block(blk.clone()).is_err() {
                    return None;
                }

                Some(buff)
            })
        })
    }
    fn find_existing_buffer(&self, blk: &BlockId) -> Option<Arc<RefCell<Buffer>>> {
        for i in 0..self.bufferpool.len() {
            if let Some(b) = self.bufferpool[i].borrow().block() {
                if *b == *blk {
                    return Some(Arc::clone(&self.bufferpool[i]));
                }
            }
        }

        None
    }
    fn choose_unpinned_buffer(&mut self) -> Option<Arc<RefCell<Buffer>>> {
        for i in 0..self.bufferpool.len() {
            if !self.bufferpool[i].borrow().is_pinned() {
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
