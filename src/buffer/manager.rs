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

#[derive(Debug, Clone)]
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
    pub fn available(&self) -> usize {
        *(self.num_available.lock().unwrap())
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
    // TODO: fix for thread safe
    fn try_to_pin(&mut self, blk: &BlockId) -> Result<Arc<Mutex<Buffer>>> {
        let mut buff = self.find_existing_buffer(blk);
        if buff.is_none() {
            buff = self.choose_unpinned_buffer();
            if buff.is_none() {
                return Err(From::from(BufferMgrError::BufferAbort));
            }
            let mut b = buff.as_ref().unwrap().lock().unwrap();
            b.assign_to_block(blk.clone())?;
        }

        let mut b = buff.as_ref().unwrap().lock().unwrap();
        if !b.is_pinned() {
            *(self.num_available.lock().unwrap()) -= 1;
        }
        b.pin();

        drop(b); // release lock
        Ok(Arc::clone(&buff.unwrap()))
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

#[cfg(test)]
mod tests {
    use crate::server::simpledb::SimpleDB;

    use super::*;

    use anyhow::Result;
    use std::fs;
    use std::path::Path;

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_buffermgrtest").exists() {
            fs::remove_dir_all("_buffermgrtest")?;
        }

        let simpledb = SimpleDB::new_with("_buffermgrtest", 400, 3);

        let bm = simpledb.buffer_mgr();
        let mut bm = bm.lock().unwrap();

        let mut buff: Vec<Option<Arc<Mutex<Buffer>>>> = vec![None; 6];
        buff[0] = bm.pin(&BlockId::new("testfile", 0))?.into();
        buff[1] = bm.pin(&BlockId::new("testfile", 1))?.into();
        buff[2] = bm.pin(&BlockId::new("testfile", 2))?.into();
        bm.unpin(Arc::clone(&buff[1].clone().unwrap()))?;
        buff[1] = None;

        buff[3] = bm.pin(&BlockId::new("testfile", 0))?.into();
        buff[4] = bm.pin(&BlockId::new("testfile", 1))?.into();
        println!("Available buffers: {:?}", bm.available());

        println!("Attempting to pin block 3...");
        if let Ok(_) = bm.pin(&BlockId::new("testfile", 3)) {
            // couldn't come here!
            println!("Succeed!");
        } else {
            println!("Failed!");
        }
        bm.unpin(Arc::clone(&buff[2].clone().unwrap()))?;
        buff[2] = None;
        buff[5] = bm.pin(&BlockId::new("testfile", 3))?.into(); // now this works

        println!("Final buffer Allocation:");
        for i in 0..buff.len() {
            if let Some(b) = buff[i].clone() {
                println!(
                    "buff[{:?}] pinned to block {:?}",
                    i,
                    b.lock().unwrap().block()
                );
            }
        }

        Ok(())
    }
}
