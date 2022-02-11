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
            println!("pin: BUSY LOOP");
            thread::sleep(Duration::new(1, 0))
        }

        return Err(From::from(BufferMgrError::BufferAbort));
    }
    // TODO: fix for thread safe
    fn try_to_pin(&mut self, blk: &BlockId) -> Result<Arc<Mutex<Buffer>>> {
        println!("try_to_pin: CALL find_existing_buffer");
        let mut buff = self.find_existing_buffer(blk);
        if buff.is_none() {
            println!("try_to_pin: NOT found and CALL choose_unpinned_buffer");
            buff = self.choose_unpinned_buffer();
            if buff.is_none() {
                println!("try_to_pin: NOT choose_unpinned_buffer");
                return Err(From::from(BufferMgrError::BufferAbort));
            }
            println!("try_to_pin: choose_unpinned_buffer");
            let mut b = buff.as_ref().unwrap().lock().unwrap();
            println!("try_to_pin: assign_to_block");
            b.assign_to_block(blk.clone())?;
        }

        println!("try_to_pin: is_pinned?");
        let mut b = buff.as_ref().unwrap().lock().unwrap();
        if !b.is_pinned() {
            println!("try_to_pin: NOT is_pinned");
            *(self.num_available.lock().unwrap()) -= 1;
        }
        println!("try_to_pin: pin!!");
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
    use super::*;

    use anyhow::Result;
    use std::fs;
    use std::path::Path;

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_buffermgrtest").exists() {
            fs::remove_dir_all("_buffermgrtest")?;
        }

        let fm = Arc::new(Mutex::new(FileMgr::new("_buffermgrtest", 400)?));
        let lm = Arc::new(Mutex::new(LogMgr::new(Arc::clone(&fm), "testfile")?));

        let mut bm = BufferMgr::new(Arc::clone(&fm), Arc::clone(&lm), 3);

        let buff0 = bm.pin(&BlockId::new("testfile", 0));
        let buff1 = bm.pin(&BlockId::new("testfile", 1)).unwrap();
        let buff2 = bm.pin(&BlockId::new("testfile", 2)).unwrap();
        bm.unpin(Arc::clone(&buff1))?;

        let buff3 = bm.pin(&BlockId::new("testfile", 0));
        let buff4 = bm.pin(&BlockId::new("testfile", 1));
        println!("Available buffers: {:?}", bm.available());

        print!("Attempting to pin block 3...");
        if let Ok(_) = bm.pin(&BlockId::new("testfile", 3)) {
            // couldn't come here!
            println!("Succeed!");
        } else {
            println!("Failed!");
        }
        bm.unpin(Arc::clone(&buff2))?;
        let buff5 = bm.pin(&BlockId::new("testfile", 3)).unwrap(); // now this works

        println!("Final buffer Allocation:");
        println!(
            "buff0 pinned to block {:?}",
            buff0.unwrap().lock().unwrap().block()
        );
        println!("buff1 pinned to block {:?}", buff1.lock().unwrap().block());
        println!("buff2 pinned to block {:?}", buff2.lock().unwrap().block());
        println!(
            "buff3 pinned to block {:?}",
            buff3.unwrap().lock().unwrap().block()
        );
        println!(
            "buff4 pinned to block {:?}",
            buff4.unwrap().lock().unwrap().block()
        );
        println!("buff5 pinned to block {:?}", buff5.lock().unwrap().block());

        Ok(())
    }
}
