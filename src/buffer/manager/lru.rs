use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime},
};

use super::{BufferMgr, BufferMgrError, MAX_TIME};
use crate::{
    buffer::buffer::Buffer,
    file::{block_id::BlockId, manager::FileMgr},
    log::manager::LogMgr,
};

#[derive(Debug, Clone)]
pub struct LruBufferMgr {
    bufferpool: Vec<Arc<Mutex<Buffer>>>,
    num_available: Arc<Mutex<usize>>,
    // extends statistics by exercise 4.18
    num_of_total_pinned: u32,
    num_of_total_unpinned: u32,
    num_of_cache_hits: u32,
    num_of_buffer_assigned: u32,
    // extends by exercise 4.17
    // Let only try_to_pin to handle this HashMap.
    assigned_block_ids: HashMap<BlockId, Arc<Mutex<Buffer>>>,
    // extends by exercise 4.14
    unassigned_buffers: Vec<Arc<Mutex<Buffer>>>,
}

impl LruBufferMgr {
    pub fn new(fm: Arc<Mutex<FileMgr>>, lm: Arc<Mutex<LogMgr>>, numbuffs: usize) -> Self {
        let bufferpool: Vec<Arc<Mutex<Buffer>>> = (0..numbuffs)
            .map(|_| Arc::new(Mutex::new(Buffer::new(Arc::clone(&fm), Arc::clone(&lm)))))
            .collect();
        // Initially all buffers are unpinned.
        let unassigned_buffers = (0..numbuffs).map(|i| Arc::clone(&bufferpool[i])).collect();

        Self {
            bufferpool,
            num_available: Arc::new(Mutex::new(numbuffs)),
            num_of_total_pinned: 0,
            num_of_total_unpinned: 0,
            num_of_cache_hits: 0,
            num_of_buffer_assigned: 0,
            assigned_block_ids: HashMap::new(),
            unassigned_buffers,
        }
    }
    // TODO: fix for thread safe
    fn try_to_pin(&mut self, blk: &BlockId) -> Result<Arc<Mutex<Buffer>>> {
        let mut buff = self.find_existing_buffer(blk);
        match buff {
            Some(_) => {
                // for statistics
                self.num_of_cache_hits += 1;
            }
            None => {
                let found = self.choose_unpinned_buffer();
                match found {
                    None => {
                        return Err(From::from(BufferMgrError::BufferAbort));
                    }
                    Some((_, b)) => {
                        buff = Some(Arc::clone(&b));

                        // update assigned_block_ids
                        if let Some(blk) = buff.as_ref().unwrap().lock().unwrap().block() {
                            self.assigned_block_ids.remove(blk);
                        }
                        self.assigned_block_ids
                            .insert(blk.clone(), Arc::clone(&buff.as_ref().unwrap()));
                    }
                }

                let mut b = buff.as_ref().unwrap().lock().unwrap();
                b.assign_to_block(blk.clone())?;
                // for statistics
                self.num_of_buffer_assigned += 1;
            }
        }

        let mut b = buff.as_ref().unwrap().lock().unwrap();
        if !b.is_pinned() {
            *(self.num_available.lock().unwrap()) -= 1;
        }
        b.pin();

        drop(b); // release lock
        Ok(buff.unwrap())
    }
    fn try_to_unpin(&mut self, buff: Arc<Mutex<Buffer>>) -> Result<()> {
        let blk: Option<BlockId> = buff.lock().unwrap().block().map(|b| b.clone());

        if let Some(blk) = blk {
            if let Some(i) = self
                .unassigned_buffers
                .iter()
                .enumerate()
                .find(|(_, x)| x.lock().unwrap().block() == Some(&blk))
                .map(|(i, _)| i)
            {
                self.unassigned_buffers.remove(i);
            }
        }

        self.unassigned_buffers.push(buff);

        Ok(())
    }
    fn find_existing_buffer(&self, blk: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
        self.assigned_block_ids.get(blk).map(|b| Arc::clone(b))
    }
    // The LRU Strategy
    fn choose_unpinned_buffer(&mut self) -> Option<(usize, Arc<Mutex<Buffer>>)> {
        self.unassigned_buffers
            .iter()
            .enumerate()
            .find(|(_, x)| !x.lock().unwrap().is_pinned())
            .map(|(i, x)| (i, Arc::clone(x)))
    }
}
impl BufferMgr for LruBufferMgr {
    // synchronized
    fn available(&self) -> usize {
        *(self.num_available.lock().unwrap())
    }
    // synchronized
    fn flush_all(&mut self, txnum: i32) -> Result<()> {
        for i in 0..self.bufferpool.len() {
            let mut buff = self.bufferpool[i].lock().unwrap();
            if buff.modifying_tx() == txnum {
                buff.flush()?;
            }
        }

        Ok(())
    }
    // synchronized
    fn unpin(&mut self, buff: Arc<Mutex<Buffer>>) -> Result<()> {
        // The LRU strategy
        // update unassigned_buffers
        self.try_to_unpin(Arc::clone(&buff))?;

        let mut b = buff.lock().unwrap();

        b.unpin();

        // for statistics
        self.num_of_total_unpinned += 1;

        if !b.is_pinned() {
            *(self.num_available.lock().unwrap()) += 1;
        }
        Ok(())
    }
    // synchronized
    fn pin(&mut self, blk: &BlockId) -> Result<Arc<Mutex<Buffer>>> {
        let timestamp = SystemTime::now();

        while !waiting_too_long(timestamp) {
            if let Ok(buff) = self.try_to_pin(blk) {
                // for statistics
                self.num_of_total_pinned += 1;

                return Ok(buff);
            }
            thread::sleep(Duration::new(1, 0))
        }

        return Err(From::from(BufferMgrError::BufferAbort));
    }
    // extends by exercise 4.18
    fn nums_total_pinned_unpinned(&self) -> (u32, u32) {
        (self.num_of_total_pinned, self.num_of_total_unpinned)
    }
    // extends by exercise 4.18
    fn buffer_cache_hit_assigned(&self) -> (u32, u32) {
        (self.num_of_cache_hits, self.num_of_buffer_assigned)
    }
}

fn waiting_too_long(starttime: SystemTime) -> bool {
    let now = SystemTime::now();
    let diff = now.duration_since(starttime).unwrap();

    diff.as_millis() as i64 > MAX_TIME
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use std::fs;
    use std::path::Path;

    use super::*;

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/buffermgrtest/lru").exists() {
            fs::remove_dir_all("_test/buffermgrtest/lru")?;
        }

        let fm = Arc::new(Mutex::new(FileMgr::new("_test/buffermgrtest/lru", 400)?));
        let lm = Arc::new(Mutex::new(LogMgr::new(Arc::clone(&fm), "simpledb.log")?));

        let mut bm = LruBufferMgr::new(fm, lm, 3);

        let mut buff: Vec<Option<Arc<Mutex<Buffer>>>> = vec![None; 6];
        buff[0] = bm.pin(&BlockId::new("testfile", 0))?.into();
        buff[1] = bm.pin(&BlockId::new("testfile", 1))?.into();
        buff[2] = bm.pin(&BlockId::new("testfile", 2))?.into();
        bm.unpin(Arc::clone(&buff[1].clone().unwrap()))?;
        buff[1] = None;

        buff[3] = bm.pin(&BlockId::new("testfile", 0))?.into();
        buff[4] = bm.pin(&BlockId::new("testfile", 1))?.into();

        assert_eq!(bm.available(), 0);

        println!("Attempting to pin block 3...");
        let result = bm.pin(&BlockId::new("testfile", 3));
        assert!(result.is_err());

        bm.unpin(Arc::clone(&buff[2].clone().unwrap()))?;
        buff[2] = None;
        buff[5] = bm.pin(&BlockId::new("testfile", 3))?.into(); // now this works

        println!("Check buff");
        // 0
        assert!(buff[0].is_some());
        {
            let result = buff[0].as_ref().unwrap().lock().unwrap();
            assert_eq!(result.block(), Some(&BlockId::new("testfile", 0)));
        }
        // 1
        assert!(buff[1].is_none());
        // 2
        assert!(buff[2].is_none());
        // 3
        assert!(buff[3].is_some());
        {
            let result = buff[3].as_ref().unwrap().lock().unwrap();
            assert_eq!(result.block(), Some(&BlockId::new("testfile", 0)));
        }
        // 4
        assert!(buff[4].is_some());
        {
            let result = buff[4].as_ref().unwrap().lock().unwrap();
            assert_eq!(result.block(), Some(&BlockId::new("testfile", 1)));
        }
        // 5
        assert!(buff[5].is_some());
        {
            let result = buff[5].as_ref().unwrap().lock().unwrap();
            assert_eq!(result.block(), Some(&BlockId::new("testfile", 3)));
        }

        println!("Final buffer Allocation:");
        // bufferpool
        let b = bm.bufferpool[0].lock().unwrap();
        assert_eq!(b.block(), Some(&BlockId::new("testfile", 0)));
        assert_eq!(b.is_pinned(), true);
        let b = bm.bufferpool[1].lock().unwrap();
        assert_eq!(b.block(), Some(&BlockId::new("testfile", 1)));
        assert_eq!(b.is_pinned(), true);
        let b = bm.bufferpool[2].lock().unwrap();
        assert_eq!(b.block(), Some(&BlockId::new("testfile", 3)));
        assert_eq!(b.is_pinned(), true);

        Ok(())
    }

    #[test]
    fn replace_strategy_test() -> Result<()> {
        if Path::new("_test/buffermgrtest/lrustrategy").exists() {
            fs::remove_dir_all("_test/buffermgrtest/lrustrategy")?;
        }

        let fm = Arc::new(Mutex::new(FileMgr::new(
            "_test/buffermgrtest/lrustrategy",
            400,
        )?));
        let lm = Arc::new(Mutex::new(LogMgr::new(Arc::clone(&fm), "simpledb.log")?));

        let mut bm = LruBufferMgr::new(fm, lm, 4);

        // p91 senario
        // pin(10); pin(20); pin(30); pin(40); unpin(20);
        // pin(50); unpin(40); unpin(10); unpin(30); unpin(50);
        let mut buff: Vec<Option<Arc<Mutex<Buffer>>>> = vec![None; 7];
        buff[0] = bm.pin(&BlockId::new("testfile", 10))?.into();
        buff[1] = bm.pin(&BlockId::new("testfile", 20))?.into();
        buff[2] = bm.pin(&BlockId::new("testfile", 30))?.into();
        buff[3] = bm.pin(&BlockId::new("testfile", 40))?.into();
        bm.unpin(Arc::clone(&buff[1].clone().unwrap()))?; // unpin 20
        buff[1] = None;
        buff[4] = bm.pin(&BlockId::new("testfile", 50))?.into();
        bm.unpin(Arc::clone(&buff[3].clone().unwrap()))?; // unpin 40
        buff[3] = None;
        bm.unpin(Arc::clone(&buff[0].clone().unwrap()))?; // unpin 10
        buff[0] = None;
        bm.unpin(Arc::clone(&buff[2].clone().unwrap()))?; // unpin 30
        buff[2] = None;
        bm.unpin(Arc::clone(&buff[4].clone().unwrap()))?; // unpin 50
        buff[4] = None;
        // p91 senario
        // pin(60); pin(70);
        buff[5] = bm.pin(&BlockId::new("testfile", 60))?.into();
        buff[6] = bm.pin(&BlockId::new("testfile", 70))?.into();

        assert_eq!(bm.available(), 2);
        println!("Final buffer Allocation:");
        // bufferpool
        let b = bm.bufferpool[0].lock().unwrap();
        assert_eq!(b.block(), Some(&BlockId::new("testfile", 70)));
        assert_eq!(b.is_pinned(), true);
        let b = bm.bufferpool[1].lock().unwrap();
        assert_eq!(b.block(), Some(&BlockId::new("testfile", 50)));
        assert_eq!(b.is_pinned(), false);
        let b = bm.bufferpool[2].lock().unwrap();
        assert_eq!(b.block(), Some(&BlockId::new("testfile", 30)));
        assert_eq!(b.is_pinned(), false);
        let b = bm.bufferpool[3].lock().unwrap();
        assert_eq!(b.block(), Some(&BlockId::new("testfile", 60)));
        assert_eq!(b.is_pinned(), true);

        Ok(())
    }
}
