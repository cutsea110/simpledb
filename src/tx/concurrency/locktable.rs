use anyhow::Result;
use core::fmt;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    thread,
    time::Duration,
};

use crate::file::block_id::BlockId;

#[derive(Debug)]
enum LockTableError {
    LockAbort,
}

impl std::error::Error for LockTableError {}
impl fmt::Display for LockTableError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LockTableError::LockAbort => {
                write!(f, "lock abort")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct LockTable {
    locks: Arc<Mutex<HashMap<BlockId, i32>>>,
}

impl LockTable {
    pub fn new() -> Self {
        Self {
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    // synchronized
    pub fn s_lock(&mut self, blk: &BlockId) -> Result<()> {
        if let Ok(mut locks) = self.locks.try_lock() {
            if !has_x_lock(&locks, blk) {
                *locks.entry(blk.clone()).or_insert(0) += 1; // will not be negative
                return Ok(());
            }
        }

        Err(From::from(LockTableError::LockAbort))
    }
    // synchronized
    pub fn x_lock(&mut self, blk: &BlockId) -> Result<()> {
        if let Ok(mut locks) = self.locks.try_lock() {
            if !has_other_s_locks(&locks, blk) {
                *locks.entry(blk.clone()).or_insert(-1) = -1; // means eXclusive lock
                return Ok(());
            }
        }
        thread::sleep(Duration::new(1, 0));

        Err(From::from(LockTableError::LockAbort))
    }
    // synchronized
    pub fn unlock(&mut self, blk: &BlockId) -> Result<()> {
        if let Ok(mut locks) = self.locks.lock() {
            let val = get_lock_val(&locks, &blk);
            if val > 1 {
                locks.entry(blk.clone()).and_modify(|e| *e -= 1);
            } else {
                locks.remove(&blk);
                // means notify_all
            }
            return Ok(());
        }

        Err(From::from(LockTableError::LockAbort))
    }
}

fn has_x_lock(locks: &MutexGuard<HashMap<BlockId, i32>>, blk: &BlockId) -> bool {
    get_lock_val(locks, blk) < 0
}
fn has_other_s_locks(locks: &MutexGuard<HashMap<BlockId, i32>>, blk: &BlockId) -> bool {
    get_lock_val(locks, blk) > 1
}
fn get_lock_val(locks: &MutexGuard<HashMap<BlockId, i32>>, blk: &BlockId) -> i32 {
    match locks.get(&blk) {
        Some(&ival) => ival,
        None => 0,
    }
}
