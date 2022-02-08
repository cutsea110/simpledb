use anyhow::Result;
use core::fmt;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime},
};

use crate::file::block_id::BlockId;

const MAX_TIME: i64 = 10_000; // 10 sec

#[derive(Debug)]
enum LockTableError {
    LockAbort,
    LockFailed(String),
}

impl std::error::Error for LockTableError {}
impl fmt::Display for LockTableError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LockTableError::LockAbort => {
                write!(f, "lock abort")
            }
            LockTableError::LockFailed(s) => {
                write!(f, "lock failed: {}", s)
            }
        }
    }
}

pub struct LockTable {
    locks: HashMap<BlockId, i32>,
    l: Arc<Mutex<()>>,
}

impl LockTable {
    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
            l: Arc::new(Mutex::default()),
        }
    }
    pub fn s_lock(&mut self, blk: BlockId) -> Result<()> {
        if self.l.lock().is_ok() {
            let timestamp = SystemTime::now();

            while self.has_x_lock(&blk) {
                if waiting_too_long(timestamp) {
                    return Err(From::from(LockTableError::LockAbort));
                }
                thread::sleep(Duration::new(1, 0));
            }
            let val = self.get_lock_val(&blk); // will not be negative
            *self.locks.entry(blk).or_insert(val.try_into().unwrap()) = val;

            return Ok(());
        }

        Err(From::from(LockTableError::LockFailed("s_lock".to_string())))
    }
    pub fn x_lock(&mut self, blk: BlockId) -> Result<()> {
        if self.l.lock().is_ok() {
            let timestamp = SystemTime::now();

            while self.has_other_s_locks(&blk) {
                if waiting_too_long(timestamp) {
                    return Err(From::from(LockTableError::LockAbort));
                }
                thread::sleep(Duration::new(1, 0));
            }
            self.locks.entry(blk).or_insert(-1); // means eXclusive lock

            return Ok(());
        }

        Err(From::from(LockTableError::LockFailed("x_lock".to_string())))
    }
    pub fn unlock(&mut self, blk: BlockId) -> Result<()> {
        if self.l.lock().is_ok() {
            let val = self.get_lock_val(&blk);
            if val > 1 {
                self.locks.entry(blk).or_insert(val - 1);
            } else {
                self.locks.remove(&blk);
                // means notify_all
            }

            return Ok(());
        }

        Err(From::from(LockTableError::LockFailed("x_lock".to_string())))
    }
    fn has_x_lock(&self, blk: &BlockId) -> bool {
        self.get_lock_val(blk) < 0
    }
    fn has_other_s_locks(&self, blk: &BlockId) -> bool {
        self.get_lock_val(blk) > 1
    }
    fn get_lock_val(&self, blk: &BlockId) -> i32 {
        match self.locks.get(&blk) {
            Some(&ival) => ival,
            None => 0,
        }
    }
}

fn waiting_too_long(starttime: SystemTime) -> bool {
    let now = SystemTime::now();
    let diff = now.duration_since(starttime).unwrap();

    diff.as_millis() as i64 > MAX_TIME
}
