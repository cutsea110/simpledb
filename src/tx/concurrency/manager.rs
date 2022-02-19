use anyhow::Result;

use core::fmt;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime},
};

use super::locktable::LockTable;
use crate::file::block_id::BlockId;

const MAX_TIME: i64 = 10_000; // 10 sec

#[derive(Debug)]
enum ConcurrencyMgrError {
    LockAbort,
}

impl std::error::Error for ConcurrencyMgrError {}
impl fmt::Display for ConcurrencyMgrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConcurrencyMgrError::LockAbort => {
                write!(f, "lock abort")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConcurrencyMgr {
    // static member (shared by all ConcurrentMgr)
    locktbl: Arc<Mutex<LockTable>>,

    locks: Arc<Mutex<HashMap<BlockId, String>>>,
}

impl ConcurrencyMgr {
    pub fn new(locktbl: Arc<Mutex<LockTable>>) -> Self {
        Self {
            locktbl,
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    pub fn s_lock(&mut self, blk: &BlockId) -> Result<()> {
        if self.locks.lock().unwrap().get(blk).is_none() {
            self.try_s_lock(blk)?;
            self.locks
                .lock()
                .unwrap()
                .insert(blk.clone(), "S".to_string());
        }

        Ok(())
    }
    pub fn x_lock(&mut self, blk: &BlockId) -> Result<()> {
        if !self.has_x_lock(blk) {
            self.s_lock(blk)?;
            self.try_x_lock(blk)?;
            self.locks
                .lock()
                .unwrap()
                .insert(blk.clone(), "X".to_string());
        }

        Ok(())
    }
    pub fn release(&mut self) -> Result<()> {
        for blk in self.locks.lock().unwrap().keys() {
            self.locktbl.lock().unwrap().unlock(blk)?;
        }
        self.locks.lock().unwrap().clear();

        Ok(())
    }
    fn has_x_lock(&self, blk: &BlockId) -> bool {
        if let Some(locktype) = self.locks.lock().unwrap().get(blk) {
            return locktype.eq("X");
        }
        false
    }
    // NOTE: Because locktbl is static member, locking/unlocking the member must be here, not in LockTable's.
    fn try_s_lock(&mut self, blk: &BlockId) -> Result<()> {
        let timestamp = SystemTime::now();

        while !waiting_too_long(timestamp) {
            if let Ok(mut locktbl) = self.locktbl.try_lock() {
                if locktbl.s_lock(blk).is_ok() {
                    return Ok(());
                }
            }
            thread::sleep(Duration::from_millis(1000));
        }

        Err(From::from(ConcurrencyMgrError::LockAbort))
    }
    // NOTE: Because locktbl is static member, locking/unlocking the member must be here, not in LockTable's.
    fn try_x_lock(&mut self, blk: &BlockId) -> Result<()> {
        let timestamp = SystemTime::now();

        while !waiting_too_long(timestamp) {
            if let Ok(mut locktbl) = self.locktbl.try_lock() {
                if locktbl.x_lock(blk).is_ok() {
                    return Ok(());
                }
            }
            thread::sleep(Duration::from_millis(1000));
        }

        Err(From::from(ConcurrencyMgrError::LockAbort))
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
    use crate::server::simpledb::SimpleDB;

    use anyhow::Result;
    use std::path::Path;
    use std::time::Duration;
    use std::{fs, thread};

    // FIXME: this test is flaky. Tx B and Tx C can gone to deadlock.
    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_concurrencytest").exists() {
            fs::remove_dir_all("_concurrencytest")?;
        }

        let simpledb = SimpleDB::new("_concurrencytest", "simpledb.log", 400, 8);

        let mut tx_a = simpledb.new_tx()?;
        let handle1 = thread::spawn(move || {
            let blk1 = BlockId::new("testfile", 1);
            let blk2 = BlockId::new("testfile", 2);
            tx_a.pin(&blk1).unwrap();
            tx_a.pin(&blk2).unwrap();
            println!("Tx A: request slock 1");
            tx_a.get_i32(&blk1, 0).unwrap();
            println!("Tx A: receive slock 1");
            thread::sleep(Duration::new(1, 0));
            println!("Tx A: request slock 2");
            tx_a.get_i32(&blk2, 0).unwrap();
            println!("Tx A: receive slock 2");
            tx_a.commit().unwrap();
        });

        let mut tx_b = simpledb.new_tx()?;
        let handle2 = thread::spawn(move || {
            let blk1 = BlockId::new("testfile", 1);
            let blk2 = BlockId::new("testfile", 2);
            tx_b.pin(&blk1).unwrap();
            tx_b.pin(&blk2).unwrap();
            println!("Tx B: request xlock 2");
            tx_b.set_i32(&blk2, 0, 0, false).unwrap();
            println!("Tx B: receive xlock 2");
            thread::sleep(Duration::new(1, 0));
            println!("Tx B: request slock 1");
            tx_b.get_i32(&blk1, 0).unwrap();
            println!("Tx B: receive slock 1");
            tx_b.commit().unwrap();
        });

        let mut tx_c = simpledb.new_tx()?;
        let handle3 = thread::spawn(move || {
            // Tx B and Tx C can be deadlocked.
            // Letting Tx B go first, prevent deadlock.
            thread::sleep(Duration::new(1, 0));
            let blk1 = BlockId::new("testfile", 1);
            let blk2 = BlockId::new("testfile", 2);
            tx_c.pin(&blk1).unwrap();
            tx_c.pin(&blk2).unwrap();
            println!("Tx C: request xlock 1");
            tx_c.set_i32(&blk1, 0, 0, false).unwrap();
            println!("Tx C: receive xlock 1");
            thread::sleep(Duration::new(1, 0));
            println!("Tx C: request slock 2");
            tx_c.get_i32(&blk2, 0).unwrap();
            println!("Tx C: receive slock 2");
            tx_c.commit().unwrap();
        });

        handle1.join().unwrap();
        handle2.join().unwrap();
        handle3.join().unwrap();

        Ok(())
    }
}
