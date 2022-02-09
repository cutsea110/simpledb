use anyhow::Result;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Once},
};

use super::locktable::LockTable;
use crate::file::block_id::BlockId;

pub struct ConcurrencyMgr {
    // static member (shared by all ConcurrentMgr)
    locktbl: Arc<Mutex<LockTable>>,
    locks: HashMap<BlockId, String>,
}

impl ConcurrencyMgr {
    // emulate for static member locktbl
    pub fn new() -> Self {
        // make locktbl a static member by singleton pattern
        // ref.) https://stackoverflow.com/questions/27791532/how-do-i-create-a-global-mutable-singleton
        static mut LOCKTBL: Option<Arc<Mutex<LockTable>>> = None;
        static ONCE: Once = Once::new();

        unsafe {
            ONCE.call_once(|| {
                let locktbl = Arc::new(Mutex::new(LockTable::new()));
                LOCKTBL = Some(locktbl);
            });

            Self {
                locktbl: LOCKTBL.clone().unwrap(),
                locks: HashMap::new(),
            }
        }
    }
    pub fn s_lock(&mut self, blk: BlockId) -> Result<()> {
        if self.locks.get(&blk).is_none() {
            self.locktbl.lock().unwrap().s_lock(blk.clone())?;
            self.locks.insert(blk, "S".to_string());
        }

        Ok(())
    }
    pub fn x_lock(&mut self, blk: BlockId) -> Result<()> {
        if !self.has_x_lock(&blk) {
            self.s_lock(blk.clone())?;
            self.locktbl.lock().unwrap().x_lock(blk.clone())?;
            self.locks.insert(blk, "X".to_string());
        }

        Ok(())
    }
    pub fn release(&mut self) -> Result<()> {
        for blk in self.locks.keys() {
            self.locktbl.lock().unwrap().unlock(blk.clone())?;
        }
        self.locks.clear();

        Ok(())
    }
    fn has_x_lock(&self, blk: &BlockId) -> bool {
        let locktype = self.locks.get(blk);
        return locktype.is_some() && locktype.unwrap().eq("X");
    }
}
