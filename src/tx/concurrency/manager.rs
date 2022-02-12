use anyhow::Result;

use std::{
    cell::RefCell,
    collections::HashMap,
    rc::Rc,
    sync::{Arc, Mutex, Once},
};

use super::locktable::LockTable;
use crate::file::block_id::BlockId;

#[derive(Debug, Clone)]
pub struct ConcurrencyMgr {
    // static member (shared by all ConcurrentMgr)
    locktbl: Arc<Mutex<LockTable>>,

    locks: Rc<RefCell<HashMap<BlockId, String>>>,
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
                locks: Rc::new(RefCell::new(HashMap::new())),
            }
        }
    }
    pub fn s_lock(&mut self, blk: &BlockId) -> Result<()> {
        if self.locks.borrow().get(blk).is_none() {
            self.locktbl.lock().unwrap().s_lock(blk)?;
            self.locks.borrow_mut().insert(blk.clone(), "S".to_string());
        }

        Ok(())
    }
    pub fn x_lock(&mut self, blk: &BlockId) -> Result<()> {
        if !self.has_x_lock(blk) {
            self.s_lock(blk)?;
            self.locktbl.lock().unwrap().x_lock(blk)?;
            self.locks.borrow_mut().insert(blk.clone(), "X".to_string());
        }

        Ok(())
    }
    pub fn release(&mut self) -> Result<()> {
        for blk in self.locks.borrow().keys() {
            self.locktbl.lock().unwrap().unlock(blk)?;
        }
        self.locks.borrow_mut().clear();

        Ok(())
    }
    fn has_x_lock(&self, blk: &BlockId) -> bool {
        if let Some(locktype) = self.locks.borrow().get(blk) {
            return locktype.eq("X");
        }
        false
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn unit_test() {
        // TODO: ConcurrencyTest p128
    }
}
