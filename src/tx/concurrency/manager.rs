use std::collections::HashMap;
use std::sync::{Arc, LockResult, Mutex, Once, ONCE_INIT};

use super::locktable::LockTable;
use crate::file::block_id::BlockId;
use crate::tx::concurrency::locktable;

pub struct ConcurrencyMgr {
    // static member (shared by all ConcurrentMgr)
    locktbl: Arc<Mutex<LockTable>>,
    locks: HashMap<BlockId, String>,
}

impl ConcurrencyMgr {
    pub fn new() -> Self {
        // make locktbl a static member by singleton pattern
        // ref.) https://stackoverflow.com/questions/27791532/how-do-i-create-a-global-mutable-singleton
        static mut SINGLETON: Option<Arc<Mutex<LockTable>>> = None;
        static ONCE: Once = ONCE_INIT;

        unsafe {
            ONCE.call_once(|| {
                let singleton = Arc::new(Mutex::new(LockTable::new()));
                SINGLETON = Some(singleton);
            });

            Self {
                locktbl: SINGLETON.clone().unwrap(),
                locks: HashMap::new(),
            }
        }
    }
}
