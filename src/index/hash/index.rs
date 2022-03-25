use anyhow::Result;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
};

use crate::{
    index::{Index, IndexError},
    query::{constant::Constant, scan::Scan, updatescan::UpdateScan},
    record::{layout::Layout, rid::RID, tablescan::TableScan},
    tx::transaction::Transaction,
};

pub const NUM_BUCKETS: i32 = 100;

pub struct HashIndex {
    tx: Arc<Mutex<Transaction>>,
    idxname: String,
    layout: Arc<Layout>,
    searchkey: Option<Constant>,
    ts: Option<TableScan>,
}

impl HashIndex {
    pub fn new(tx: Arc<Mutex<Transaction>>, idxname: String, layout: Arc<Layout>) -> Self {
        Self {
            tx,
            idxname,
            layout,
            searchkey: None,
            ts: None,
        }
    }
    pub fn search_cost(numblocks: i32, _rpb: i32) -> i32 {
        numblocks / NUM_BUCKETS
    }
}

impl Index for HashIndex {
    fn before_first(&mut self, searchkey: Constant) -> Result<()> {
        self.close()?;
        self.searchkey = Some(searchkey.clone());
        let mut hasher = DefaultHasher::new();
        searchkey.hash(&mut hasher);
        let bucket = hasher.finish() % NUM_BUCKETS as u64;
        let tblname = format!("{}{}", self.idxname, bucket);
        self.ts = TableScan::new(Arc::clone(&self.tx), &tblname, Arc::clone(&self.layout)).ok();

        Ok(())
    }
    fn next(&mut self) -> bool {
        if let Some(ts) = self.ts.as_mut() {
            while ts.next() {
                if ts.get_val("dataval").ok() == self.searchkey {
                    return true;
                }
            }
            return false;
        }

        false
    }
    fn get_data_rid(&mut self) -> Result<RID> {
        if let Some(ts) = self.ts.as_mut() {
            let blknum = ts.get_i32("block")?;
            let id = ts.get_i32("id")?;

            return Ok(RID::new(blknum, id));
        }

        Err(From::from(IndexError::NoTableScan))
    }
    fn insert(&mut self, val: Constant, rid: RID) -> Result<()> {
        self.before_first(val.clone())?;
        if let Some(ts) = self.ts.as_mut() {
            ts.insert()?;
            ts.set_i32("block", rid.block_number())?;
            ts.set_i32("id", rid.slot())?;
            ts.set_val("dataval", val)?;
            return Ok(());
        }

        Err(From::from(IndexError::NoTableScan))
    }
    fn delete(&mut self, val: Constant, rid: RID) -> Result<()> {
        self.before_first(val)?;
        while self.next() {
            if self.get_data_rid().unwrap() == rid {
                self.ts.as_mut().unwrap().delete()?;
                return Ok(());
            }
        }

        Err(From::from(IndexError::NoTableScan))
    }
    fn close(&mut self) -> Result<()> {
        if let Some(ts) = self.ts.as_mut() {
            return ts.close();
        }

        Ok(())
    }
}
