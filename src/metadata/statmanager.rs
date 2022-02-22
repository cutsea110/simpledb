use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    record::{layout::Layout, tablescan::TableScan},
    tx::transaction::Transaction,
};

use super::tablemanager::TableMgr;

#[derive(Debug, Clone)]
pub struct StatMgr {
    tbl_mgr: TableMgr,
    tablestats: HashMap<String, StatInfo>,
    numcalls: i32,
}

impl StatMgr {
    pub fn new(tbl_mgr: TableMgr, tx: Arc<Mutex<Transaction>>) -> Result<Self> {
        let mut mgr = Self {
            tbl_mgr,
            tablestats: HashMap::new(), // dummy
            numcalls: 0,                // dummy
        };

        mgr.refresh_statistics(tx)?;

        Ok(mgr)
    }
    // synchronized
    pub fn get_stat_info(
        &mut self,
        tblname: &str,
        layout: Arc<Layout>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<StatInfo> {
        self.numcalls += 1;

        if self.numcalls > 100 {
            self.refresh_statistics(Arc::clone(&tx))?;
        }

        if let Some(&si) = self.tablestats.get(tblname) {
            return Ok(si);
        } else {
            let si = self.calc_table_stats(tblname, layout, tx)?;
            self.tablestats.insert(tblname.to_string(), si);
            return Ok(si);
        }
    }
    // synchronized
    pub fn refresh_statistics(&mut self, tx: Arc<Mutex<Transaction>>) -> Result<()> {
        self.tablestats = HashMap::new();
        self.numcalls = 0;
        let tcatlayout = self.tbl_mgr.get_layout("tblcat", Arc::clone(&tx))?;
        let mut tcat = TableScan::new(Arc::clone(&tx), "tblcat", tcatlayout)?;
        while tcat.next() {
            let tblname = tcat.get_string("tblname")?;
            let layout = self.tbl_mgr.get_layout(&tblname, Arc::clone(&tx))?;
            let si = self.calc_table_stats(&tblname, layout, Arc::clone(&tx))?;
            self.tablestats.insert(tblname, si);
        }
        tcat.close()?;

        Ok(())
    }
    // synchronized
    pub fn calc_table_stats(
        &self,
        tblname: &str,
        layout: Arc<Layout>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<StatInfo> {
        let mut num_recs = 0;
        let mut numblocks = 0;
        let mut ts = TableScan::new(tx, tblname, layout)?;
        while ts.next() {
            num_recs += 1;
            numblocks = ts.get_rid().block_number() + 1;
        }
        ts.close()?;

        Ok(StatInfo::new(numblocks, num_recs))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StatInfo {
    num_blocks: i32,
    num_recs: i32,
}

impl StatInfo {
    pub fn new(numblocks: i32, numrecs: i32) -> Self {
        Self {
            num_blocks: numblocks,
            num_recs: numrecs,
        }
    }
    pub fn blocks_accessed(&self) -> i32 {
        self.num_blocks
    }
    pub fn records_output(&self) -> i32 {
        self.num_recs
    }
    pub fn distinct_values(&self, _fldname: &str) -> i32 {
        1 + (self.num_recs / 3) // This is wildly inaccurate.
    }
}
