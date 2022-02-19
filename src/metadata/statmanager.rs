use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{record::layout::Layout, tx::transaction::Transaction};

use super::tablemanager::TableMgr;

#[derive(Debug, Clone)]
pub struct StatMgr {
    tbl_mgr: TableMgr,
    tablestats: HashMap<String, StatInfo>,
    numcalls: i32,
}

impl StatMgr {
    pub fn new(tbl_mgr: TableMgr, tx: Arc<Mutex<Transaction>>) -> Self {
        panic!("TODO")
    }
    // synchronized
    pub fn get_stat_info(
        &self,
        tblname: &str,
        layout: Layout,
        tx: Arc<Mutex<Transaction>>,
    ) -> StatInfo {
        panic!("TODO")
    }
    // synchronized
    pub fn refresh_statistics(&self, tx: Arc<Mutex<Transaction>>) -> Result<()> {
        panic!("TODO")
    }
    // synchronized
    pub fn calc_table_stats(
        &self,
        tblname: &str,
        layout: Layout,
        tx: Arc<Mutex<Transaction>>,
    ) -> StatInfo {
        panic!("TODO")
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
