use anyhow::Result;
use std::sync::{Arc, Mutex};

use crate::{
    metadata::{manager::MetadataMgr, statmanager::StatInfo},
    query::scan::Scan,
    record::{layout::Layout, schema::Schema, tablescan::TableScan},
    tx::transaction::Transaction,
};

use super::plan::Plan;

pub struct TablePlan {
    tx: Arc<Mutex<Transaction>>,
    tblname: String,
    layout: Arc<Layout>,
    si: StatInfo,
}

impl Plan for TablePlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let scan = TableScan::new(
            Arc::clone(&self.tx),
            &self.tblname,
            Arc::clone(&self.layout),
        )?;

        Ok(Arc::new(Mutex::new(scan)))
    }
    fn blocks_accessed(&self) -> i32 {
        self.si.blocks_accessed()
    }
    fn records_output(&self) -> i32 {
        self.si.records_output()
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        self.si.distinct_values(fldname)
    }
    fn schema(&self) -> Arc<Schema> {
        self.layout.schema()
    }
}

impl TablePlan {
    pub fn new(tx: Arc<Mutex<Transaction>>, tblname: &str, mut md: MetadataMgr) -> Result<Self> {
        let layout = md.get_layout(tblname, Arc::clone(&tx))?;
        let si = md.get_stat_info(tblname, Arc::clone(&layout), Arc::clone(&tx))?;
        Ok(Self {
            tx,
            tblname: tblname.to_string(),
            layout,
            si,
        })
    }
}
