use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::tablemanager::{TableMgr, MAX_NAME};
use crate::{
    record::{schema::Schema, tablescan::TableScan},
    tx::transaction::Transaction,
};

pub const MAX_VIEWDEF: usize = 100; // max view def chars

pub struct ViewMgr {
    tbl_mgr: TableMgr,
}

impl ViewMgr {
    pub fn new(is_new: bool, tbl_mgr: TableMgr, tx: Arc<Mutex<Transaction>>) -> Result<Self> {
        let mgr = Self { tbl_mgr };

        if is_new {
            let mut sch = Schema::new();
            sch.add_string_field("viewname", MAX_NAME);
            sch.add_string_field("viewdef", MAX_VIEWDEF);
            mgr.tbl_mgr.create_table("viewcat", sch, tx)?;
        }

        Ok(mgr)
    }
    pub fn create_view(&self, vname: &str, vdef: &str, tx: Arc<Mutex<Transaction>>) -> Result<()> {
        let layout = self.tbl_mgr.get_layout("viewcat", Arc::clone(&tx))?;
        let mut ts = TableScan::new(tx, "viewcat", layout);
        ts.set_string("viewname", vname.to_string())?;
        ts.set_string("viewdef", vdef.to_string())?;
        ts.close()?;

        Ok(())
    }
    pub fn get_view_def(&self, vname: &str, tx: Arc<Mutex<Transaction>>) -> Result<String> {
        let mut result = "".to_string();

        let layout = self.tbl_mgr.get_layout("viewcat", Arc::clone(&tx))?;
        let mut ts = TableScan::new(tx, "viewcat", layout);
        while ts.next() {
            if ts.get_string("viewname")? == vname {
                result = ts.get_string("viewdef").unwrap_or("".to_string());
                break;
            }
        }
        ts.close()?;

        Ok(result)
    }
}
