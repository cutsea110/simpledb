use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use crate::{
    index::Index,
    materialize::sortscan::SortScan,
    query::{constant::Constant, scan::Scan, updatescan::UpdateScan},
    record::tablescan::TableScan,
};

#[derive(Debug)]
pub enum IndexSelectScanError {
    DowncastError,
}

impl std::error::Error for IndexSelectScanError {}
impl fmt::Display for IndexSelectScanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IndexSelectScanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

pub struct IndexSelectScan {
    ts: Arc<Mutex<TableScan>>,
    idx: Arc<Mutex<dyn Index>>,
    val: Constant,
}

impl IndexSelectScan {
    pub fn new(
        ts: Arc<Mutex<TableScan>>,
        idx: Arc<Mutex<dyn Index>>,
        val: Constant,
    ) -> Result<Self> {
        let mut scan = Self { ts, idx, val };
        scan.before_first()?;

        Ok(scan)
    }
}

impl Scan for IndexSelectScan {
    fn before_first(&mut self) -> Result<()> {
        self.idx.lock().unwrap().before_first(self.val.clone())
    }
    fn next(&mut self) -> bool {
        let ok = self.idx.lock().unwrap().next();
        if ok {
            let rid = self.idx.lock().unwrap().get_data_rid().unwrap();
            self.ts.lock().unwrap().move_to_rid(rid).unwrap();
        }

        ok
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        self.ts.lock().unwrap().get_i32(fldname)
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        self.ts.lock().unwrap().get_string(fldname)
    }
    fn get_val(&mut self, fldname: &str) -> Result<Constant> {
        self.ts.lock().unwrap().get_val(fldname)
    }
    fn has_field(&self, fldname: &str) -> bool {
        self.ts.lock().unwrap().has_field(fldname)
    }
    fn close(&mut self) -> Result<()> {
        self.idx.lock().unwrap().close()?;
        self.ts.lock().unwrap().close()
    }

    fn to_update_scan(&mut self) -> Result<&mut dyn UpdateScan> {
        Err(From::from(IndexSelectScanError::DowncastError))
    }
    fn as_table_scan(&mut self) -> Result<&mut TableScan> {
        Err(From::from(IndexSelectScanError::DowncastError))
    }
    fn as_sort_scan(&mut self) -> Result<&mut SortScan> {
        Err(From::from(IndexSelectScanError::DowncastError))
    }
}
