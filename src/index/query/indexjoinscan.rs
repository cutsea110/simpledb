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
pub enum IndexJoinScanError {
    DowncastError,
}

impl std::error::Error for IndexJoinScanError {}
impl fmt::Display for IndexJoinScanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IndexJoinScanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

pub struct IndexJoinScan {
    lhs: Arc<Mutex<dyn Scan>>,
    idx: Arc<Mutex<dyn Index>>,
    joinfield: String,
    rhs: Arc<Mutex<TableScan>>,
}

impl IndexJoinScan {
    pub fn new(
        lhs: Arc<Mutex<dyn Scan>>,
        idx: Arc<Mutex<dyn Index>>,
        joinfld: &str,
        rhs: Arc<Mutex<TableScan>>,
    ) -> Result<Self> {
        let mut scan = Self {
            lhs,
            idx,
            joinfield: joinfld.to_string(),
            rhs,
        };
        scan.before_first()?;

        Ok(scan)
    }
    fn reset_index(&self) -> Result<()> {
        let searchkey = self.lhs.lock().unwrap().get_val(&self.joinfield)?;
        self.idx.lock().unwrap().before_first(searchkey)
    }
}

impl Scan for IndexJoinScan {
    fn before_first(&mut self) -> Result<()> {
        self.lhs.lock().unwrap().before_first()?;
        self.lhs.lock().unwrap().next();
        self.reset_index()
    }
    fn next(&mut self) -> bool {
        loop {
            if self.idx.lock().unwrap().next() {
                let rid = self.idx.lock().unwrap().get_data_rid().unwrap();
                self.rhs.lock().unwrap().move_to_rid(rid).unwrap();
                return true;
            }
            if !self.lhs.lock().unwrap().next() {
                return false;
            }
            self.reset_index().unwrap();
        }
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        if self.rhs.lock().unwrap().has_field(fldname) {
            self.rhs.lock().unwrap().get_i32(fldname)
        } else {
            self.lhs.lock().unwrap().get_i32(fldname)
        }
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        if self.rhs.lock().unwrap().has_field(fldname) {
            self.rhs.lock().unwrap().get_string(fldname)
        } else {
            self.lhs.lock().unwrap().get_string(fldname)
        }
    }
    fn get_val(&mut self, fldname: &str) -> Result<Constant> {
        if self.rhs.lock().unwrap().has_field(fldname) {
            self.rhs.lock().unwrap().get_val(fldname)
        } else {
            self.lhs.lock().unwrap().get_val(fldname)
        }
    }
    fn has_field(&self, fldname: &str) -> bool {
        self.rhs.lock().unwrap().has_field(fldname) || self.lhs.lock().unwrap().has_field(fldname)
    }
    fn close(&mut self) -> Result<()> {
        self.lhs.lock().unwrap().close()?;
        self.idx.lock().unwrap().close()?;
        self.rhs.lock().unwrap().close()?;

        Ok(())
    }
    fn to_update_scan(&mut self) -> Result<&mut dyn UpdateScan> {
        Err(From::from(IndexJoinScanError::DowncastError))
    }
    fn as_table_scan(&mut self) -> Result<&mut TableScan> {
        Err(From::from(IndexJoinScanError::DowncastError))
    }
    fn as_sort_scan(&mut self) -> Result<&mut SortScan> {
        Err(From::from(IndexJoinScanError::DowncastError))
    }
}
