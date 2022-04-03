use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::sortscan::SortScan;
use crate::query::{constant::Constant, scan::Scan};

#[derive(Debug)]
pub enum MergeJoinScanError {
    DowncastError,
}

impl std::error::Error for MergeJoinScanError {}
impl fmt::Display for MergeJoinScanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MergeJoinScanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

pub struct MergeJoinScan {
    s1: Arc<Mutex<dyn Scan>>,
    s2: Arc<Mutex<SortScan>>,
    fldname1: String,
    fldname2: String,
    joinval: Option<Constant>,
}

impl MergeJoinScan {
    pub fn new(
        s1: Arc<Mutex<dyn Scan>>,
        s2: Arc<Mutex<SortScan>>,
        fldname1: &str,
        fldname2: &str,
    ) -> Self {
        let mut scan = Self {
            s1,
            s2,
            fldname1: fldname1.to_string(),
            fldname2: fldname2.to_string(),
            joinval: None,
        };
        scan.before_first().unwrap();

        scan
    }
}

impl Scan for MergeJoinScan {
    fn before_first(&mut self) -> Result<()> {
        self.s1.lock().unwrap().before_first()?;
        self.s2.lock().unwrap().before_first()?;

        Ok(())
    }
    fn next(&mut self) -> bool {
        let mut hasmore2 = self.s2.lock().unwrap().next();
        if hasmore2 && self.s2.lock().unwrap().get_val(&self.fldname2).ok() == self.joinval {
            return true;
        }

        let mut hasmore1 = self.s1.lock().unwrap().next();
        if hasmore1 && self.s1.lock().unwrap().get_val(&self.fldname1).ok() == self.joinval {
            self.s2.lock().unwrap().restore_position();
            return true;
        }

        while hasmore1 && hasmore2 {
            let v1 = self.s1.lock().unwrap().get_val(&self.fldname1).unwrap();
            let v2 = self.s2.lock().unwrap().get_val(&self.fldname2).unwrap();
            if v1 < v2 {
                hasmore1 = self.s1.lock().unwrap().next();
            } else if v1 > v2 {
                hasmore2 = self.s2.lock().unwrap().next();
            } else {
                self.s2.lock().unwrap().save_position();
                self.joinval = self.s2.lock().unwrap().get_val(&self.fldname2).ok();
                return true;
            }
        }

        false
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        if self.s2.lock().unwrap().has_field(fldname) {
            return self.s1.lock().unwrap().get_i32(fldname);
        } else {
            return self.s2.lock().unwrap().get_i32(fldname);
        }
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        if self.s2.lock().unwrap().has_field(fldname) {
            return self.s1.lock().unwrap().get_string(fldname);
        } else {
            return self.s2.lock().unwrap().get_string(fldname);
        }
    }
    fn get_val(&mut self, fldname: &str) -> Result<Constant> {
        if self.s2.lock().unwrap().has_field(fldname) {
            return self.s1.lock().unwrap().get_val(fldname);
        } else {
            return self.s2.lock().unwrap().get_val(fldname);
        }
    }
    fn has_field(&self, fldname: &str) -> bool {
        self.s1.lock().unwrap().has_field(fldname) || self.s2.lock().unwrap().has_field(fldname)
    }
    fn close(&mut self) -> Result<()> {
        self.s1.lock().unwrap().close()?;
        self.s2.lock().unwrap().close()?;

        Ok(())
    }
    fn to_update_scan(&mut self) -> Result<&mut dyn crate::query::updatescan::UpdateScan> {
        panic!("TODO")
    }
    fn as_table_scan(&mut self) -> Result<&mut crate::record::tablescan::TableScan> {
        panic!("TODO")
    }
    fn as_sort_scan(&mut self) -> Result<&mut SortScan> {
        panic!("TODO")
    }
}
