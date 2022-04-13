use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::{recordcomparator::RecordComparator, temptable::TempTable};
use crate::{
    query::{constant::Constant, scan::Scan, updatescan::UpdateScan},
    record::{rid::RID, tablescan::TableScan},
};

#[derive(Debug)]
pub enum SortScanError {
    NoCurrentScan,
    DowncastError,
}

impl std::error::Error for SortScanError {}
impl fmt::Display for SortScanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SortScanError::NoCurrentScan => {
                write!(f, "no current scan")
            }
            SortScanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
enum ScanEither {
    Scan1,
    Scan2,
    NoScan,
}

#[derive(Clone)]
pub struct SortScan {
    s1: Arc<Mutex<dyn UpdateScan>>,
    s2: Option<Arc<Mutex<dyn UpdateScan>>>,
    currentscan: ScanEither,
    comp: RecordComparator,
    hasmore1: bool,
    hasmore2: bool,
    savedposition: Vec<RID>,
}

impl SortScan {
    pub fn new(mut runs: Vec<TempTable>, comp: RecordComparator) -> Self {
        let s1 = runs[0].open().unwrap();
        let hasmore1 = s1.lock().unwrap().next();
        let mut s2 = None;
        let mut hasmore2 = false;
        if runs.len() > 1 {
            s2 = runs[1].open().ok();
            hasmore2 = s2.as_ref().unwrap().lock().unwrap().next();
        }

        Self {
            s1,
            s2,
            currentscan: ScanEither::NoScan,
            comp,
            hasmore1,
            hasmore2,
            savedposition: vec![],
        }
    }
    pub fn save_position(&mut self) {
        let rid1 = self.s1.lock().unwrap().get_rid().unwrap();
        match self.s2.as_ref() {
            Some(s2) => {
                let rid2 = s2.lock().unwrap().get_rid().unwrap();
                self.savedposition = vec![rid1, rid2];
            }
            None => self.savedposition = vec![rid1],
        }
    }
    pub fn restore_position(&mut self) {
        let rid1 = self.savedposition.get(0).unwrap();
        self.s1.lock().unwrap().move_to_rid(rid1.clone()).unwrap();
        if let Some(rid2) = self.savedposition.get(1) {
            let mut s2 = self.s2.as_ref().unwrap().lock().unwrap();
            s2.move_to_rid(rid2.clone()).unwrap();
        }
    }
}

impl Scan for SortScan {
    fn before_first(&mut self) -> Result<()> {
        self.s1.lock().unwrap().before_first()?;
        self.hasmore1 = self.s1.lock().unwrap().next();
        if let Some(s2) = self.s2.as_ref() {
            s2.lock().unwrap().before_first()?;
            self.hasmore2 = s2.lock().unwrap().next();
        }

        Ok(())
    }
    fn next(&mut self) -> bool {
        match self.currentscan {
            ScanEither::Scan1 => {
                self.hasmore1 = self.s1.lock().unwrap().next();
            }
            ScanEither::Scan2 => {
                self.hasmore2 = self.s2.as_ref().unwrap().lock().unwrap().next();
            }
            _ => {}
        }

        if !self.hasmore1 && !self.hasmore2 {
            return false;
        } else if self.hasmore1 && self.hasmore2 {
            let s1 = self.s1.lock().unwrap().to_scan().unwrap();
            let s2 = self.s2.as_ref().unwrap().lock().unwrap().to_scan().unwrap();
            if self.comp.compare(s1, s2).is_lt() {
                self.currentscan = ScanEither::Scan1;
            } else {
                self.currentscan = ScanEither::Scan2;
            }
        } else if self.hasmore1 {
            self.currentscan = ScanEither::Scan1;
        } else if self.hasmore2 {
            self.currentscan = ScanEither::Scan2;
        }

        true
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        match self.currentscan {
            ScanEither::Scan1 => self.s1.lock().unwrap().get_i32(fldname),
            ScanEither::Scan2 => self.s2.as_ref().unwrap().lock().unwrap().get_i32(fldname),
            ScanEither::NoScan => Err(From::from(SortScanError::NoCurrentScan)),
        }
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        match self.currentscan {
            ScanEither::Scan1 => self.s1.lock().unwrap().get_string(fldname),
            ScanEither::Scan2 => {
                let mut s2 = self.s2.as_ref().unwrap().lock().unwrap();
                s2.get_string(fldname)
            }
            ScanEither::NoScan => Err(From::from(SortScanError::NoCurrentScan)),
        }
    }
    fn get_val(&mut self, fldname: &str) -> Result<Constant> {
        match self.currentscan {
            ScanEither::Scan1 => self.s1.lock().unwrap().get_val(fldname),
            ScanEither::Scan2 => self.s2.as_ref().unwrap().lock().unwrap().get_val(fldname),
            ScanEither::NoScan => Err(From::from(SortScanError::NoCurrentScan)),
        }
    }
    fn has_field(&self, fldname: &str) -> bool {
        match self.currentscan {
            ScanEither::Scan1 => self.s1.lock().unwrap().has_field(fldname),
            ScanEither::Scan2 => self.s2.as_ref().unwrap().lock().unwrap().has_field(fldname),
            ScanEither::NoScan => false,
        }
    }
    fn close(&mut self) -> Result<()> {
        self.s1.lock().unwrap().close()?;
        if let Some(s2) = self.s2.as_ref() {
            s2.lock().unwrap().close()?;
        }

        Ok(())
    }
    fn to_update_scan(&mut self) -> Result<&mut dyn UpdateScan> {
        Err(From::from(SortScanError::DowncastError))
    }
    fn as_table_scan(&mut self) -> Result<&mut TableScan> {
        Err(From::from(SortScanError::DowncastError))
    }
    fn as_sort_scan(&mut self) -> Result<&mut SortScan> {
        Ok(self)
    }
}
