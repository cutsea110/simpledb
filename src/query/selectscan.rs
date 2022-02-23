use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::{predicate::Predicate, scan::Scan, updatescan::UpdateScan};
use crate::{query::constant::Constant, record::rid::RID};

#[derive(Debug)]
pub enum SelectScanError {
    DowncastError,
}

impl std::error::Error for SelectScanError {}
impl fmt::Display for SelectScanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &SelectScanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

pub struct SelectScan {
    s: Arc<Mutex<dyn Scan>>,
    pred: Predicate,
}

impl Scan for SelectScan {
    fn before_first(&mut self) -> Result<()> {
        self.s.lock().unwrap().before_first()
    }
    fn next(&mut self) -> bool {
        panic!("TODO")
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        self.s.lock().unwrap().get_i32(fldname)
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        self.s.lock().unwrap().get_string(fldname)
    }
    fn get_val(&mut self, fldname: &str) -> Constant {
        self.s.lock().unwrap().get_val(fldname)
    }
    fn has_field(&self, fldname: &str) -> bool {
        self.s.lock().unwrap().has_field(fldname)
    }
    fn close(&mut self) -> Result<()> {
        self.s.lock().unwrap().close()
    }
    fn to_update_scan(&mut self) -> Result<&mut dyn UpdateScan> {
        Err(From::from(SelectScanError::DowncastError))
    }
}

impl UpdateScan for SelectScan {
    fn set_i32(&mut self, fldname: &str, val: i32) -> Result<()> {
        if let Ok(us) = self.s.lock().unwrap().to_update_scan() {
            us.set_i32(fldname, val)?;
        }

        Err(From::from(SelectScanError::DowncastError))
    }
    fn set_string(&mut self, fldname: &str, val: String) -> Result<()> {
        panic!("TODO")
    }
    fn set_val(&mut self, fldname: &str, val: Constant) -> Result<()> {
        panic!("TODO")
    }
    fn insert(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn delete(&mut self) -> Result<()> {
        panic!("TODO")
    }
    fn get_rid(&self) -> RID {
        panic!("TODO")
    }
    fn move_to_rid(&mut self, rid: RID) -> Result<()> {
        panic!("TODO")
    }
}

impl SelectScan {
    pub fn new(s: Arc<Mutex<dyn Scan>>, pred: Predicate) -> Self {
        Self { s, pred }
    }
}
