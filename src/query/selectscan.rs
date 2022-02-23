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
        let mut us = self.s.lock().unwrap();
        us.to_update_scan()?.set_i32(fldname, val)
    }
    fn set_string(&mut self, fldname: &str, val: String) -> Result<()> {
        let mut us = self.s.lock().unwrap();
        us.to_update_scan()?.set_string(fldname, val)
    }
    fn set_val(&mut self, fldname: &str, val: Constant) -> Result<()> {
        let mut us = self.s.lock().unwrap();
        us.to_update_scan()?.set_val(fldname, val)
    }
    fn insert(&mut self) -> Result<()> {
        let mut us = self.s.lock().unwrap();
        us.to_update_scan()?.insert()
    }
    fn delete(&mut self) -> Result<()> {
        let mut us = self.s.lock().unwrap();
        us.to_update_scan()?.delete()
    }
    fn get_rid(&self) -> Result<RID> {
        let mut us = self.s.lock().unwrap();
        us.to_update_scan()?.get_rid()
    }
    fn move_to_rid(&mut self, rid: RID) -> Result<()> {
        let mut us = self.s.lock().unwrap();
        us.to_update_scan()?.move_to_rid(rid)
    }
}

impl SelectScan {
    pub fn new(s: Arc<Mutex<dyn Scan>>, pred: Predicate) -> Self {
        Self { s, pred }
    }
}
