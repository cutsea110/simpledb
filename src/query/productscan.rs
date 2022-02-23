use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::{constant::Constant, scan::Scan};

#[derive(Debug)]
pub enum ProductScanError {
    DowncastError,
    FieldNotFoundError(String),
}

impl std::error::Error for ProductScanError {}
impl fmt::Display for ProductScanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ProductScanError::DowncastError => {
                write!(f, "downcast error")
            }
            ProductScanError::FieldNotFoundError(fld) => {
                write!(f, "field({}) not found error", fld)
            }
        }
    }
}

pub struct ProductScan {
    s1: Arc<Mutex<dyn Scan>>,
    s2: Arc<Mutex<dyn Scan>>,
}

impl Scan for ProductScan {
    fn before_first(&mut self) -> Result<()> {
        let mut s1 = self.s1.lock().unwrap();
        let mut s2 = self.s2.lock().unwrap();
        s1.before_first()?;
        s1.next();
        s2.before_first()
    }
    fn next(&mut self) -> bool {
        let mut s2 = self.s2.lock().unwrap();
        if s2.next() {
            true
        } else {
            s2.before_first().unwrap();
            s2.next() && self.s1.lock().unwrap().next()
        }
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        let mut s1 = self.s1.lock().unwrap();
        if s1.has_field(fldname) {
            s1.get_i32(fldname)
        } else {
            self.s2.lock().unwrap().get_i32(fldname)
        }
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        let mut s1 = self.s1.lock().unwrap();
        if s1.has_field(fldname) {
            s1.get_string(fldname)
        } else {
            self.s2.lock().unwrap().get_string(fldname)
        }
    }
    fn get_val(&mut self, fldname: &str) -> Result<Constant> {
        let mut s1 = self.s1.lock().unwrap();
        if s1.has_field(fldname) {
            s1.get_val(fldname)
        } else {
            self.s2.lock().unwrap().get_val(fldname)
        }
    }
    fn has_field(&self, fldname: &str) -> bool {
        self.s1.lock().unwrap().has_field(fldname) || self.s2.lock().unwrap().has_field(fldname)
    }
    fn close(&mut self) -> anyhow::Result<()> {
        let mut s1 = self.s1.lock().unwrap();
        let mut s2 = self.s2.lock().unwrap();
        s1.close()?;
        s2.close()
    }
    fn to_update_scan(&mut self) -> anyhow::Result<&mut dyn super::updatescan::UpdateScan> {
        Err(From::from(ProductScanError::DowncastError))
    }
}

impl ProductScan {
    pub fn new(s1: Arc<Mutex<dyn Scan>>, s2: Arc<Mutex<dyn Scan>>) -> Self {
        s1.lock().unwrap().next();
        Self { s1, s2 }
    }
}
