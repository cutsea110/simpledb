use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::scan::Scan;

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
    fn before_first(&mut self) -> anyhow::Result<()> {
        panic!("TODO")
    }
    fn next(&mut self) -> bool {
        panic!("TODO")
    }
    fn get_i32(&mut self, fldname: &str) -> anyhow::Result<i32> {
        panic!("TODO")
    }
    fn get_string(&mut self, fldname: &str) -> anyhow::Result<String> {
        panic!("TODO")
    }
    fn get_val(&mut self, fldname: &str) -> anyhow::Result<super::constant::Constant> {
        panic!("TODO")
    }
    fn has_field(&self, fldname: &str) -> bool {
        panic!("TODO")
    }
    fn close(&mut self) -> anyhow::Result<()> {
        panic!("TODO")
    }
    fn to_update_scan(&mut self) -> anyhow::Result<&mut dyn super::updatescan::UpdateScan> {
        panic!("TODO")
    }
}

impl ProductScan {
    pub fn new(s1: Arc<Mutex<dyn Scan>>, s2: Arc<Mutex<dyn Scan>>) -> Self {
        Self { s1, s2 }
    }
}
