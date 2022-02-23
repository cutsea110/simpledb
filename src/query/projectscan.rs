use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::{constant::Constant, scan::Scan, updatescan::UpdateScan};

#[derive(Debug)]
pub enum ProjectScanError {
    DowncastError,
    FieldNotFoundError(String),
}

impl std::error::Error for ProjectScanError {}
impl fmt::Display for ProjectScanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ProjectScanError::DowncastError => {
                write!(f, "downcast error")
            }
            ProjectScanError::FieldNotFoundError(fld) => {
                write!(f, "field({}) not found error", fld)
            }
        }
    }
}

pub struct ProjectScan {
    s: Arc<Mutex<dyn Scan>>,
    fieldlist: Vec<String>,
}

impl Scan for ProjectScan {
    fn before_first(&mut self) -> anyhow::Result<()> {
        self.s.lock().unwrap().before_first()
    }
    fn next(&mut self) -> bool {
        self.s.lock().unwrap().next()
    }
    fn get_i32(&mut self, fldname: &str) -> anyhow::Result<i32> {
        if self.has_field(fldname) {
            self.s.lock().unwrap().get_i32(fldname)
        } else {
            Err(From::from(ProjectScanError::FieldNotFoundError(
                fldname.to_string(),
            )))
        }
    }
    fn get_string(&mut self, fldname: &str) -> anyhow::Result<String> {
        if self.has_field(fldname) {
            self.s.lock().unwrap().get_string(fldname)
        } else {
            Err(From::from(ProjectScanError::FieldNotFoundError(
                fldname.to_string(),
            )))
        }
    }
    fn get_val(&mut self, fldname: &str) -> Result<Constant> {
        if self.has_field(fldname) {
            self.s.lock().unwrap().get_val(fldname)
        } else {
            Err(From::from(ProjectScanError::FieldNotFoundError(
                fldname.to_string(),
            )))
        }
    }
    fn has_field(&self, fldname: &str) -> bool {
        self.fieldlist.contains(&fldname.to_string())
    }
    fn close(&mut self) -> anyhow::Result<()> {
        self.s.lock().unwrap().close()
    }
    fn to_update_scan(&mut self) -> Result<&mut dyn UpdateScan> {
        Err(From::from(ProjectScanError::DowncastError))
    }
}

impl ProjectScan {
    pub fn new(s: Arc<Mutex<dyn Scan>>, fieldlist: Vec<String>) -> Self {
        Self { s, fieldlist }
    }
}
