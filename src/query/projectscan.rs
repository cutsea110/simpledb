use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::scan::Scan;

#[derive(Debug)]
pub enum ProjectScanError {
    DowncastError,
}

impl std::error::Error for ProjectScanError {}
impl fmt::Display for ProjectScanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ProjectScanError::DowncastError => {
                write!(f, "downcast error")
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
    fn get_val(&mut self, fldname: &str) -> super::constant::Constant {
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

impl ProjectScan {
    pub fn new(s: Arc<Mutex<dyn Scan>>, fieldlist: Vec<String>) -> Self {
        Self { s, fieldlist }
    }
}
