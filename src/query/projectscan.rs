use anyhow::Result;
use chrono::NaiveDate;
use core::fmt;
use std::sync::{Arc, Mutex};

use crate::{materialize::sortscan::SortScan, record::tablescan::TableScan};

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

#[derive(Clone)]
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
    fn get_i16(&mut self, fldname: &str) -> Result<i16> {
        if self.has_field(fldname) {
            self.s.lock().unwrap().get_i16(fldname)
        } else {
            Err(From::from(ProjectScanError::FieldNotFoundError(
                fldname.to_string(),
            )))
        }
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
    fn get_bool(&mut self, fldname: &str) -> Result<bool> {
        if self.has_field(fldname) {
            self.s.lock().unwrap().get_bool(fldname)
        } else {
            Err(From::from(ProjectScanError::FieldNotFoundError(
                fldname.to_string(),
            )))
        }
    }
    fn get_date(&mut self, fldname: &str) -> Result<NaiveDate> {
        if self.has_field(fldname) {
            self.s.lock().unwrap().get_date(fldname)
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
    fn as_table_scan(&mut self) -> Result<&mut TableScan> {
        Err(From::from(ProjectScanError::DowncastError))
    }
    fn as_sort_scan(&mut self) -> Result<&mut SortScan> {
        Err(From::from(ProjectScanError::DowncastError))
    }
}

impl ProjectScan {
    pub fn new(s: Arc<Mutex<dyn Scan>>, fieldlist: Vec<String>) -> Self {
        Self { s, fieldlist }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use crate::{metadata::manager::MetadataMgr, server::simpledb::SimpleDB};

    use super::super::tests;
    use super::*;

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/projectscan").exists() {
            fs::remove_dir_all("_test/projectscan")?;
        }

        let simpledb = SimpleDB::new_with("_test/projectscan", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        let mut mdm = MetadataMgr::new(true, Arc::clone(&tx))?;

        tests::init_sampledb(&mut mdm, Arc::clone(&tx))?;

        // the STUDENT node
        let layout = mdm.get_layout("STUDENT", Arc::clone(&tx))?;
        let ts = TableScan::new(Arc::clone(&tx), "STUDENT", layout)?;

        // the Project node
        let cols = vec!["SName".to_string(), "MajorId".to_string()];
        let mut s2 = ProjectScan::new(Arc::new(Mutex::new(ts)), cols);
        println!("SELECT SName, MajorId FROM STUDENT");
        while s2.next() {
            println!("{} {}", s2.get_string("SName")?, s2.get_i32("MajorId")?);
        }

        tx.lock().unwrap().commit()?;

        Ok(())
    }
}
