use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::{aggregationfn::AggregationFn, groupvalue::GroupValue};
use crate::{
    query::{constant::Constant, scan::Scan, updatescan::UpdateScan},
    record::tablescan::TableScan,
};

#[derive(Debug)]
pub enum GroupByScanError {
    NoFieldError(String),
    DowncastError,
}

impl std::error::Error for GroupByScanError {}
impl fmt::Display for GroupByScanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GroupByScanError::NoFieldError(fldname) => {
                write!(f, "no field: {}", fldname)
            }
            GroupByScanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

pub struct GroupByScan {
    s: Arc<Mutex<dyn Scan>>,
    groupfields: Vec<String>,
    aggfns: Vec<Arc<Mutex<dyn AggregationFn>>>,
    groupval: Option<GroupValue>,
    moregroups: bool,
}

impl GroupByScan {
    pub fn new(
        s: Arc<Mutex<dyn Scan>>,
        groupfields: Vec<String>,
        aggfns: Vec<Arc<Mutex<dyn AggregationFn>>>,
    ) -> Self {
        let mut scan = Self {
            s,
            groupfields,
            aggfns,
            groupval: None,
            moregroups: false,
        };
        scan.before_first().unwrap();

        scan
    }
}

impl Scan for GroupByScan {
    fn before_first(&mut self) -> Result<()> {
        self.s.lock().unwrap().before_first()?;
        self.moregroups = self.s.lock().unwrap().next();

        Ok(())
    }
    fn next(&mut self) -> bool {
        if !self.moregroups {
            return false;
        }
        for aggfn in self.aggfns.iter() {
            aggfn.lock().unwrap().process_first(Arc::clone(&self.s));
        }
        self.groupval = Some(GroupValue::new(
            Arc::clone(&self.s),
            self.groupfields.clone(),
        ));
        loop {
            self.moregroups = self.s.lock().unwrap().next();
            if !self.moregroups {
                break;
            }

            let gv = Some(GroupValue::new(
                Arc::clone(&self.s),
                self.groupfields.clone(),
            ));
            if self.groupval != gv {
                break;
            }
            for aggfn in self.aggfns.iter() {
                aggfn.lock().unwrap().process_next(Arc::clone(&self.s));
            }
        }

        true
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        self.get_val(fldname)?.as_i32()
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        self.get_val(fldname)?
            .as_string()
            .map(|sval| sval.to_string())
    }
    fn get_val(&mut self, fldname: &str) -> Result<Constant> {
        if self.groupfields.contains(&fldname.to_string()) {
            if let Some(val) = self.groupval.as_ref().unwrap().get_val(fldname) {
                return Ok(val.clone());
            }
        }
        for aggfn in self.aggfns.iter() {
            if aggfn.lock().unwrap().field_name() == fldname {
                let val = aggfn.lock().unwrap().value();
                return Ok(val);
            }
        }

        Err(From::from(GroupByScanError::NoFieldError(
            fldname.to_string(),
        )))
    }
    fn has_field(&self, fldname: &str) -> bool {
        if self.groupfields.contains(&fldname.to_string()) {
            return true;
        }
        for aggfn in self.aggfns.iter() {
            if aggfn.lock().unwrap().field_name() == fldname {
                return true;
            }
        }

        false
    }
    fn close(&mut self) -> Result<()> {
        self.s.lock().unwrap().close()
    }
    fn to_update_scan(&mut self) -> Result<&mut dyn UpdateScan> {
        Err(From::from(GroupByScanError::DowncastError))
    }
    fn as_table_scan(&mut self) -> Result<&mut TableScan> {
        Err(From::from(GroupByScanError::DowncastError))
    }
}
