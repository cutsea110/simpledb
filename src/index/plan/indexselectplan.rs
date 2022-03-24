use core::fmt;
use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::{
    index::query::indexselectscan::IndexSelectScan,
    metadata::indexmanager::IndexInfo,
    plan::plan::Plan,
    query::{constant::Constant, scan::Scan},
    record::schema::Schema,
};

#[derive(Debug)]
pub enum IndexSelectPlanError {
    DowncastError,
}
impl std::error::Error for IndexSelectPlanError {}
impl fmt::Display for IndexSelectPlanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IndexSelectPlanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

pub struct IndexSelectPlan {
    p: Arc<dyn Plan>,
    ii: IndexInfo,
    val: Constant,
}

impl IndexSelectPlan {
    pub fn new(p: Arc<dyn Plan>, ii: IndexInfo, val: Constant) -> Self {
        Self { p, ii, val }
    }
}

impl Plan for IndexSelectPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        // throws an exception if p is not a table plan.
        if let Ok(ts) = self.p.open()?.lock().unwrap().as_table_scan() {
            let scan = IndexSelectScan::new(
                Arc::new(Mutex::new(ts.clone())),
                self.ii.open(),
                self.val.clone(),
            )?;
            return Ok(Arc::new(Mutex::new(scan)));
        }

        Err(From::from(IndexSelectPlanError::DowncastError))
    }
    fn blocks_accessed(&self) -> i32 {
        self.ii.blocks_accessed() + self.records_output()
    }
    fn records_output(&self) -> i32 {
        self.ii.records_output()
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        self.ii.distinct_values(fldname)
    }
    fn schema(&self) -> Arc<Schema> {
        self.p.schema()
    }
}
