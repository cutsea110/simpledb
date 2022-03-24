use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use crate::{
    index::query::indexjoinscan::IndexJoinScan, metadata::indexmanager::IndexInfo,
    plan::plan::Plan, query::scan::Scan, record::schema::Schema,
};

#[derive(Debug)]
pub enum IndexJoinPlanError {
    DowncastError,
}

impl std::error::Error for IndexJoinPlanError {}
impl fmt::Display for IndexJoinPlanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &IndexJoinPlanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

pub struct IndexJoinPlan {
    p1: Arc<dyn Plan>,
    p2: Arc<dyn Plan>,
    ii: IndexInfo,
    joinfield: String,
    sch: Arc<Schema>,
}

impl IndexJoinPlan {
    pub fn new(p1: Arc<dyn Plan>, p2: Arc<dyn Plan>, ii: IndexInfo, joinfield: &str) -> Self {
        let mut sch = Schema::new();
        sch.add_all(p1.schema());
        sch.add_all(p2.schema());

        Self {
            p1,
            p2,
            ii,
            joinfield: joinfield.to_string(),
            sch: Arc::new(sch),
        }
    }
}

impl Plan for IndexJoinPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let s = self.p1.open()?;
        // throws an exception if p2 is not a table plan
        if let Ok(ts) = self.p2.open()?.lock().unwrap().as_table_scan() {
            let scan = IndexJoinScan::new(
                s,
                self.ii.open(),
                &self.joinfield,
                Arc::new(Mutex::new(ts.clone())),
            );
            return Ok(Arc::new(Mutex::new(scan)));
        }

        Err(From::from(IndexJoinPlanError::DowncastError))
    }
    fn blocks_accessed(&self) -> i32 {
        self.p1.blocks_accessed()
            + (self.p1.records_output() * self.ii.blocks_accessed())
            + self.records_output()
    }
    fn records_output(&self) -> i32 {
        self.p1.records_output() * self.ii.records_output()
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        if self.p1.schema().has_field(fldname) {
            self.p1.distinct_values(fldname)
        } else {
            self.p2.distinct_values(fldname)
        }
    }
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.sch)
    }
}
