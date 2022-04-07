use anyhow::Result;
use core::fmt;
use std::{
    cmp::max,
    sync::{Arc, Mutex},
};

use super::mergejoinscan::MergeJoinScan;
use crate::{
    materialize::sortplan::SortPlan, plan::plan::Plan, query::scan::Scan, record::schema::Schema,
    tx::transaction::Transaction,
};

#[derive(Debug)]
pub enum MergeJoinPlanError {
    DowncastError,
}

impl std::error::Error for MergeJoinPlanError {}
impl fmt::Display for MergeJoinPlanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MergeJoinPlanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

#[derive(Clone)]
pub struct MergeJoinPlan {
    p1: Arc<dyn Plan>,
    p2: Arc<dyn Plan>,
    fldname1: String,
    fldname2: String,
    sch: Arc<Schema>,
}

impl MergeJoinPlan {
    pub fn new(
        next_table_num: Arc<Mutex<i32>>,

        tx: Arc<Mutex<Transaction>>,
        p1: Arc<dyn Plan>,
        p2: Arc<dyn Plan>,
        fldname1: &str,
        fldname2: &str,
    ) -> Self {
        let mut sch = Schema::new();
        sch.add_all(p1.schema());
        sch.add_all(p2.schema());

        let sortlist1 = vec![fldname1.to_string()];
        let plan1 = SortPlan::new(Arc::clone(&next_table_num), p1, sortlist1, Arc::clone(&tx));
        let sortlist2 = vec![fldname2.to_string()];
        let plan2 = SortPlan::new(Arc::clone(&next_table_num), p2, sortlist2, Arc::clone(&tx));

        Self {
            p1: Arc::new(plan1),
            p2: Arc::new(plan2),
            fldname1: fldname1.to_string(),
            fldname2: fldname2.to_string(),
            sch: Arc::new(sch),
        }
    }
}

impl Plan for MergeJoinPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let s1 = self.p1.open()?;
        if let Ok(s2) = self.p2.open()?.lock().unwrap().as_sort_scan() {
            let scan = MergeJoinScan::new(
                s1,
                Arc::new(Mutex::new(s2.clone())),
                &self.fldname1,
                &self.fldname2,
            );

            return Ok(Arc::new(Mutex::new(scan)));
        }

        Err(From::from(MergeJoinPlanError::DowncastError))
    }
    fn blocks_accessed(&self) -> i32 {
        self.p1.blocks_accessed() + self.p2.blocks_accessed()
    }
    fn records_output(&self) -> i32 {
        let maxvals = max(
            self.p1.distinct_values(&self.fldname1),
            self.p2.distinct_values(&self.fldname2),
        );

        (self.p1.records_output() * self.p2.records_output()) / maxvals
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
