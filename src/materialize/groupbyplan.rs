use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::{aggregationfn::AggregationFn, groupbyscan::GroupByScan, sortplan::SortPlan};
use crate::{
    plan::plan::Plan, query::scan::Scan, record::schema::Schema, tx::transaction::Transaction,
};

pub struct GroupByPlan {
    p: Arc<dyn Plan>,
    groupfields: Vec<String>,
    aggfns: Vec<Arc<Mutex<dyn AggregationFn>>>,
    sch: Arc<Schema>,
}

impl GroupByPlan {
    pub fn new(
        next_table_num: Arc<Mutex<i32>>,
        tx: Arc<Mutex<Transaction>>,
        p: Arc<dyn Plan>,
        groupfields: Vec<String>,
        aggfns: Vec<Arc<Mutex<dyn AggregationFn>>>,
    ) -> Self {
        let plan = SortPlan::new(
            Arc::clone(&next_table_num),
            p,
            groupfields.clone(),
            Arc::clone(&tx),
        );
        let mut sch = Schema::new();

        for fldname in groupfields.iter() {
            sch.add(fldname, plan.schema());
        }
        for aggfn in aggfns.iter() {
            sch.add_i32_field(&aggfn.lock().unwrap().field_name());
        }
        Self {
            p: Arc::new(plan),
            groupfields,
            aggfns,
            sch: Arc::new(sch),
        }
    }
}

impl Plan for GroupByPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let s = self.p.open()?;
        let scan = GroupByScan::new(s, self.groupfields.clone(), self.aggfns.clone());

        Ok(Arc::new(Mutex::new(scan)))
    }
    fn blocks_accessed(&self) -> i32 {
        self.p.blocks_accessed()
    }
    fn records_output(&self) -> i32 {
        let mut numgroups = 1;
        for fldname in self.groupfields.iter() {
            numgroups *= self.p.distinct_values(fldname);
        }

        numgroups
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        if self.p.schema().has_field(fldname) {
            self.p.distinct_values(fldname)
        } else {
            self.records_output()
        }
    }
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.sch)
    }
}
