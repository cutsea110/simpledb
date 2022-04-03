use anyhow::Result;
use std::sync::{Arc, Mutex};

use crate::{
    materialize::sortplan::SortPlan, plan::plan::Plan, query::scan::Scan, record::schema::Schema,
    tx::transaction::Transaction,
};

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
        panic!("TODO")
    }
    fn blocks_accessed(&self) -> i32 {
        panic!("TODO")
    }
    fn records_output(&self) -> i32 {
        panic!("TODO")
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        panic!("TODO")
    }
    fn schema(&self) -> Arc<Schema> {
        panic!("TODO")
    }
}
