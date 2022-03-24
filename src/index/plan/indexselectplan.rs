use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::{
    index::query::indexselectscan::IndexSelectScan,
    metadata::indexmanager::IndexInfo,
    plan::plan::Plan,
    query::{constant::Constant, scan::Scan},
    record::schema::Schema,
};

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
        panic!("TODO")
        /*
                // throws an exception if p is not a table plan.
                let ts = self.p.open()?;
                let idx = self.ii.open();
                Ok(Arc::new(Mutex::new(IndexSelectScan::new(
                    ts,
                    idx,
                    self.val.clone(),
                ))))
        */
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
