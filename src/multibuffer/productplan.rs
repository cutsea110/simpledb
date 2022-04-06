use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::{
    materialize::temptable::TempTable, plan::plan::Plan, query::scan::Scan, record::schema::Schema,
    tx::transaction::Transaction,
};

pub struct MultibufferProductPlan {
    tx: Arc<Mutex<Transaction>>,
    lhs: Arc<dyn Plan>,
    rhs: Arc<dyn Plan>,
    schema: Arc<Schema>,
}

impl MultibufferProductPlan {
    pub fn new(tx: Arc<Mutex<Transaction>>, lhs: Arc<dyn Plan>, rhs: Arc<dyn Plan>) -> Self {
        panic!("TODO")
    }
    fn copy_records_from(&self, p: Arc<dyn Plan>) -> TempTable {
        panic!("TODO")
    }
}

impl Plan for MultibufferProductPlan {
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
