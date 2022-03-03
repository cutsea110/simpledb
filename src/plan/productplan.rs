use anyhow::Result;
use std::sync::Arc;

use super::plan::Plan;
use crate::record::schema::Schema;

pub struct ProductPlan {
    p1: Arc<dyn Plan>,
    p2: Arc<dyn Plan>,
    schema: Arc<Schema>,
}

impl Plan for ProductPlan {
    fn open(&self) -> Result<Arc<std::sync::Mutex<dyn crate::query::scan::Scan>>> {
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

impl ProductPlan {
    pub fn new(p1: Arc<dyn Plan>, p2: Arc<dyn Plan>) -> Self {
        let mut schema = Schema::new();
        schema.add_all(p1.schema());
        schema.add_all(p2.schema());

        Self {
            p1,
            p2,
            schema: Arc::new(schema),
        }
    }
}
