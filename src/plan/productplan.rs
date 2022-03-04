use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::plan::Plan;
use crate::{
    query::{productscan::ProductScan, scan::Scan},
    record::schema::Schema,
};

pub struct ProductPlan {
    p1: Arc<dyn Plan>,
    p2: Arc<dyn Plan>,
    schema: Arc<Schema>,
}

impl Plan for ProductPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let s1 = self.p1.open()?;
        let s2 = self.p2.open()?;

        Ok(Arc::new(Mutex::new(ProductScan::new(s1, s2))))
    }
    fn blocks_accessed(&self) -> i32 {
        self.p1.blocks_accessed() + (self.p1.records_output() * self.p2.blocks_accessed())
    }
    fn records_output(&self) -> i32 {
        self.p1.records_output() * self.p2.records_output()
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        if self.p1.schema().has_field(fldname) {
            return self.p1.distinct_values(fldname);
        } else {
            return self.p2.distinct_values(fldname);
        }
    }
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
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
