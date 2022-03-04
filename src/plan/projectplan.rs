use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::plan::Plan;
use crate::{
    query::{projectscan::ProjectScan, scan::Scan},
    record::schema::Schema,
};

#[derive(Clone)]
pub struct ProjectPlan {
    p: Arc<dyn Plan>,
    schema: Arc<Schema>,
}

impl Plan for ProjectPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let s = self.p.open()?;
        Ok(Arc::new(Mutex::new(ProjectScan::new(
            s,
            self.schema.fields().clone(),
        ))))
    }
    fn blocks_accessed(&self) -> i32 {
        self.p.blocks_accessed()
    }
    fn records_output(&self) -> i32 {
        self.p.records_output()
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        self.p.distinct_values(fldname)
    }
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}

impl ProjectPlan {
    pub fn new(p: Arc<dyn Plan>, fieldlist: Vec<String>) -> Self {
        let mut schema = Schema::new();
        for fldname in fieldlist {
            schema.add(&fldname, p.schema())
        }

        Self {
            p,
            schema: Arc::new(schema),
        }
    }
}
