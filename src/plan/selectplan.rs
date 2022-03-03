use std::cmp::*;
use std::sync::{Arc, Mutex};

use anyhow::Result;

use super::plan::Plan;
use crate::{
    query::{predicate::Predicate, scan::Scan, selectscan::SelectScan},
    record::schema::Schema,
};

pub struct SelectPlan {
    p: Arc<dyn Plan>,
    pred: Predicate,
}

impl Plan for SelectPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let s = self.p.open()?;
        Ok(Arc::new(Mutex::new(SelectScan::new(s, self.pred.clone()))))
    }
    fn blocks_accessed(&self) -> i32 {
        self.p.blocks_accessed()
    }
    fn records_output(&self) -> i32 {
        self.p.records_output() / self.pred.reduction_factor(Arc::clone(&self.p)).unwrap()
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        if self.pred.equates_with_constant(fldname).is_some() {
            return 1;
        }
        if let Some(fldname2) = self.pred.equates_with_field(fldname) {
            return min(
                self.p.distinct_values(fldname),
                self.p.distinct_values(fldname2),
            );
        }

        self.p.distinct_values(fldname)
    }
    fn schema(&self) -> Arc<Schema> {
        self.p.schema()
    }
}
