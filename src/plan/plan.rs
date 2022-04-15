use anyhow::Result;
use std::sync::{Arc, Mutex};

use crate::{query::scan::Scan, record::schema::Schema, repr::planrepr::PlanRepr};

pub trait Plan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>>;
    fn blocks_accessed(&self) -> i32;
    fn records_output(&self) -> i32;
    fn distinct_values(&self, fldname: &str) -> i32;
    fn schema(&self) -> Arc<Schema>;
    // my own extends
    fn repr(&self) -> Arc<dyn PlanRepr>;
}
