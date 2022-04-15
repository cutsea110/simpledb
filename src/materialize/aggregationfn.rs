use std::sync::{Arc, Mutex};

use crate::query::{constant::Constant, scan::Scan};

pub mod maxfn;

pub trait AggregationFn {
    fn process_first(&self, scan: Arc<Mutex<dyn Scan>>);
    fn process_next(&self, scan: Arc<Mutex<dyn Scan>>);
    fn field_name(&self) -> String;
    fn value(&self) -> Constant;
}
