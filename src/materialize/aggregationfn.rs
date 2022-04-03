use std::sync::{Arc, Mutex};

use crate::query::{constant::Constant, scan::Scan};

pub trait AggregationFn {
    fn process_first(&mut self, scan: Arc<Mutex<dyn Scan>>);
    fn process_next(&mut self, scan: Arc<Mutex<dyn Scan>>);
    fn field_name(&self) -> String;
    fn value(&self) -> Constant;
}
