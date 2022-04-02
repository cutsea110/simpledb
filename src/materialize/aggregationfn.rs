use std::sync::Arc;

use crate::query::{constant::Constant, scan::Scan};

pub trait AggregationFn {
    fn process_first(&mut self, scan: Arc<dyn Scan>);
    fn process_next(&mut self, scan: Arc<dyn Scan>);
    fn field_name(&self) -> String;
    fn value(&self) -> Constant;
}
