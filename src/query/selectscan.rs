use std::sync::Arc;

use super::{predicate::Predicate, scan::Scan};

pub struct SelectScan {
    s: Arc<dyn Scan>,
    pred: Predicate,
}
