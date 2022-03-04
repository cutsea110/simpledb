use std::sync::{Arc, Mutex};

use super::plan::Plan;
use crate::{parser::querydata::QueryData, tx::transaction::Transaction};

pub trait QueryPlanner {
    fn create_plan(data: QueryData, tx: Arc<Mutex<Transaction>>) -> Arc<dyn Plan>;
}
