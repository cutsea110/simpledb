use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::plan::Plan;
use crate::{parser::querydata::QueryData, tx::transaction::Transaction};

pub trait QueryPlanner {
    fn create_plan(
        &mut self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Arc<dyn Plan>>;
}
