use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::{plan::Plan, queryplanner::QueryPlanner, updateplanner::UpdatePlanner};
use crate::tx::transaction::Transaction;

pub struct Planner {
    qplanner: Arc<dyn QueryPlanner>,
    uplanner: Arc<dyn UpdatePlanner>,
}

impl Planner {
    pub fn new(qplanner: Arc<dyn QueryPlanner>, uplanner: Arc<dyn UpdatePlanner>) -> Self {
        Self { qplanner, uplanner }
    }
    pub fn create_query_plan(cmd: String, tx: Arc<Mutex<Transaction>>) -> Arc<dyn Plan> {
        panic!("TODO")
    }
    pub fn execute_update(cmd: String, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        panic!("TODO")
    }
}
