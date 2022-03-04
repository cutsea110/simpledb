use anyhow::Result;
use combine::Parser;
use std::sync::{Arc, Mutex};

use super::{
    plan::Plan,
    queryplanner::{self, QueryPlanner},
    updateplanner::UpdatePlanner,
};
use crate::{parser::parser::query, tx::transaction::Transaction};

#[derive(Clone)]
pub struct Planner {
    qplanner: Arc<Mutex<dyn QueryPlanner>>,
    uplanner: Arc<Mutex<dyn UpdatePlanner>>,
}

impl Planner {
    pub fn new(
        qplanner: Arc<Mutex<dyn QueryPlanner>>,
        uplanner: Arc<Mutex<dyn UpdatePlanner>>,
    ) -> Self {
        Self { qplanner, uplanner }
    }
    pub fn create_query_plan(
        &mut self,
        cmd: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Arc<dyn Plan>> {
        let mut parser = query();
        let (data, _) = parser.parse(cmd)?;
        // TODO: code to verify the query should be here...
        self.qplanner.lock().unwrap().create_plan(data, tx)
    }
    pub fn execute_update(cmd: String, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        panic!("TODO")
    }
}
