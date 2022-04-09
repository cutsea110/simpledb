use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::tableplanner::TablePlanner;
use crate::{
    metadata::manager::MetadataMgr,
    parser::querydata::QueryData,
    plan::{plan::Plan, queryplanner::QueryPlanner},
    tx::transaction::Transaction,
};

pub struct HeuristicQueryPlanner {
    tableplanners: Vec<TablePlanner>,
    mdm: Arc<Mutex<MetadataMgr>>,
}

impl HeuristicQueryPlanner {
    pub fn new(mdm: Arc<Mutex<MetadataMgr>>) -> Self {
        panic!("TODO")
    }
}

impl QueryPlanner for HeuristicQueryPlanner {
    fn create_plan(
        &mut self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Arc<dyn Plan>> {
        panic!("TODO")
    }
}
