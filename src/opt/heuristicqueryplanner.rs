use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::tableplanner::TablePlanner;
use crate::{
    metadata::manager::MetadataMgr,
    parser::querydata::QueryData,
    plan::{plan::Plan, planner::Planner, queryplanner::QueryPlanner},
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
    fn get_lowest_select_plan(&self) -> Result<Arc<dyn Plan>> {
        panic!("TODO")
    }
    fn get_lowest_join_plan(&self, current: Arc<dyn Plan>) -> Result<Arc<dyn Plan>> {
        panic!("TODO")
    }
    fn get_lowest_product_plan(&self, current: Arc<dyn Plan>) -> Result<Arc<dyn Plan>> {
        panic!("TODO")
    }
    pub fn set_planner(&mut self, p: Planner) {
        // for use in planning views, which
        // for simplicity this code doesn't do.
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
