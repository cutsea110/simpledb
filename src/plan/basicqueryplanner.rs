use std::sync::{Arc, Mutex};

use super::plan::Plan;
use crate::{
    metadata::manager::MetadataMgr, parser::querydata::QueryData, tx::transaction::Transaction,
};

pub struct BasicQueryPlanner {
    mdm: MetadataMgr,
}

impl BasicQueryPlanner {
    pub fn new(mdm: MetadataMgr) -> Self {
        Self { mdm }
    }
    pub fn create_plan(&mut self, data: QueryData, tx: Arc<Mutex<Transaction>>) -> Arc<dyn Plan> {
        panic!("TODO")
    }
}
