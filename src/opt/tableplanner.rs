use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    metadata::{indexmanager::IndexInfo, manager::MetadataMgr},
    plan::{plan::Plan, tableplan::TablePlan},
    query::predicate::Predicate,
    record::schema::Schema,
    tx::transaction::Transaction,
};

#[derive(Debug, Clone)]
pub struct TablePlanner {
    myplan: Arc<TablePlan>,
    mypred: Predicate,
    myschema: Arc<Schema>,
    indexes: HashMap<String, IndexInfo>,
    tx: Arc<Mutex<Transaction>>,
}

impl TablePlanner {
    pub fn new(
        tblname: &str,
        mypred: Predicate,
        tx: Arc<Mutex<Transaction>>,
        mdm: Arc<Mutex<MetadataMgr>>,
    ) -> Self {
        panic!("TODO")
    }
    pub fn make_select_plan(&self) -> Arc<dyn Plan> {
        panic!("TODO")
    }
    pub fn make_join_plan(&self, current: Arc<dyn Plan>) -> Option<Arc<dyn Plan>> {
        panic!("TODO")
    }
    pub fn make_product_plan(&self, current: Arc<dyn Plan>) -> Arc<dyn Plan> {
        panic!("TODO")
    }
    fn make_index_select(&self) -> Arc<dyn Plan> {
        panic!("TODO")
    }
    fn make_index_join(current: Arc<dyn Plan>, currsch: Arc<Schema>) -> Arc<dyn Plan> {
        panic!("TODO")
    }
    fn make_product_join(current: Arc<dyn Plan>, currsch: Arc<Schema>) -> Arc<dyn Plan> {
        panic!("TODO")
    }
    fn add_select_pred(p: Arc<dyn Plan>) -> Arc<dyn Plan> {
        panic!("TODO")
    }
    fn add_join_pred(current: Arc<dyn Plan>, currsch: Arc<Schema>) -> Arc<dyn Plan> {
        panic!("TODO")
    }
}
