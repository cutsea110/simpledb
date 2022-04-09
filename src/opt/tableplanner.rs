use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    metadata::{indexmanager::IndexInfo, manager::MetadataMgr},
    plan::tableplan::TablePlan,
    query::predicate::Predicate,
    record::schema::Schema,
    tx::transaction::Transaction,
};

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
}
