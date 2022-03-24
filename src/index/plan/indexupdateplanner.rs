use anyhow::Result;
use std::sync::{Arc, Mutex};

use crate::{
    metadata::manager::MetadataMgr,
    parser::{
        createindexdata::CreateIndexData, createtabledata::CreateTableData,
        createviewdata::CreateViewData, deletedata::DeleteData, insertdata::InsertData,
        modifydata::ModifyData,
    },
    plan::updateplanner::UpdatePlanner,
    tx::transaction::Transaction,
};

pub struct IndexUpdatePlanner {
    mdm: Arc<Mutex<MetadataMgr>>,
}

impl IndexUpdatePlanner {
    pub fn new(mdm: Arc<Mutex<MetadataMgr>>) -> Self {
        panic!("TODO")
    }
}

impl UpdatePlanner for IndexUpdatePlanner {
    fn execute_insert(&self, data: InsertData, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        panic!("TODO")
    }
    fn execute_delete(&self, data: DeleteData, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        panic!("TODO")
    }
    fn execute_modify(&self, data: ModifyData, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        panic!("TODO")
    }
    fn execute_create_table(
        &self,
        data: CreateTableData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32> {
        panic!("TODO")
    }
    fn execute_create_view(
        &self,
        data: CreateViewData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32> {
        panic!("TODO")
    }
    fn execute_create_index(
        &self,
        data: CreateIndexData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32> {
        panic!("TODO")
    }
}
