use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::{selectplan::SelectPlan, tableplan::TablePlan};
use crate::{
    metadata::manager::MetadataMgr,
    parser::{
        createindexdata::CreateIndexData, createtabledata::CreateTableData,
        createviewdata::CreateViewData, deletedata::DeleteData, insertdata::InsertData,
        modifydata::ModifyData,
    },
    plan::plan::Plan,
    tx::transaction::Transaction,
};

#[derive(Debug)]
pub enum BasicUpdatePlannerError {
    DeleteAbort,
    InsertAbort,
    ModifyAbort,
}

impl std::error::Error for BasicUpdatePlannerError {}
impl fmt::Display for BasicUpdatePlannerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BasicUpdatePlannerError::DeleteAbort => {
                write!(f, "delete abort")
            }
            BasicUpdatePlannerError::InsertAbort => {
                write!(f, "insert abort")
            }
            BasicUpdatePlannerError::ModifyAbort => {
                write!(f, "modify abort")
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BasicUpdatePlanner {
    mdm: MetadataMgr,
}

impl BasicUpdatePlanner {
    pub fn new(mdm: MetadataMgr) -> Self {
        Self { mdm }
    }
    pub fn execute_delete(&self, data: DeleteData, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        let p1 = Arc::new(TablePlan::new(data.table_name(), tx, self.mdm.clone())?);
        let p2 = SelectPlan::new(p1, data.pred().clone());
        if let Ok(s) = p2.open() {
            if let Ok(us) = s.lock().unwrap().to_update_scan() {
                let mut count = 0;
                while us.next() {
                    us.delete()?;
                    count += 1;
                }
                us.close()?;
                return Ok(count);
            }
        }
        Err(From::from(BasicUpdatePlannerError::DeleteAbort))
    }
    pub fn execute_modify(&self, data: ModifyData, tx: Arc<Mutex<Transaction>>) -> i32 {
        panic!("TODO")
    }
    pub fn execute_insert(&self, data: InsertData, tx: Arc<Mutex<Transaction>>) -> i32 {
        panic!("TODO")
    }
    pub fn execute_create_table(&self, data: CreateTableData, tx: Arc<Mutex<Transaction>>) -> i32 {
        panic!("TODO")
    }
    pub fn execute_create_view(&self, data: CreateViewData, tx: Arc<Mutex<Transaction>>) -> i32 {
        panic!("TODO")
    }
    pub fn execute_create_index(&self, data: CreateIndexData, tx: Arc<Mutex<Transaction>>) -> i32 {
        panic!("TODO")
    }
}
