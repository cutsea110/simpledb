use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::{selectplan::SelectPlan, tableplan::TablePlan, updateplanner::UpdatePlanner};
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

#[derive(Debug, Clone)]
pub struct BasicUpdatePlanner {
    mdm: Arc<Mutex<MetadataMgr>>,
}

impl UpdatePlanner for BasicUpdatePlanner {
    fn execute_delete(&self, data: DeleteData, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        let p1 = Arc::new(TablePlan::new(
            data.table_name(),
            tx,
            Arc::clone(&self.mdm),
        )?);
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
    fn execute_modify(&self, data: ModifyData, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        let p1 = Arc::new(TablePlan::new(
            data.table_name(),
            tx,
            Arc::clone(&self.mdm),
        )?);
        let p2 = SelectPlan::new(p1, data.pred().clone());
        if let Ok(s) = p2.open() {
            if let Ok(us) = s.lock().unwrap().to_update_scan() {
                let mut count = 0;
                while us.next() {
                    let val = data.new_value().evaluate(us.to_scan()?)?;
                    us.set_val(data.target_field(), val)?;
                    count += 1;
                }
                us.close()?;
                return Ok(count);
            }
        }
        Err(From::from(BasicUpdatePlannerError::ModifyAbort))
    }
    fn execute_insert(&self, data: InsertData, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        let p = Arc::new(TablePlan::new(
            data.table_name(),
            tx,
            Arc::clone(&self.mdm),
        )?);
        if let Ok(s) = p.open() {
            if let Ok(us) = s.lock().unwrap().to_update_scan() {
                us.insert()?;
                let mut iter = data.vals().iter();
                for fldname in data.fields() {
                    if let Some(val) = iter.next() {
                        us.set_val(fldname, val.clone())?;
                    }
                }
                us.close()?;
                return Ok(1);
            }
        }
        Err(From::from(BasicUpdatePlannerError::InsertAbort))
    }
    fn execute_create_table(
        &self,
        data: CreateTableData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32> {
        let mdm = self.mdm.lock().unwrap();
        mdm.create_table(data.table_name(), Arc::new(data.new_schema().clone()), tx)?;
        Ok(0)
    }
    fn execute_create_view(
        &self,
        data: CreateViewData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32> {
        let mdm = self.mdm.lock().unwrap();
        mdm.create_view(data.view_name(), &data.view_def(), tx)?;
        Ok(0)
    }
    fn execute_create_index(
        &self,
        data: CreateIndexData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32> {
        let mdm = self.mdm.lock().unwrap();
        mdm.create_index(data.index_name(), data.table_name(), data.field_name(), tx)?;
        Ok(0)
    }
}

impl BasicUpdatePlanner {
    pub fn new(mdm: Arc<Mutex<MetadataMgr>>) -> Self {
        Self { mdm }
    }
}
