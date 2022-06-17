use anyhow::Result;
use core::fmt;
use log::debug;
use std::sync::{Arc, Mutex};

use crate::{
    metadata::manager::MetadataMgr,
    parser::{
        createindexdata::CreateIndexData, createtabledata::CreateTableData,
        createviewdata::CreateViewData, deletedata::DeleteData, insertdata::InsertData,
        modifydata::ModifyData,
    },
    plan::{
        plan::Plan, selectplan::SelectPlan, tableplan::TablePlan, updateplanner::UpdatePlanner,
    },
    tx::transaction::Transaction,
};

#[derive(Debug)]
pub enum IndexUpdatePlannerError {
    DowncastError,
}

impl std::error::Error for IndexUpdatePlannerError {}
impl fmt::Display for IndexUpdatePlannerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IndexUpdatePlannerError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

#[derive(Debug)]
pub struct IndexUpdatePlanner {
    mdm: Arc<Mutex<MetadataMgr>>,
}

impl IndexUpdatePlanner {
    pub fn new(mdm: Arc<Mutex<MetadataMgr>>) -> Self {
        Self { mdm }
    }
}

impl UpdatePlanner for IndexUpdatePlanner {
    fn execute_insert(&self, data: InsertData, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        let tblname = data.table_name();
        let p = TablePlan::new(tblname, Arc::clone(&tx), Arc::clone(&self.mdm))?;
        // first, insert the record
        if let Ok(s) = p.open()?.lock().unwrap().to_update_scan() {
            s.insert()?;
            let rid = s.get_rid()?;
            // then modify each field, inserting index records
            let mut md = self.mdm.lock().unwrap();
            let indexes = md.get_index_info(tblname, Arc::clone(&tx))?;
            let mut valiter = data.vals().iter();
            for fldname in data.fields() {
                let val = valiter.next().unwrap();
                debug!("Modify field {} to val {:?}", fldname, &val);
                // NOTE: UpdateScan can convert val to the correct type.
                s.set_val(fldname, val.clone())?;
                if let Some(ii) = indexes.get(fldname) {
                    // NOTE: convert the type here, because Index doesn't convert val.
                    let fldtype = ii.table_schema().field_type(fldname);
                    let val = val.as_field_type(fldtype)?;

                    let idx = ii.open();
                    idx.lock().unwrap().insert(val, rid)?;
                    idx.lock().unwrap().close()?;
                }
            }
            s.close()?;

            return Ok(1);
        }

        Err(From::from(IndexUpdatePlannerError::DowncastError))
    }
    fn execute_delete(&self, data: DeleteData, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        let tblname = data.table_name();
        let tp = TablePlan::new(tblname, Arc::clone(&tx), Arc::clone(&self.mdm))?;
        let p = SelectPlan::new(Arc::new(tp), data.pred().clone());
        let mut md = self.mdm.lock().unwrap();
        let indexes = md.get_index_info(tblname, Arc::clone(&tx))?;

        if let Ok(s) = p.open()?.lock().unwrap().to_update_scan() {
            let mut count = 0;
            while s.next() {
                // first, delete the record's RID from every index
                let rid = s.get_rid()?;
                for fldname in indexes.keys() {
                    // NOTE: UpdateScan can convert val to the correct type.
                    let val = s.get_val(fldname)?;
                    if let Some(ii) = indexes.get(fldname) {
                        // NOTE: convert the type here, because Index doesn't convert val.
                        let fldtype = ii.table_schema().field_type(fldname);
                        let val = val.as_field_type(fldtype)?;

                        let idx = ii.open();
                        idx.lock().unwrap().delete(val, rid)?;
                        idx.lock().unwrap().close()?;
                    }
                }
                // then delete the record
                s.delete()?;
                count += 1;
            }
            s.close()?;

            return Ok(count);
        }

        Err(From::from(IndexUpdatePlannerError::DowncastError))
    }
    fn execute_modify(&self, data: ModifyData, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        let tblname = data.table_name();
        let fldname = data.target_field();
        let tp = TablePlan::new(tblname, Arc::clone(&tx), Arc::clone(&self.mdm))?;
        let p = SelectPlan::new(Arc::new(tp), data.pred().clone());
        let mut md = self.mdm.lock().unwrap();
        let indexes = md.get_index_info(tblname, Arc::clone(&tx))?;
        let ii = indexes.get(fldname);
        let idx = ii.map(|ii| ii.open());

        if let Ok(s) = p.open()?.lock().unwrap().to_update_scan() {
            let mut count = 0;
            while s.next() {
                // first, update the record
                let scan = s.to_scan()?;
                let newval = data.new_value().evaluate(scan)?;
                let oldval = s.get_val(fldname)?;
                // NOTE: UpdateScan can convert val to the correct type.
                s.set_val(data.target_field(), newval.clone())?;
                // then update the appropriate index, if it exists
                if let Some(idx) = idx.as_ref() {
                    // NOTE: convert the type here, because Index doesn't convert val.
                    let fldtype = ii.unwrap().table_schema().field_type(fldname);
                    let oldval = oldval.as_field_type(fldtype)?;
                    let newval = newval.as_field_type(fldtype)?;

                    let rid = s.get_rid()?;
                    idx.lock().unwrap().delete(oldval, rid)?;
                    idx.lock().unwrap().insert(newval, rid)?;
                }
                count += 1;
            }
            if let Some(idx) = idx.as_ref() {
                idx.lock().unwrap().close()?;
            }
            s.close()?;

            return Ok(count);
        }

        Err(From::from(IndexUpdatePlannerError::DowncastError))
    }
    fn execute_create_table(
        &self,
        data: CreateTableData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32> {
        let md = self.mdm.lock().unwrap();
        md.create_table(data.table_name(), Arc::new(data.new_schema().clone()), tx)?;
        Ok(0)
    }
    fn execute_create_view(
        &self,
        data: CreateViewData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32> {
        let md = self.mdm.lock().unwrap();
        md.create_view(data.view_name(), &data.view_def(), tx)?;
        Ok(0)
    }
    fn execute_create_index(
        &self,
        data: CreateIndexData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32> {
        let md = self.mdm.lock().unwrap();
        md.create_index(data.index_name(), data.table_name(), data.field_name(), tx)?;
        Ok(0)
    }
}
