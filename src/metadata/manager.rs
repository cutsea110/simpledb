use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    record::{layout::Layout, schema::Schema},
    tx::transaction::Transaction,
};

use super::{
    indexmanager::{IndexInfo, IndexMgr},
    statmanager::{StatInfo, StatMgr},
    tablemanager::TableMgr,
    viewmanager::ViewMgr,
};

#[derive(Debug, Clone)]
pub struct MetadataMgr {
    tblmgr: TableMgr,
    viewmgr: ViewMgr,
    statmgr: StatMgr,
    idxmgr: IndexMgr,
}

impl MetadataMgr {
    pub fn new(isnew: bool, tx: Arc<Mutex<Transaction>>) -> Result<Self> {
        let tblmgr = TableMgr::new(isnew, Arc::clone(&tx))?;
        let viewmgr = ViewMgr::new(isnew, tblmgr.clone(), Arc::clone(&tx))?;
        let statmgr = StatMgr::new(tblmgr.clone(), Arc::clone(&tx))?;
        let idxmgr = IndexMgr::new(isnew, tblmgr.clone(), statmgr.clone(), Arc::clone(&tx))?;

        Ok(Self {
            tblmgr,
            viewmgr,
            statmgr,
            idxmgr,
        })
    }
    pub fn create_table(
        &self,
        tblname: &str,
        sch: Schema,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<()> {
        self.tblmgr.create_table(tblname, sch, tx)
    }
    pub fn get_layout(&self, tblname: &str, tx: Arc<Mutex<Transaction>>) -> Result<Layout> {
        self.tblmgr.get_layout(tblname, tx)
    }
    pub fn create_view(
        &self,
        viewname: &str,
        viewdef: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<()> {
        self.viewmgr.create_view(viewname, viewdef, tx)
    }
    pub fn get_view_def(&self, viewname: &str, tx: Arc<Mutex<Transaction>>) -> Result<String> {
        self.viewmgr.get_view_def(viewname, tx)
    }
    pub fn create_index(
        &self,
        idxname: &str,
        tblname: &str,
        fldname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<()> {
        self.idxmgr.create_index(idxname, tblname, fldname, tx)
    }
    pub fn get_index_info(
        &mut self,
        tblname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<HashMap<String, IndexInfo>> {
        self.idxmgr.get_index_info(tblname, tx)
    }
    pub fn get_stat_info(
        &mut self,
        tblname: &str,
        layout: Layout,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<StatInfo> {
        self.statmgr.get_stat_info(tblname, layout, tx)
    }
}
