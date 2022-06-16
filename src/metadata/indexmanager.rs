use anyhow::Result;
use core::fmt;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::{
    statmanager::{StatInfo, StatMgr},
    tablemanager::{TableMgr, MAX_NAME},
};
use crate::{
    index::{btree::index::BTreeIndex, Index},
    query::{scan::Scan, updatescan::UpdateScan},
    record::{layout::Layout, schema::FieldType, schema::Schema, tablescan::TableScan},
    tx::transaction::Transaction,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IndexMgr {
    layout: Arc<Layout>,
    tblmgr: TableMgr,
    statmgr: StatMgr,
}

impl IndexMgr {
    pub fn new(
        isnew: bool,
        tblmgr: TableMgr,
        statmgr: StatMgr,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Self> {
        if isnew {
            let mut sch = Schema::new();
            sch.add_string_field("indexname", MAX_NAME);
            sch.add_string_field("tablename", MAX_NAME);
            sch.add_string_field("fieldname", MAX_NAME);
            tblmgr.create_table("idxcat", Arc::new(sch), Arc::clone(&tx))?;
        }
        let layout = tblmgr.get_layout("idxcat", tx)?;

        Ok(Self {
            layout,
            tblmgr,
            statmgr,
        })
    }
    pub fn create_index(
        &self,
        idxname: &str,
        tblname: &str,
        fldname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<()> {
        let mut ts = TableScan::new(tx, "idxcat", Arc::clone(&self.layout))?;
        ts.insert()?;
        ts.set_string("indexname", idxname.to_string())?;
        ts.set_string("tablename", tblname.to_string())?;
        ts.set_string("fieldname", fldname.to_string())?;
        ts.close()?;

        Ok(())
    }
    pub fn get_index_info(
        &mut self,
        tblname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<HashMap<String, IndexInfo>> {
        let mut result = HashMap::new();
        let mut ts = TableScan::new(Arc::clone(&tx), "idxcat", Arc::clone(&self.layout))?;
        while ts.next() {
            if ts.get_string("tablename")? == tblname {
                let idxname = ts.get_string("indexname")?;
                let fldname: String = ts.get_string("fieldname")?;
                let tbl_layout = self.tblmgr.get_layout(tblname, Arc::clone(&tx))?;
                let tblsi =
                    self.statmgr
                        .get_stat_info(&tblname, tbl_layout.clone(), Arc::clone(&tx))?;
                let ii = IndexInfo::new(
                    idxname,
                    fldname.clone(),
                    tbl_layout.schema(),
                    Arc::clone(&tx),
                    tblsi,
                );
                result.insert(fldname, ii);
            }
        }
        ts.close()?;

        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct IndexInfo {
    idxname: String,
    fldname: String,
    tx: Arc<Mutex<Transaction>>,
    tbl_schema: Arc<Schema>,
    idx_layout: Arc<Layout>,
    si: StatInfo,
}

impl fmt::Display for IndexInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IndexInfo{{idxname:{}, fldname:{}}}",
            self.idxname, self.fldname
        )
    }
}

impl IndexInfo {
    pub fn new(
        idxname: String,
        fldname: String,
        tbl_schema: Arc<Schema>,
        tx: Arc<Mutex<Transaction>>,
        si: StatInfo,
    ) -> Self {
        let sch = Schema::new();
        let layout = Arc::new(Layout::new(Arc::new(sch)));

        let mut mgr = Self {
            idxname,
            fldname,
            tx,
            tbl_schema,
            idx_layout: layout, // dummy
            si,
        };

        mgr.idx_layout = mgr.create_idx_layout();

        mgr
    }
    pub fn open(&self) -> Arc<Mutex<dyn Index>> {
        let idx = BTreeIndex::new(
            Arc::clone(&self.tx),
            &self.idxname,
            Arc::clone(&self.idx_layout),
        )
        .expect("create index");

        Arc::new(Mutex::new(idx))
    }
    pub fn blocks_accessed(&self) -> i32 {
        let rpb = self.tx.lock().unwrap().block_size() / self.idx_layout.slot_size() as i32;
        let numblocks = (self.si.records_output() as f32 / rpb as f32).ceil() as i32;
        BTreeIndex::search_cost(numblocks, rpb)
    }
    pub fn records_output(&self) -> i32 {
        self.si.records_output() / self.si.distinct_values(&self.fldname)
    }
    pub fn distinct_values(&self, fname: &str) -> i32 {
        if self.fldname == fname {
            return 1;
        } else {
            return self.si.distinct_values(&self.fldname);
        }
    }
    fn create_idx_layout(&mut self) -> Arc<Layout> {
        let mut sch = Schema::new();
        sch.add_i32_field("block");
        sch.add_i32_field("id");
        match self.tbl_schema.field_type(&self.fldname) {
            FieldType::SMALLINT => {
                sch.add_i16_field("dataval");
            }
            FieldType::INTEGER => {
                sch.add_i32_field("dataval");
            }
            FieldType::VARCHAR => {
                let fldlen = self.tbl_schema.length(&self.fldname);
                sch.add_string_field("dataval", fldlen);
            }
            FieldType::BOOL => {
                sch.add_bool_field("dataval");
            }
            FieldType::DATE => {
                sch.add_date_field("dataval");
            }
        }

        Arc::new(Layout::new(Arc::new(sch)))
    }
    // my own extend
    pub fn index_name(&self) -> &str {
        &self.idxname
    }
    // my own extend
    pub fn field_name(&self) -> &str {
        &self.fldname
    }
    // my own extend
    pub fn table_schema(&self) -> Arc<Schema> {
        Arc::clone(&self.tbl_schema)
    }
}
