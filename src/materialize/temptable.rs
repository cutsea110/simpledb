use anyhow::Result;
use std::sync::{Arc, Mutex};

use crate::{
    query::updatescan::UpdateScan,
    record::{layout::Layout, schema::Schema, tablescan::TableScan},
    tx::transaction::Transaction,
};

pub struct TempTable {
    // static member (shared by all TempTable)
    next_table_num: Arc<Mutex<i32>>,

    tx: Arc<Mutex<Transaction>>,
    tblname: String,
    layout: Arc<Layout>,
}

impl TempTable {
    pub fn new(
        next_table_num: Arc<Mutex<i32>>,
        tx: Arc<Mutex<Transaction>>,
        sch: Arc<Schema>,
    ) -> Self {
        let mut tt = Self {
            next_table_num,
            tx,
            tblname: "".to_string(), // dummy
            layout: Arc::new(Layout::new(sch)),
        };
        tt.tblname = tt.next_table_name();

        tt
    }
    pub fn open(&mut self) -> Result<Arc<Mutex<dyn UpdateScan>>> {
        let ts = TableScan::new(
            Arc::clone(&self.tx),
            &self.tblname,
            Arc::clone(&self.layout),
        )?;

        Ok(Arc::new(Mutex::new(ts)))
    }
    pub fn table_name(&self) -> &str {
        &self.tblname
    }
    pub fn get_layout(&self) -> Arc<Layout> {
        Arc::clone(&self.layout)
    }
    // synchronized
    fn next_table_name(&mut self) -> String {
        let mut next_table_num = self.next_table_num.lock().unwrap();
        *next_table_num += 1;

        format!("temp{}", *next_table_num)
    }
}
