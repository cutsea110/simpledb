use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::temptable::TempTable;
use crate::{
    plan::plan::Plan,
    query::scan::Scan,
    record::{layout::Layout, schema::Schema},
    tx::transaction::Transaction,
};

pub struct MaterializePlan {
    // static member (shared by all Materializeplan and Temptable)
    next_table_num: Arc<Mutex<i32>>,

    srcplan: Arc<dyn Plan>,
    tx: Arc<Mutex<Transaction>>,
}

impl MaterializePlan {
    pub fn new(
        next_table_num: Arc<Mutex<i32>>,
        tx: Arc<Mutex<Transaction>>,
        srcplan: Arc<dyn Plan>,
    ) -> Self {
        Self {
            next_table_num,
            srcplan,
            tx,
        }
    }
}

impl Plan for MaterializePlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let sch = self.srcplan.schema();
        let mut temp = TempTable::new(
            Arc::clone(&self.next_table_num),
            Arc::clone(&self.tx),
            Arc::clone(&sch),
        );
        let src = self.srcplan.open()?;
        let dest = temp.open()?;
        while src.lock().unwrap().next() {
            dest.lock().unwrap().insert()?;
            for fldname in sch.fields() {
                dest.lock()
                    .unwrap()
                    .set_val(fldname, src.lock().unwrap().get_val(fldname)?)?;
            }
        }
        src.lock().unwrap().close()?;
        dest.lock().unwrap().before_first()?;

        let dest = dest.lock().unwrap().to_scan()?;
        Ok(dest)
    }
    fn blocks_accessed(&self) -> i32 {
        // create a dummy Layout object to calculate slot size
        let y = Layout::new(self.srcplan.schema());
        let rpb = (self.tx.lock().unwrap().block_size() / y.slot_size() as i32) as f32;
        (self.srcplan.records_output() as f32 / rpb).ceil() as i32
    }
    fn records_output(&self) -> i32 {
        self.srcplan.records_output()
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        self.srcplan.distinct_values(fldname)
    }
    fn schema(&self) -> Arc<Schema> {
        self.srcplan.schema()
    }
}
