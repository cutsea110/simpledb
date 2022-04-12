use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::multibufferproductscan::MultibufferProductScan;
use crate::{
    materialize::{materializeplan::MaterializePlan, temptable::TempTable},
    plan::plan::Plan,
    query::scan::Scan,
    record::schema::Schema,
    tx::transaction::Transaction,
};

#[derive(Debug)]
pub enum MultibufferProductPlanError {
    DowncastError,
}

impl std::error::Error for MultibufferProductPlanError {}
impl fmt::Display for MultibufferProductPlanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MultibufferProductPlanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

#[derive(Clone)]
pub struct MultibufferProductPlan {
    // static member (shared by all Materializeplan and Temptable)
    next_table_num: Arc<Mutex<i32>>,

    tx: Arc<Mutex<Transaction>>,
    lhs: Arc<dyn Plan>,
    rhs: Arc<dyn Plan>,
    schema: Arc<Schema>,
}

impl MultibufferProductPlan {
    pub fn new(
        next_table_num: Arc<Mutex<i32>>,
        tx: Arc<Mutex<Transaction>>,
        lhs: Arc<dyn Plan>,
        rhs: Arc<dyn Plan>,
    ) -> Self {
        let lhs = Arc::new(MaterializePlan::new(
            Arc::clone(&next_table_num),
            Arc::clone(&tx),
            Arc::clone(&lhs),
        ));
        let mut schema = Schema::new();
        schema.add_all(lhs.schema());
        schema.add_all(rhs.schema());

        Self {
            next_table_num,
            tx: Arc::clone(&tx),
            lhs,
            rhs,
            schema: Arc::new(schema),
        }
    }
    fn copy_records_from(&self, p: Arc<dyn Plan>) -> Result<TempTable> {
        let src = p.open()?;
        let sch = p.schema();
        let mut tt = TempTable::new(
            Arc::clone(&self.next_table_num),
            Arc::clone(&self.tx),
            Arc::clone(&sch),
        );

        if let Ok(dest) = tt.open()?.lock().unwrap().to_update_scan() {
            while src.lock().unwrap().next() {
                dest.insert()?;
                for fldname in sch.fields().iter() {
                    dest.set_val(fldname, src.lock().unwrap().get_val(fldname)?)?;
                }
            }
            src.lock().unwrap().close()?;
            dest.close()?;

            return Ok(tt);
        }

        Err(From::from(MultibufferProductPlanError::DowncastError))
    }
}

impl Plan for MultibufferProductPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let leftscan = self.lhs.open()?;
        let t = self.copy_records_from(Arc::clone(&self.rhs))?;
        let scan = MultibufferProductScan::new(
            Arc::clone(&self.tx),
            leftscan,
            t.table_name(),
            t.get_layout(),
        );

        Ok(Arc::new(Mutex::new(scan)))
    }
    fn blocks_accessed(&self) -> i32 {
        // this guesses at the % of chunks
        let avail = self.tx.lock().unwrap().available_buffs() as i32;
        let size = MaterializePlan::new(
            Arc::clone(&self.next_table_num),
            Arc::clone(&self.tx),
            Arc::clone(&self.rhs),
        )
        .blocks_accessed();
        let numchunks = size / avail;
        self.rhs.blocks_accessed() + (self.lhs.blocks_accessed() * numchunks)
    }
    fn records_output(&self) -> i32 {
        self.lhs.records_output() * self.rhs.records_output()
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        if self.lhs.schema().has_field(fldname) {
            self.lhs.distinct_values(fldname)
        } else {
            self.rhs.distinct_values(fldname)
        }
    }
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
    fn dump(&self) -> String {
        format!(
            "MultibufferProductPlan{{lhs:{}, rhs:{}, fields:{:?}}}",
            self.lhs.dump(),
            self.rhs.dump(),
            self.schema.fields()
        )
    }
}
