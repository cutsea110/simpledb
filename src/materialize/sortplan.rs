use anyhow::Result;
use std::{
    cmp::Ordering,
    sync::{Arc, Mutex},
};

use crate::{
    plan::plan::Plan,
    query::{scan::Scan, updatescan::UpdateScan},
    record::schema::Schema,
    tx::transaction::Transaction,
};

use super::{materializeplan::MaterializePlan, sortscan::SortScan, temptable::TempTable};

#[derive(Clone)]
pub struct SortPlan {
    // static member (shared by all Materializeplan and Temptable)
    next_table_num: Arc<Mutex<i32>>,

    p: Arc<dyn Plan>,
    tx: Arc<Mutex<Transaction>>,
    sch: Arc<Schema>,
    comp: RecordComparator,
}

impl SortPlan {
    pub fn new(
        next_table_num: Arc<Mutex<i32>>,
        p: Arc<dyn Plan>,
        sortfields: Vec<String>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Self {
        let sch = p.schema();
        let comp = RecordComparator::new(sortfields);

        Self {
            next_table_num,
            p,
            tx,
            sch,
            comp,
        }
    }
    fn copy(&self, src: Arc<Mutex<dyn Scan>>, dest: Arc<Mutex<dyn UpdateScan>>) -> bool {
        dest.lock().unwrap().insert().unwrap();
        for fldname in self.sch.fields() {
            let srcval = src.lock().unwrap().get_val(fldname).unwrap();
            dest.lock().unwrap().set_val(fldname, srcval).unwrap();
        }

        src.lock().unwrap().next()
    }
}
fn split_into_runs(src: Arc<Mutex<dyn Scan>>) -> Vec<TempTable> {
    panic!("TODO")
}
fn do_a_merge_iteration(runs: &mut Vec<TempTable>) -> Vec<TempTable> {
    let mut result = vec![];
    while runs.len() > 1 {
        let p1 = runs.remove(0);
        let p2 = runs.remove(0);
        result.push(merge_two_runs(p1, p2));
    }
    if runs.len() == 1 {
        result.push(runs.get(0).unwrap().clone());
    }

    result
}
fn merge_two_runs(p1: TempTable, p2: TempTable) -> TempTable {
    panic!("TODO")
}

impl Plan for SortPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let src = self.p.open()?;
        let mut runs = split_into_runs(Arc::clone(&src));
        src.lock().unwrap().close()?;
        while runs.len() > 2 {
            runs = do_a_merge_iteration(&mut runs);
        }

        Ok(Arc::new(Mutex::new(SortScan::new(runs))))
    }
    fn blocks_accessed(&self) -> i32 {
        // does not include the one-time cost of sorting
        let mp = MaterializePlan::new(
            Arc::clone(&self.next_table_num),
            Arc::clone(&self.tx),
            Arc::clone(&self.p),
        );

        mp.blocks_accessed()
    }
    fn records_output(&self) -> i32 {
        self.p.records_output()
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        self.p.distinct_values(fldname)
    }
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.sch)
    }
}

#[derive(Debug, Clone)]
pub struct RecordComparator {
    fields: Vec<String>,
}

impl RecordComparator {
    pub fn new(fields: Vec<String>) -> Self {
        Self { fields }
    }
    pub fn compare(&self, s1: Arc<Mutex<dyn Scan>>, s2: Arc<Mutex<dyn Scan>>) -> Ordering {
        for fldname in self.fields.iter() {
            let val1 = s1.lock().unwrap().get_val(fldname).unwrap();
            let val2 = s2.lock().unwrap().get_val(fldname).unwrap();
            let result = val1.cmp(&val2);
            if result.is_ne() {
                return result;
            }
        }

        Ordering::Equal
    }
}
