use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::{
    materializeplan::MaterializePlan, recordcomparator::RecordComparator, sortscan::SortScan,
    temptable::TempTable,
};
use crate::{
    plan::plan::Plan,
    query::{scan::Scan, updatescan::UpdateScan},
    record::schema::Schema,
    tx::transaction::Transaction,
};

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
    fn split_into_runs(&self, src: Arc<Mutex<dyn Scan>>) -> Vec<TempTable> {
        let mut temps = vec![];
        src.lock().unwrap().before_first().unwrap();
        if !src.lock().unwrap().next() {
            return temps;
        }
        let mut currenttemp = TempTable::new(
            Arc::clone(&self.next_table_num),
            Arc::clone(&self.tx),
            Arc::clone(&self.sch),
        );
        temps.push(currenttemp.clone());
        let mut currentscan = currenttemp.open().unwrap();
        while self.copy(Arc::clone(&src), Arc::clone(&currentscan)) {
            let curscan = currentscan.lock().unwrap().to_scan().unwrap();
            if self.comp.compare(Arc::clone(&src), curscan).is_lt() {
                // start a new run
                currentscan.lock().unwrap().close().unwrap();
                currenttemp = TempTable::new(
                    Arc::clone(&self.next_table_num),
                    Arc::clone(&self.tx),
                    Arc::clone(&self.sch),
                );
                temps.push(currenttemp.clone());
                currentscan = currenttemp.open().unwrap();
            }
        }
        currentscan.lock().unwrap().close().unwrap();

        temps
    }
    fn do_a_merge_iteration(&self, runs: &mut Vec<TempTable>) -> Vec<TempTable> {
        let mut result = vec![];
        while runs.len() > 1 {
            let p1 = runs.remove(0);
            let p2 = runs.remove(0);
            result.push(self.merge_two_runs(p1, p2));
        }
        if runs.len() == 1 {
            result.push(runs.get(0).unwrap().clone());
        }

        result
    }
    fn merge_two_runs(&self, mut p1: TempTable, mut p2: TempTable) -> TempTable {
        let src1 = p1.open().unwrap();
        let src2 = p2.open().unwrap();
        let mut result = TempTable::new(
            Arc::clone(&self.next_table_num),
            Arc::clone(&self.tx),
            Arc::clone(&self.sch),
        );
        let dest = result.open().unwrap();

        let mut hasmore1 = src1.lock().unwrap().next();
        let mut hasmore2 = src2.lock().unwrap().next();
        while hasmore1 && hasmore2 {
            let s1 = src1.lock().unwrap().to_scan().unwrap();
            let s2 = src2.lock().unwrap().to_scan().unwrap();
            if self.comp.compare(Arc::clone(&s1), Arc::clone(&s2)).is_lt() {
                hasmore1 = self.copy(s1, Arc::clone(&dest));
            } else {
                hasmore2 = self.copy(s2, Arc::clone(&dest));
            }
        }

        if hasmore1 {
            while hasmore1 {
                let s1 = src1.lock().unwrap().to_scan().unwrap();
                hasmore1 = self.copy(s1, Arc::clone(&dest));
            }
        } else {
            while hasmore2 {
                let s2 = src2.lock().unwrap().to_scan().unwrap();
                hasmore2 = self.copy(s2, Arc::clone(&dest));
            }
        }
        src1.lock().unwrap().close().unwrap();
        src2.lock().unwrap().close().unwrap();
        dest.lock().unwrap().close().unwrap();

        result
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

impl Plan for SortPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let src = self.p.open()?;
        let mut runs = self.split_into_runs(Arc::clone(&src));
        src.lock().unwrap().close()?;
        while runs.len() > 2 {
            runs = self.do_a_merge_iteration(&mut runs);
        }

        Ok(Arc::new(Mutex::new(SortScan::new(runs, self.comp.clone()))))
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
