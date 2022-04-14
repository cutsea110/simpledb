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
    repr::planrepr::{Operation, PlanRepr},
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
        let mut currentscan = currenttemp.open().unwrap();
        temps.push(currenttemp);
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
        let src1 = p1.open().unwrap().lock().unwrap().to_scan().unwrap();
        let src2 = p2.open().unwrap().lock().unwrap().to_scan().unwrap();
        let mut result = TempTable::new(
            Arc::clone(&self.next_table_num),
            Arc::clone(&self.tx),
            Arc::clone(&self.sch),
        );
        let dest = result.open().unwrap();

        let mut hasmore1 = src1.lock().unwrap().next();
        let mut hasmore2 = src2.lock().unwrap().next();
        while hasmore1 && hasmore2 {
            if self
                .comp
                .compare(Arc::clone(&src1), Arc::clone(&src2))
                .is_lt()
            {
                hasmore1 = self.copy(Arc::clone(&src1), Arc::clone(&dest));
            } else {
                hasmore2 = self.copy(Arc::clone(&src2), Arc::clone(&dest));
            }
        }

        if hasmore1 {
            while hasmore1 {
                hasmore1 = self.copy(Arc::clone(&src1), Arc::clone(&dest));
            }
        } else {
            while hasmore2 {
                hasmore2 = self.copy(Arc::clone(&src2), Arc::clone(&dest));
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

    fn repr(&self) -> Arc<dyn PlanRepr> {
        Arc::new(SortPlanRepr {
            p: self.p.repr(),
            compflds: self.comp.fields(),
            r: self.blocks_accessed(),
            w: self.records_output(),
        })
    }
}

#[derive(Clone)]
pub struct SortPlanRepr {
    p: Arc<dyn PlanRepr>,
    compflds: Vec<String>,
    r: i32,
    w: i32,
}

impl PlanRepr for SortPlanRepr {
    fn operation(&self) -> Operation {
        Operation::SortScan
    }
    fn reads(&self) -> i32 {
        self.r
    }
    fn writes(&self) -> i32 {
        self.w
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use std::{fs, path::Path};

    use super::*;
    use crate::{
        metadata::manager::MetadataMgr,
        plan::{plan::Plan, tableplan::TablePlan},
        query::tests,
        server::simpledb::SimpleDB,
    };

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/sortplan").exists() {
            fs::remove_dir_all("_test/sortplan")?;
        }

        let simpledb = SimpleDB::new_with("_test/sortplan", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let next_table_num = Arc::new(Mutex::new(0));
        let mut mdm = MetadataMgr::new(true, Arc::clone(&tx))?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        tests::init_sampledb(&mut mdm, Arc::clone(&tx))?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let mdm = Arc::new(Mutex::new(mdm));
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let srcplan = Arc::new(TablePlan::new(
            "STUDENT",
            Arc::clone(&tx),
            Arc::clone(&mdm),
        )?);
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let plan = SortPlan::new(
            next_table_num,
            srcplan,
            vec!["GradYear".to_string(), "SName".to_string()],
            Arc::clone(&tx),
        );

        let scan = plan.open()?;
        scan.lock().unwrap().before_first()?;
        let mut iter = scan.lock().unwrap();
        while iter.next() {
            let name = iter.get_string("SName")?;
            let year = iter.get_i32("GradYear")?;
            let major_id = iter.get_i32("MajorId")?;
            println!("{:<10}{:>8}{:>8}", name, major_id, year);
        }
        tx.lock().unwrap().commit()?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        Ok(())
    }
}
