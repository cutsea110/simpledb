use anyhow::Result;
use core::fmt;
use std::{
    cmp::max,
    sync::{Arc, Mutex},
};

use super::mergejoinscan::MergeJoinScan;
use crate::{
    materialize::sortplan::SortPlan,
    plan::plan::Plan,
    query::scan::Scan,
    record::schema::Schema,
    repr::planrepr::{Operation, PlanRepr},
    tx::transaction::Transaction,
};

#[derive(Debug)]
pub enum MergeJoinPlanError {
    DowncastError,
}

impl std::error::Error for MergeJoinPlanError {}
impl fmt::Display for MergeJoinPlanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MergeJoinPlanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

#[derive(Clone)]
pub struct MergeJoinPlan {
    p1: Arc<dyn Plan>,
    p2: Arc<dyn Plan>,
    fldname1: String,
    fldname2: String,
    sch: Arc<Schema>,
}

impl MergeJoinPlan {
    pub fn new(
        next_table_num: Arc<Mutex<i32>>,

        tx: Arc<Mutex<Transaction>>,
        p1: Arc<dyn Plan>,
        p2: Arc<dyn Plan>,
        fldname1: &str,
        fldname2: &str,
    ) -> Self {
        let mut sch = Schema::new();
        sch.add_all(p1.schema());
        sch.add_all(p2.schema());

        let sortlist1 = vec![fldname1.to_string()];
        let plan1 = SortPlan::new(Arc::clone(&next_table_num), p1, sortlist1, Arc::clone(&tx));
        let sortlist2 = vec![fldname2.to_string()];
        let plan2 = SortPlan::new(Arc::clone(&next_table_num), p2, sortlist2, Arc::clone(&tx));

        Self {
            p1: Arc::new(plan1),
            p2: Arc::new(plan2),
            fldname1: fldname1.to_string(),
            fldname2: fldname2.to_string(),
            sch: Arc::new(sch),
        }
    }
}

impl Plan for MergeJoinPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let s1 = self.p1.open()?;
        if let Ok(s2) = self.p2.open() {
            let s2 = Arc::new(Mutex::new(s2.lock().unwrap().as_sort_scan()?.to_owned()));
            let scan = MergeJoinScan::new(s1, s2, &self.fldname1, &self.fldname2);

            return Ok(Arc::new(Mutex::new(scan)));
        }

        Err(From::from(MergeJoinPlanError::DowncastError))
    }
    fn blocks_accessed(&self) -> i32 {
        self.p1.blocks_accessed() + self.p2.blocks_accessed()
    }
    fn records_output(&self) -> i32 {
        let maxvals = max(
            self.p1.distinct_values(&self.fldname1),
            self.p2.distinct_values(&self.fldname2),
        );

        (self.p1.records_output() * self.p2.records_output()) / maxvals
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        if self.p1.schema().has_field(fldname) {
            self.p1.distinct_values(fldname)
        } else {
            self.p2.distinct_values(fldname)
        }
    }
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.sch)
    }

    fn repr(&self) -> Arc<dyn PlanRepr> {
        panic!("TODO")
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct MergeJoinPlanRepr {
    // TODO
}

impl PlanRepr for MergeJoinPlanRepr {
    fn operation(&self) -> Operation {
        Operation::MergeJoinScan
    }
    fn reads(&self) -> Option<i32> {
        panic!("TODO")
    }
    fn buffers(&self) -> Option<i32> {
        panic!("TODO")
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
        if Path::new("_test/mergejoinplan").exists() {
            fs::remove_dir_all("_test/mergejoinplan")?;
        }

        let simpledb = SimpleDB::new_with("_test/mergejoinplan", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let next_table_num = Arc::new(Mutex::new(0));
        let mut mdm = MetadataMgr::new(true, Arc::clone(&tx))?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        tests::init_sampledb(&mut mdm, Arc::clone(&tx))?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let mdm = Arc::new(Mutex::new(mdm));
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let p1 = Arc::new(TablePlan::new(
            "STUDENT",
            Arc::clone(&tx),
            Arc::clone(&mdm),
        )?);
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let p2 = Arc::new(TablePlan::new("DEPT", Arc::clone(&tx), Arc::clone(&mdm))?);
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let plan = MergeJoinPlan::new(next_table_num, Arc::clone(&tx), p1, p2, "MajorId", "DId");

        let scan = plan.open()?;
        scan.lock().unwrap().before_first()?;
        let mut iter = scan.lock().unwrap();
        while iter.next() {
            let sname = iter.get_string("SName")?;
            let dname = iter.get_string("DName")?;
            let did = iter.get_i32("DId")?;
            let year = iter.get_i32("GradYear")?;
            println!("{:<10}{:<10}{:>8}{:>8}", sname, dname, year, did);
        }
        tx.lock().unwrap().commit()?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        Ok(())
    }
}
