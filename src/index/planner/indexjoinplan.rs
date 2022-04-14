use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use crate::{
    index::query::indexjoinscan::IndexJoinScan,
    metadata::indexmanager::IndexInfo,
    plan::plan::Plan,
    query::scan::Scan,
    record::schema::Schema,
    repr::planrepr::{Operation, PlanRepr},
};

#[derive(Debug)]
pub enum IndexJoinPlanError {
    DowncastError,
}

impl std::error::Error for IndexJoinPlanError {}
impl fmt::Display for IndexJoinPlanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &IndexJoinPlanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

pub struct IndexJoinPlan {
    p1: Arc<dyn Plan>,
    p2: Arc<dyn Plan>,
    ii: IndexInfo,
    joinfield: String,
    sch: Arc<Schema>,
}

impl IndexJoinPlan {
    pub fn new(p1: Arc<dyn Plan>, p2: Arc<dyn Plan>, ii: IndexInfo, joinfield: &str) -> Self {
        let mut sch = Schema::new();
        sch.add_all(p1.schema());
        sch.add_all(p2.schema());

        Self {
            p1,
            p2,
            ii,
            joinfield: joinfield.to_string(),
            sch: Arc::new(sch),
        }
    }
}

impl Plan for IndexJoinPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let s = self.p1.open()?;
        // throws an exception if p2 is not a table plan
        if let Ok(ts) = self.p2.open()?.lock().unwrap().as_table_scan() {
            let scan = IndexJoinScan::new(
                s,
                self.ii.open(),
                &self.joinfield,
                Arc::new(Mutex::new(ts.clone())),
            )?;
            return Ok(Arc::new(Mutex::new(scan)));
        }

        Err(From::from(IndexJoinPlanError::DowncastError))
    }
    fn blocks_accessed(&self) -> i32 {
        self.p1.blocks_accessed()
            + (self.p1.records_output() * self.ii.blocks_accessed())
            + self.records_output()
    }
    fn records_output(&self) -> i32 {
        self.p1.records_output() * self.ii.records_output()
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
pub struct IndexJoinPlanRepr {
    // TODO
}

impl PlanRepr for IndexJoinPlanRepr {
    fn operation(&self) -> Operation {
        Operation::IndexJoinScan
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
        if Path::new("_test/indexjoinplan").exists() {
            fs::remove_dir_all("_test/indexjoinplan")?;
        }

        let simpledb = SimpleDB::new_with("_test/indexjoinplan", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let mut mdm = MetadataMgr::new(true, Arc::clone(&tx))?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        tests::init_sampledb(&mut mdm, Arc::clone(&tx))?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let mdm = Arc::new(Mutex::new(mdm));
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let student_plan = Arc::new(TablePlan::new(
            "STUDENT",
            Arc::clone(&tx),
            Arc::clone(&mdm),
        )?);
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let dept_plan = Arc::new(TablePlan::new("DEPT", Arc::clone(&tx), Arc::clone(&mdm))?);
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let iimap = mdm
            .lock()
            .unwrap()
            .get_index_info("STUDENT", Arc::clone(&tx))?;
        let ii = iimap.get("MajorId").unwrap().clone();
        let p1 = Arc::clone(&dept_plan);
        let p2 = Arc::clone(&student_plan);
        let plan = IndexJoinPlan::new(p1, p2, ii, "DId");

        let scan = plan.open()?;
        scan.lock().unwrap().before_first()?;
        let mut iter = scan.lock().unwrap();
        while iter.next() {
            let sname = iter.get_string("SName")?;
            let dname = iter.get_string("DName")?;
            let year = iter.get_i32("GradYear")?;
            println!("{:<10}{:<10}{:>8}", sname, dname, year);
        }
        tx.lock().unwrap().commit()?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        Ok(())
    }
}
