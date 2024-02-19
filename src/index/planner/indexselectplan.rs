use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use crate::{
    index::query::indexselectscan::IndexSelectScan,
    metadata::indexmanager::IndexInfo,
    plan::plan::Plan,
    query::{constant::Constant, scan::Scan},
    record::schema::Schema,
    repr::planrepr::{Operation, PlanRepr},
};

#[derive(Debug)]
pub enum IndexSelectPlanError {
    DowncastError,
}
impl std::error::Error for IndexSelectPlanError {}
impl fmt::Display for IndexSelectPlanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IndexSelectPlanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

pub struct IndexSelectPlan {
    p: Arc<dyn Plan>,
    ii: IndexInfo,
    val: Constant,
}

impl IndexSelectPlan {
    pub fn new(p: Arc<dyn Plan>, ii: IndexInfo, val: Constant) -> Self {
        Self { p, ii, val }
    }
}

impl Plan for IndexSelectPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        // throws an exception if p is not a table plan.
        if let Ok(ts) = self.p.open()?.lock().unwrap().as_table_scan() {
            // NOTE: need to convert val to the correct type.
            let fldname = self.ii.field_name();
            let fldtype = self.ii.table_schema().field_type(fldname);
            let val = self.val.as_field_type(fldtype)?;

            let scan = IndexSelectScan::new(Arc::new(Mutex::new(ts.clone())), self.ii.open(), val)?;
            return Ok(Arc::new(Mutex::new(scan)));
        }

        Err(From::from(IndexSelectPlanError::DowncastError))
    }
    fn blocks_accessed(&self) -> i32 {
        self.ii.blocks_accessed() + self.records_output()
    }
    fn records_output(&self) -> i32 {
        self.ii.records_output()
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        self.ii.distinct_values(fldname)
    }
    fn schema(&self) -> Arc<Schema> {
        self.p.schema()
    }

    fn repr(&self) -> Arc<dyn PlanRepr> {
        Arc::new(IndexSelectPlanRepr {
            p: self.p.repr(),
            idxname: self.ii.index_name().to_string(),
            idxfldname: self.ii.field_name().to_string(),
            val: self.val.clone(),
            r: self.blocks_accessed(),
            w: self.records_output(),
        })
    }
}

#[derive(Clone)]
pub struct IndexSelectPlanRepr {
    p: Arc<dyn PlanRepr>,
    idxname: String,
    idxfldname: String,
    val: Constant,
    r: i32,
    w: i32,
}

impl PlanRepr for IndexSelectPlanRepr {
    fn operation(&self) -> Operation {
        Operation::IndexSelectScan {
            idxname: self.idxname.clone(),
            idxfldname: self.idxfldname.clone(),
            val: self.val.clone(),
        }
    }
    fn reads(&self) -> i32 {
        self.r
    }
    fn writes(&self) -> i32 {
        self.w
    }
    fn sub_plan_reprs(&self) -> Vec<Arc<dyn PlanRepr>> {
        vec![Arc::clone(&self.p)]
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::*;
    use crate::{
        metadata::manager::MetadataMgr, plan::tableplan::TablePlan, query::tests,
        server::simpledb::SimpleDB,
    };

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/indexselectplan").exists() {
            fs::remove_dir_all("_test/indexselectplan")?;
        }

        let simpledb = SimpleDB::new_with("_test/indexselectplan", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

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

        let iimap = mdm
            .lock()
            .unwrap()
            .get_index_info("STUDENT", Arc::clone(&tx))?;
        let ii = iimap.get("GradYear").unwrap().clone();
        let p = Arc::clone(&srcplan);
        let plan = IndexSelectPlan::new(p, ii, Constant::I32(2020));

        let scan = plan.open()?;
        scan.lock().unwrap().before_first()?;
        let mut iter = scan.lock().unwrap();
        while iter.next() {
            let name = iter.get_string("SName")?;
            let year = iter.get_i32("GradYear")?;
            println!("{:<10}{:>8}", name, year);
        }
        tx.lock().unwrap().commit()?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        Ok(())
    }
}
