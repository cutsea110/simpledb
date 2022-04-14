use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::plan::Plan;
use crate::{
    query::{projectscan::ProjectScan, scan::Scan},
    record::schema::Schema,
    repr::planrepr::{Operation, PlanRepr},
};

#[derive(Clone)]
pub struct ProjectPlan {
    p: Arc<dyn Plan>,
    schema: Arc<Schema>,
}

impl Plan for ProjectPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let s = self.p.open()?;
        Ok(Arc::new(Mutex::new(ProjectScan::new(
            s,
            self.schema.fields().clone(),
        ))))
    }
    fn blocks_accessed(&self) -> i32 {
        self.p.blocks_accessed()
    }
    fn records_output(&self) -> i32 {
        self.p.records_output()
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        self.p.distinct_values(fldname)
    }
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn repr(&self) -> Arc<dyn PlanRepr> {
        panic!("TODO")
    }
}

impl PlanRepr for ProjectPlan {
    fn operation(&self) -> Operation {
        Operation::ProjectScan
    }
    fn reads(&self) -> Option<i32> {
        panic!("TODO")
    }
    fn buffers(&self) -> Option<i32> {
        panic!("TODO")
    }
}

impl ProjectPlan {
    pub fn new(p: Arc<dyn Plan>, fieldlist: Vec<String>) -> Self {
        let mut schema = Schema::new();
        for fldname in fieldlist {
            schema.add(&fldname, p.schema())
        }

        Self {
            p,
            schema: Arc::new(schema),
        }
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
        if Path::new("_test/projectplan").exists() {
            fs::remove_dir_all("_test/projectplan")?;
        }

        let simpledb = SimpleDB::new_with("_test/projectplan", 400, 8);

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

        let plan = ProjectPlan::new(srcplan, vec!["SName".to_string(), "MajorId".to_string()]);

        let scan = plan.open()?;
        scan.lock().unwrap().before_first()?;
        let mut iter = scan.lock().unwrap();
        while iter.next() {
            let name = iter.get_string("SName")?;
            let major_id = iter.get_i32("MajorId")?;
            println!("{:<10}{:>8}", name, major_id);
        }
        tx.lock().unwrap().commit()?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        Ok(())
    }
}
