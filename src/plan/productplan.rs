use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::plan::Plan;
use crate::{
    query::{productscan::ProductScan, scan::Scan},
    record::schema::Schema,
    repr::planrepr::{Operation, PlanRepr},
};

#[derive(Clone)]
pub struct ProductPlan {
    p1: Arc<dyn Plan>,
    p2: Arc<dyn Plan>,
    schema: Arc<Schema>,
}

impl Plan for ProductPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let s1 = self.p1.open()?;
        let s2 = self.p2.open()?;

        Ok(Arc::new(Mutex::new(ProductScan::new(s1, s2))))
    }
    fn blocks_accessed(&self) -> i32 {
        self.p1.blocks_accessed() + (self.p1.records_output() * self.p2.blocks_accessed())
    }
    fn records_output(&self) -> i32 {
        self.p1.records_output() * self.p2.records_output()
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        if self.p1.schema().has_field(fldname) {
            return self.p1.distinct_values(fldname);
        } else {
            return self.p2.distinct_values(fldname);
        }
    }
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn repr(&self) -> Arc<dyn PlanRepr> {
        panic!("TODO")
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ProductPlanRepr {
    // TODO
}

impl PlanRepr for ProductPlanRepr {
    fn operation(&self) -> Operation {
        Operation::ProductScan
    }
    fn reads(&self) -> Option<i32> {
        panic!("TODO")
    }
    fn buffers(&self) -> Option<i32> {
        panic!("TODO")
    }
}

impl ProductPlan {
    pub fn new(p1: Arc<dyn Plan>, p2: Arc<dyn Plan>) -> Self {
        let mut schema = Schema::new();
        schema.add_all(p1.schema());
        schema.add_all(p2.schema());

        Self {
            p1,
            p2,
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
        if Path::new("_test/productplan").exists() {
            fs::remove_dir_all("_test/productplan")?;
        }

        let simpledb = SimpleDB::new_with("_test/productplan", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

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

        let plan = ProductPlan::new(p1, p2);

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
