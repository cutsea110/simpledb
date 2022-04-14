use anyhow::Result;
use std::{
    cmp::*,
    sync::{Arc, Mutex},
};

use super::plan::Plan;
use crate::{
    query::{predicate::Predicate, scan::Scan, selectscan::SelectScan},
    record::schema::Schema,
    repr::planrepr::{Operation, PlanRepr},
};

#[derive(Clone)]
pub struct SelectPlan {
    p: Arc<dyn Plan>,
    pred: Predicate,
}

impl Plan for SelectPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let s = self.p.open()?;
        Ok(Arc::new(Mutex::new(SelectScan::new(s, self.pred.clone()))))
    }
    fn blocks_accessed(&self) -> i32 {
        self.p.blocks_accessed()
    }
    fn records_output(&self) -> i32 {
        self.p.records_output() / self.pred.reduction_factor(Arc::clone(&self.p))
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        if self.pred.equates_with_constant(fldname).is_some() {
            return 1;
        }
        if let Some(fldname2) = self.pred.equates_with_field(fldname) {
            return min(
                self.p.distinct_values(fldname),
                self.p.distinct_values(fldname2),
            );
        }

        self.p.distinct_values(fldname)
    }
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.p.schema())
    }

    fn repr(&self) -> Arc<dyn PlanRepr> {
        panic!("TODO")
    }
}

impl PlanRepr for SelectPlan {
    fn operation(&self) -> Operation {
        Operation::SelectScan
    }
    fn reads(&self) -> Option<i32> {
        panic!("TODO")
    }
    fn buffers(&self) -> Option<i32> {
        panic!("TODO")
    }
}

impl SelectPlan {
    pub fn new(p: Arc<dyn Plan>, pred: Predicate) -> Self {
        Self { p, pred }
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
        query::{constant::Constant, expression::Expression, term::Term, tests},
        server::simpledb::SimpleDB,
    };

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/selectplan").exists() {
            fs::remove_dir_all("_test/selectplan")?;
        }

        let simpledb = SimpleDB::new_with("_test/selectplan", 400, 8);

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

        let pred = Predicate::new(Term::new(
            Expression::Fldname("GradYear".to_string()),
            Expression::Val(Constant::I32(2020)),
        ));
        let plan = SelectPlan::new(srcplan, pred);

        println!("PLAN: {}", plan.dump());
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
