use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::temptable::TempTable;
use crate::{
    plan::plan::Plan,
    query::scan::Scan,
    record::{layout::Layout, schema::Schema},
    repr::planrepr::{Operation, PlanRepr},
    tx::transaction::Transaction,
};

#[derive(Clone)]
pub struct MaterializePlan {
    // static member (shared by all Materializeplan and Temptable)
    next_table_num: Arc<Mutex<i32>>,

    srcplan: Arc<dyn Plan>,
    tx: Arc<Mutex<Transaction>>,
}

impl MaterializePlan {
    pub fn new(
        next_table_num: Arc<Mutex<i32>>,
        tx: Arc<Mutex<Transaction>>,
        srcplan: Arc<dyn Plan>,
    ) -> Self {
        Self {
            next_table_num,
            srcplan,
            tx,
        }
    }
}

impl Plan for MaterializePlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let sch = self.srcplan.schema();
        let mut temp = TempTable::new(
            Arc::clone(&self.next_table_num),
            Arc::clone(&self.tx),
            Arc::clone(&sch),
        );
        let src = self.srcplan.open()?;
        let dest = temp.open()?;
        while src.lock().unwrap().next() {
            dest.lock().unwrap().insert()?;
            for fldname in sch.fields() {
                dest.lock()
                    .unwrap()
                    .set_val(fldname, src.lock().unwrap().get_val(fldname)?)?;
            }
        }
        src.lock().unwrap().close()?;
        dest.lock().unwrap().before_first()?;

        let dest = dest.lock().unwrap().to_scan()?;
        Ok(dest)
    }
    fn blocks_accessed(&self) -> i32 {
        // create a dummy Layout object to calculate slot size
        let y = Layout::new(self.srcplan.schema());
        let rpb = (self.tx.lock().unwrap().block_size() / y.slot_size() as i32) as f32;
        (self.srcplan.records_output() as f32 / rpb).ceil() as i32
    }
    fn records_output(&self) -> i32 {
        self.srcplan.records_output()
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        self.srcplan.distinct_values(fldname)
    }
    fn schema(&self) -> Arc<Schema> {
        self.srcplan.schema()
    }

    fn repr(&self) -> Arc<dyn PlanRepr> {
        Arc::new(MaterializePlanRepr {
            p: self.srcplan.repr(),
            r: self.blocks_accessed(),
            w: self.records_output(),
        })
    }
}

#[derive(Clone)]
pub struct MaterializePlanRepr {
    p: Arc<dyn PlanRepr>,
    r: i32,
    w: i32,
}

impl PlanRepr for MaterializePlanRepr {
    fn operation(&self) -> Operation {
        Operation::Materialize
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
        if Path::new("_test/materializeplan").exists() {
            fs::remove_dir_all("_test/materializeplan")?;
        }

        let simpledb = SimpleDB::new_with("_test/materializeplan", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let mut mdm = MetadataMgr::new(true, Arc::clone(&tx))?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        tests::init_sampledb(&mut mdm, Arc::clone(&tx))?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let next_table_num = Arc::new(Mutex::new(0));
        let mdm = Arc::new(Mutex::new(mdm));
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let srcplan = Arc::new(TablePlan::new(
            "STUDENT",
            Arc::clone(&tx),
            Arc::clone(&mdm),
        )?);
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let plan = MaterializePlan::new(Arc::clone(&next_table_num), Arc::clone(&tx), srcplan);
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let scan = plan.open()?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 7);

        let mut iter = scan.lock().unwrap();
        while iter.next() {
            let name = iter.get_string("SName")?;
            let year = iter.get_i32("GradYear")?;
            println!("{:<10}{:>8}", name, year);
        }
        assert_eq!(tx.lock().unwrap().available_buffs(), 7);

        tx.lock().unwrap().commit()?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        Ok(())
    }
}
