use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::plan::Plan;
use crate::{
    metadata::{manager::MetadataMgr, statmanager::StatInfo},
    query::scan::Scan,
    record::{layout::Layout, schema::Schema, tablescan::TableScan},
    repr::planrepr::{Operation, PlanRepr},
    tx::transaction::Transaction,
};

#[derive(Debug, Clone)]
pub struct TablePlan {
    tx: Arc<Mutex<Transaction>>,
    tblname: String,
    layout: Arc<Layout>,
    si: StatInfo,
}

impl Plan for TablePlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        let scan = TableScan::new(
            Arc::clone(&self.tx),
            &self.tblname,
            Arc::clone(&self.layout),
        )?;

        Ok(Arc::new(Mutex::new(scan)))
    }
    fn blocks_accessed(&self) -> i32 {
        self.si.blocks_accessed()
    }
    fn records_output(&self) -> i32 {
        self.si.records_output()
    }
    fn distinct_values(&self, fldname: &str) -> i32 {
        self.si.distinct_values(fldname)
    }
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.layout.schema())
    }

    fn repr(&self) -> Arc<dyn PlanRepr> {
        panic!("TODO")
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct TablePlanRepr {
    // TODO
}

impl PlanRepr for TablePlanRepr {
    fn operation(&self) -> Operation {
        Operation::TableScan
    }
    fn reads(&self) -> Option<i32> {
        panic!("TODO")
    }
    fn buffers(&self) -> Option<i32> {
        panic!("TODO")
    }
}

impl TablePlan {
    pub fn new(
        tblname: &str,
        tx: Arc<Mutex<Transaction>>,
        md: Arc<Mutex<MetadataMgr>>,
    ) -> Result<Self> {
        let mut mdm = md.lock().unwrap();
        let layout = mdm.get_layout(tblname, Arc::clone(&tx))?;
        let si = mdm.get_stat_info(tblname, Arc::clone(&layout), Arc::clone(&tx))?;

        Ok(Self {
            tx,
            tblname: tblname.to_string(),
            layout,
            si,
        })
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
        if Path::new("_test/tableplan").exists() {
            fs::remove_dir_all("_test/tableplan")?;
        }

        let simpledb = SimpleDB::new_with("_test/tableplan", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let mut mdm = MetadataMgr::new(true, Arc::clone(&tx))?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        tests::init_sampledb(&mut mdm, Arc::clone(&tx))?;
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let mdm = Arc::new(Mutex::new(mdm));
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

        let plan = Arc::new(TablePlan::new(
            "STUDENT",
            Arc::clone(&tx),
            Arc::clone(&mdm),
        )?);
        assert_eq!(tx.lock().unwrap().available_buffs(), 8);

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
