use anyhow::Result;
use std::sync::{Arc, Mutex};

use crate::{
    query::updatescan::UpdateScan,
    record::{layout::Layout, schema::Schema, tablescan::TableScan},
    tx::transaction::Transaction,
};

#[derive(Debug, Clone)]
pub struct TempTable {
    // static member (shared by all Materializeplan and Temptable)
    next_table_num: Arc<Mutex<i32>>,

    tx: Arc<Mutex<Transaction>>,
    tblname: String,
    layout: Arc<Layout>,
}

impl TempTable {
    pub fn new(
        next_table_num: Arc<Mutex<i32>>,
        tx: Arc<Mutex<Transaction>>,
        sch: Arc<Schema>,
    ) -> Self {
        let mut tt = Self {
            next_table_num,
            tx,
            tblname: "".to_string(), // dummy
            layout: Arc::new(Layout::new(sch)),
        };
        tt.tblname = tt.next_table_name();

        tt
    }
    pub fn open(&mut self) -> Result<Arc<Mutex<dyn UpdateScan>>> {
        let ts = TableScan::new(
            Arc::clone(&self.tx),
            &self.tblname,
            Arc::clone(&self.layout),
        )?;

        Ok(Arc::new(Mutex::new(ts)))
    }
    pub fn table_name(&self) -> &str {
        &self.tblname
    }
    pub fn get_layout(&self) -> Arc<Layout> {
        Arc::clone(&self.layout)
    }
    // synchronized
    fn next_table_name(&mut self) -> String {
        let mut next_table_num = self.next_table_num.lock().unwrap();
        *next_table_num += 1;

        format!("temp{}", *next_table_num) // if you change the name, you must change FileMgr, too.
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
        if Path::new("_test/temptable").exists() {
            fs::remove_dir_all("_test/temptable")?;
        }

        let simpledb = SimpleDB::new_with("_test/temptable", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        let mut mdm = MetadataMgr::new(true, Arc::clone(&tx))?;

        tests::init_sampledb(&mut mdm, Arc::clone(&tx))?;

        let next_table_num = Arc::new(Mutex::new(0));
        let mdm = Arc::new(Mutex::new(mdm));

        let plan = TablePlan::new("STUDENT", Arc::clone(&tx), Arc::clone(&mdm))?;

        let tt = TempTable::new(Arc::clone(&next_table_num), Arc::clone(&tx), plan.schema());
        assert_eq!("temp1", tt.table_name());

        let mut tt = TempTable::new(Arc::clone(&next_table_num), Arc::clone(&tx), plan.schema());
        assert_eq!("temp2", tt.table_name());
        let src = plan.open()?;
        let dest = tt.open()?;
        while src.lock().unwrap().next() {
            dest.lock().unwrap().insert()?;
            for fldname in plan.schema().fields() {
                dest.lock()
                    .unwrap()
                    .set_val(fldname, src.lock().unwrap().get_val(fldname)?)?;
            }
        }
        src.lock().unwrap().close()?;
        dest.lock().unwrap().before_first()?;

        let mut iter = dest.lock().unwrap();
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
