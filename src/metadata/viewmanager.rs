use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::tablemanager::{TableMgr, MAX_NAME};
use crate::{
    query::{scan::Scan, updatescan::UpdateScan},
    record::{schema::Schema, tablescan::TableScan},
    tx::transaction::Transaction,
};

pub const MAX_VIEWDEF: usize = 100; // max view def chars

#[derive(Debug, Clone)]
pub struct ViewMgr {
    tbl_mgr: TableMgr,
}

impl ViewMgr {
    pub fn new(is_new: bool, tbl_mgr: TableMgr, tx: Arc<Mutex<Transaction>>) -> Result<Self> {
        let mgr = Self { tbl_mgr };

        if is_new {
            let mut sch = Schema::new();
            sch.add_string_field("viewname", MAX_NAME);
            sch.add_string_field("viewdef", MAX_VIEWDEF);
            mgr.tbl_mgr.create_table("viewcat", Arc::new(sch), tx)?;
        }

        Ok(mgr)
    }
    pub fn create_view(&self, vname: &str, vdef: &str, tx: Arc<Mutex<Transaction>>) -> Result<()> {
        let layout = self.tbl_mgr.get_layout("viewcat", Arc::clone(&tx))?;
        let mut ts = TableScan::new(tx, "viewcat", layout)?;
        ts.insert()?;
        ts.set_string("viewname", vname.to_string())?;
        ts.set_string("viewdef", vdef.to_string())?;
        ts.close()?;

        Ok(())
    }
    pub fn get_view_def(&self, vname: &str, tx: Arc<Mutex<Transaction>>) -> Result<String> {
        let mut result = "".to_string();

        let layout = self.tbl_mgr.get_layout("viewcat", Arc::clone(&tx))?;
        let mut ts = TableScan::new(tx, "viewcat", layout)?;
        while ts.next() {
            if ts.get_string("viewname")? == vname {
                result = ts.get_string("viewdef")?;
                break;
            }
        }
        ts.close()?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use std::{fs, path::Path};

    use super::*;
    use crate::server::simpledb::SimpleDB;

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/viewmgrtest").exists() {
            fs::remove_dir_all("_test/viewmgrtest")?;
        }

        let simpledb = SimpleDB::new_with("_test/viewmgrtest", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        let tm = TableMgr::new(true, Arc::clone(&tx))?;
        let vm = ViewMgr::new(true, tm, Arc::clone(&tx))?;

        let viewdef = "select B from MyTable where A = 1";
        vm.create_view("viewA", viewdef, Arc::clone(&tx))?;

        let def = vm.get_view_def("viewA", Arc::clone(&tx))?;
        println!("viewA: {}", def);

        Ok(())
    }
}
