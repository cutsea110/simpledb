use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::{predicate::Predicate, scan::Scan, updatescan::UpdateScan};
use crate::{query::constant::Constant, record::rid::RID};

#[derive(Debug)]
pub enum SelectScanError {
    DowncastError,
}

impl std::error::Error for SelectScanError {}
impl fmt::Display for SelectScanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SelectScanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

#[derive(Clone)]
pub struct SelectScan {
    s: Arc<Mutex<dyn Scan>>,
    pred: Predicate,
}

impl Scan for SelectScan {
    fn before_first(&mut self) -> Result<()> {
        self.s.lock().unwrap().before_first()
    }
    fn next(&mut self) -> bool {
        while self.s.lock().unwrap().next() {
            if self.pred.is_satisfied(Arc::clone(&self.s)) {
                return true;
            }
        }
        false
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        self.s.lock().unwrap().get_i32(fldname)
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        self.s.lock().unwrap().get_string(fldname)
    }
    fn get_val(&mut self, fldname: &str) -> Result<Constant> {
        self.s.lock().unwrap().get_val(fldname)
    }
    fn has_field(&self, fldname: &str) -> bool {
        self.s.lock().unwrap().has_field(fldname)
    }
    fn close(&mut self) -> Result<()> {
        self.s.lock().unwrap().close()
    }
    // downcast
    fn to_update_scan(&mut self) -> Result<&mut dyn UpdateScan> {
        Ok(self)
    }
}

impl UpdateScan for SelectScan {
    fn set_i32(&mut self, fldname: &str, val: i32) -> Result<()> {
        let mut us = self.s.lock().unwrap();
        us.to_update_scan()?.set_i32(fldname, val)
    }
    fn set_string(&mut self, fldname: &str, val: String) -> Result<()> {
        let mut us = self.s.lock().unwrap();
        us.to_update_scan()?.set_string(fldname, val)
    }
    fn set_val(&mut self, fldname: &str, val: Constant) -> Result<()> {
        let mut us = self.s.lock().unwrap();
        us.to_update_scan()?.set_val(fldname, val)
    }
    fn insert(&mut self) -> Result<()> {
        let mut us = self.s.lock().unwrap();
        us.to_update_scan()?.insert()
    }
    fn delete(&mut self) -> Result<()> {
        let mut us = self.s.lock().unwrap();
        us.to_update_scan()?.delete()
    }
    fn get_rid(&self) -> Result<RID> {
        let mut us = self.s.lock().unwrap();
        us.to_update_scan()?.get_rid()
    }
    fn move_to_rid(&mut self, rid: RID) -> Result<()> {
        let mut us = self.s.lock().unwrap();
        us.to_update_scan()?.move_to_rid(rid)
    }
    // upcast
    fn to_scan(&self) -> Result<Arc<Mutex<dyn Scan>>> {
        Ok(Arc::new(Mutex::new(self.clone())))
    }
}

impl SelectScan {
    pub fn new(s: Arc<Mutex<dyn Scan>>, pred: Predicate) -> Self {
        Self { s, pred }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use std::{fs, path::Path};

    use crate::{
        metadata::manager::MetadataMgr,
        query::{expression::Expression, term::Term},
        record::tablescan::TableScan,
        server::simpledb::SimpleDB,
    };

    use super::super::tests;
    use super::*;

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/selectscan").exists() {
            fs::remove_dir_all("_test/selectscan")?;
        }

        let simpledb = SimpleDB::new_with("_test/selectscan", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        let mut mdm = MetadataMgr::new(true, Arc::clone(&tx))?;

        tests::init_sampledb(&mut mdm, Arc::clone(&tx))?;

        // the STUDENT node
        let layout = mdm.get_layout("STUDENT", Arc::clone(&tx))?;
        let ts = TableScan::new(Arc::clone(&tx), "STUDENT", layout)?;

        // the Select node
        let lhs1 = Expression::new_fldname("GradYear".to_string());
        let c1 = Constant::new_i32(2020);
        let rhs1 = Expression::new_val(c1);
        let t1 = Term::new(lhs1, rhs1);
        let pred1 = Predicate::new(t1);
        let mut s1 = SelectScan::new(Arc::new(Mutex::new(ts)), pred1);
        println!("SELECT SName, GradYear FROM STUDENT WHERE GradYear = 2020");
        while s1.next() {
            println!("{} {}", s1.get_string("SName")?, s1.get_i32("GradYear")?);
        }

        // the another Select node
        let lhs2 = Expression::new_fldname("MajorId".to_string());
        let c2 = Constant::new_i32(20);
        let rhs2 = Expression::new_val(c2);
        let t2 = Term::new(lhs2, rhs2);
        let pred2 = Predicate::new(t2);
        let mut s2 = SelectScan::new(Arc::new(Mutex::new(s1)), pred2);
        println!(
            "SELECT SName, GradYear, MajorId \
               FROM (SELECT SId, SName, GradYear, MajorId FROM STUDENT WHERE GradYear = 2020) \
              WHERE MajorId = 20"
        );
        while s2.next() {
            println!(
                "{} {} {}",
                s2.get_string("SName")?,
                s2.get_i32("GradYear")?,
                s2.get_i32("MajorId")?
            );
        }

        tx.lock().unwrap().commit()?;

        Ok(())
    }
}
