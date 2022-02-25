use anyhow::Result;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::{constant::Constant, scan::Scan};

#[derive(Debug)]
pub enum ProductScanError {
    DowncastError,
    FieldNotFoundError(String),
}

impl std::error::Error for ProductScanError {}
impl fmt::Display for ProductScanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ProductScanError::DowncastError => {
                write!(f, "downcast error")
            }
            ProductScanError::FieldNotFoundError(fld) => {
                write!(f, "field({}) not found error", fld)
            }
        }
    }
}

pub struct ProductScan {
    s1: Arc<Mutex<dyn Scan>>,
    s2: Arc<Mutex<dyn Scan>>,
}

impl Scan for ProductScan {
    fn before_first(&mut self) -> Result<()> {
        let mut s1 = self.s1.lock().unwrap();
        let mut s2 = self.s2.lock().unwrap();
        s1.before_first()?;
        s1.next();
        s2.before_first()
    }
    fn next(&mut self) -> bool {
        let mut s2 = self.s2.lock().unwrap();
        if s2.next() {
            true
        } else {
            s2.before_first().unwrap();
            s2.next() && self.s1.lock().unwrap().next()
        }
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        let mut s1 = self.s1.lock().unwrap();
        if s1.has_field(fldname) {
            s1.get_i32(fldname)
        } else {
            self.s2.lock().unwrap().get_i32(fldname)
        }
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        let mut s1 = self.s1.lock().unwrap();
        if s1.has_field(fldname) {
            s1.get_string(fldname)
        } else {
            self.s2.lock().unwrap().get_string(fldname)
        }
    }
    fn get_val(&mut self, fldname: &str) -> Result<Constant> {
        let mut s1 = self.s1.lock().unwrap();
        if s1.has_field(fldname) {
            s1.get_val(fldname)
        } else {
            self.s2.lock().unwrap().get_val(fldname)
        }
    }
    fn has_field(&self, fldname: &str) -> bool {
        self.s1.lock().unwrap().has_field(fldname) || self.s2.lock().unwrap().has_field(fldname)
    }
    fn close(&mut self) -> anyhow::Result<()> {
        let mut s1 = self.s1.lock().unwrap();
        let mut s2 = self.s2.lock().unwrap();
        s1.close()?;
        s2.close()
    }
    fn to_update_scan(&mut self) -> anyhow::Result<&mut dyn super::updatescan::UpdateScan> {
        Err(From::from(ProductScanError::DowncastError))
    }
}

impl ProductScan {
    pub fn new(s1: Arc<Mutex<dyn Scan>>, s2: Arc<Mutex<dyn Scan>>) -> Self {
        s1.lock().unwrap().next();
        Self { s1, s2 }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use std::{fs, path::Path};

    use crate::query::expression::Expression;
    use crate::query::predicate::Predicate;
    use crate::query::selectscan::SelectScan;
    use crate::query::term::Term;
    use crate::{
        metadata::manager::MetadataMgr, record::tablescan::TableScan, server::simpledb::SimpleDB,
    };

    use super::super::tests;
    use super::*;

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_productscan").exists() {
            fs::remove_dir_all("_productscan")?;
        }

        let simpledb = SimpleDB::new_with("_productscan", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        let mut mdm = MetadataMgr::new(true, Arc::clone(&tx))?;

        tests::init_sampledb(&mut mdm, Arc::clone(&tx))?;

        // the STUDENT node
        let layout = mdm.get_layout("STUDENT", Arc::clone(&tx))?;
        let mut ts1 = TableScan::new(Arc::clone(&tx), "STUDENT", layout)?;
        println!("SELECT SId, SName, GradYear, MajorId FROM STUDENT");
        while ts1.next() {
            println!(
                "{} {} {} {}",
                ts1.get_i32("SId")?,
                ts1.get_string("SName")?,
                ts1.get_i32("GradYear")?,
                ts1.get_i32("MajorId")?,
            );
        }

        // the DEPT node
        let layout = mdm.get_layout("DEPT", Arc::clone(&tx))?;
        let mut ts2 = TableScan::new(Arc::clone(&tx), "DEPT", layout)?;
        println!("SELECT DId, DName FROM DEPT");
        while ts2.next() {
            println!("{} {}", ts2.get_i32("DId")?, ts2.get_string("DName")?);
        }

        // the Product node
        let ts3 = ProductScan::new(Arc::new(Mutex::new(ts1)), Arc::new(Mutex::new(ts2)));

        // the Select node
        let lhs1 = Expression::new_fldname("MajorId".to_string());
        let rhs1 = Expression::new_fldname("DId".to_string());
        let t1 = Term::new(lhs1, rhs1);

        let lhs2 = Expression::new_fldname("GradYear".to_string());
        let c2 = Constant::new_i32(2020);
        let rhs2 = Expression::new_val(c2);
        let t2 = Term::new(lhs2, rhs2);

        let mut pred1 = Predicate::new(t1);
        let mut pred2 = Predicate::new(t2);
        pred2.conjoin_with(&mut pred1);

        let mut ts4 = SelectScan::new(Arc::new(Mutex::new(ts3)), pred2);
        println!("SELECT SName, GradYear, DName FROM STUDENT, DEPT WHERE MajorId = DId AND GradYear = 2020");
        while ts4.next() {
            println!(
                "{} {} {}",
                ts4.get_string("SName")?,
                ts4.get_i32("GradYear")?,
                ts4.get_string("DName")?
            );
        }

        Ok(())
    }
}
