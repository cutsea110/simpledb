use core::fmt;

use anyhow::Result;

use crate::{query::constant::Constant, record::rid::RID};

pub mod btree;
pub mod hash;
pub mod plan;
pub mod query;

#[derive(Debug)]
pub enum IndexError {
    NoTableScan,
}

impl std::error::Error for IndexError {}
impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IndexError::NoTableScan => {
                write!(f, "no table scan")
            }
        }
    }
}

pub trait Index {
    fn before_first(&mut self, searchkey: Constant) -> Result<()>;
    fn next(&mut self) -> bool;
    fn get_data_rid(&mut self) -> Result<RID>;
    fn insert(&mut self, dataval: Constant, datarid: RID) -> Result<()>;
    fn delete(&mut self, dataval: Constant, datarid: RID) -> Result<()>;
    fn close(&mut self) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use std::{
        fs,
        path::Path,
        sync::{Arc, Mutex},
    };

    use crate::{
        plan::{plan::Plan, tableplan::TablePlan},
        query::{constant::Constant, scan::Scan, updatescan::UpdateScan},
        record::schema::Schema,
        server::simpledb::SimpleDB,
    };

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/index").exists() {
            fs::remove_dir_all("_test/index")?;
        }

        let db = SimpleDB::new("_test/index")?;
        let tx = Arc::new(Mutex::new(db.new_tx()?));
        let mdm = db.metadata_mgr().unwrap();

        // Create student table
        let mut sch = Schema::new();
        sch.add_i32_field("sid");
        sch.add_string_field("sname", 10);
        sch.add_i32_field("grad_year");
        sch.add_i32_field("major_id");
        mdm.lock()
            .unwrap()
            .create_table("student", Arc::new(sch), Arc::clone(&tx))?;

        // Create index for major_id on student
        mdm.lock()
            .unwrap()
            .create_index("idx_major_id", "student", "major_id", Arc::clone(&tx))?;

        // Open an scan on the data table
        let studentplan = TablePlan::new("student", Arc::clone(&tx), Arc::clone(&mdm))?;
        let studentscan = studentplan.open()?;

        // Open the index on MajorId
        let indexes = mdm
            .lock()
            .unwrap()
            .get_index_info("student", Arc::clone(&tx))?;
        let ii = indexes.get("major_id").unwrap();
        let idx = ii.open();

        // Initialize data
        let students = vec![
            (1, "joe", 2021, 10),
            (2, "amy", 2020, 20),
            (3, "max", 2022, 10),
            (4, "sue", 2022, 20),
            (5, "bob", 2020, 30),
            (6, "kim", 2020, 20),
            (7, "art", 2021, 30),
            (8, "pat", 2019, 20),
            (9, "lee", 2021, 10),
        ];
        if let Ok(ts) = studentscan.lock().unwrap().as_table_scan() {
            ts.before_first()?;

            for (sid, sname, grad_year, major_id) in students {
                ts.insert()?;
                ts.set_i32("sid", sid)?;
                ts.set_string("sname", sname.to_string())?;
                ts.set_i32("grad_year", grad_year)?;
                ts.set_i32("major_id", major_id)?;
                idx.lock()
                    .unwrap()
                    .insert(Constant::I32(major_id), ts.get_rid()?)?;
            }
        }

        // Retrieve all index records having a dataval of 20.
        idx.lock().unwrap().before_first(Constant::I32(20))?;
        while idx.lock().unwrap().next() {
            // Use the datarid to go to the corresponding STUDENT record.
            let datarid = idx.lock().unwrap().get_data_rid()?;
            studentscan
                .lock()
                .unwrap()
                .to_update_scan()?
                .move_to_rid(datarid)?;
            println!("{}", studentscan.lock().unwrap().get_string("sname")?);
        }

        // Close the index and the data table
        idx.lock().unwrap().close()?;
        studentscan.lock().unwrap().close()?;
        tx.lock().unwrap().commit()?;

        Ok(())
    }
}
