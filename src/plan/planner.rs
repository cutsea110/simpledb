use anyhow::Result;
use combine::Parser;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::{plan::Plan, queryplanner::QueryPlanner, updateplanner::UpdatePlanner};
use crate::{
    parser::parser::{query, update_cmd},
    parser::{ddl::DDL, dml::DML, sql::SQL},
    tx::transaction::Transaction,
};

#[derive(Debug)]
pub enum PlannerError {
    InvalidExecuteCommand,
}

impl std::error::Error for PlannerError {}
impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlannerError::InvalidExecuteCommand => {
                write!(f, "invalid execute command")
            }
        }
    }
}

#[derive(Clone)]
pub struct Planner {
    qplanner: Arc<Mutex<dyn QueryPlanner>>,
    uplanner: Arc<Mutex<dyn UpdatePlanner>>,
}

impl Planner {
    pub fn new(
        qplanner: Arc<Mutex<dyn QueryPlanner>>,
        uplanner: Arc<Mutex<dyn UpdatePlanner>>,
    ) -> Self {
        Self { qplanner, uplanner }
    }
    pub fn create_query_plan(
        &mut self,
        cmd: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Arc<dyn Plan>> {
        let mut parser = query();
        let (data, _) = parser.parse(cmd)?;
        // TODO: code to verify the query should be here...
        self.qplanner.lock().unwrap().create_plan(data, tx)
    }
    pub fn execute_update(&mut self, cmd: &str, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        let mut parser = update_cmd();
        let (data, _) = parser.parse(cmd)?;
        match data {
            SQL::DML(dml) => match dml {
                DML::Insert(idata) => {
                    let planner = self.uplanner.lock().unwrap();
                    return planner.execute_insert(idata, tx);
                }
                DML::Delete(ddata) => {
                    let planner = self.uplanner.lock().unwrap();
                    return planner.execute_delete(ddata, tx);
                }
                DML::Modify(mdata) => {
                    let planner = self.uplanner.lock().unwrap();
                    return planner.execute_modify(mdata, tx);
                }
                _ => return Err(From::from(PlannerError::InvalidExecuteCommand)),
            },
            SQL::DDL(ddl) => match ddl {
                DDL::Table(ctdata) => {
                    let p = self.uplanner.lock().unwrap();
                    return p.execute_create_table(ctdata, tx);
                }
                DDL::View(cvdata) => {
                    let p = self.uplanner.lock().unwrap();
                    return p.execute_create_view(cvdata, tx);
                }
                DDL::Index(cidata) => {
                    let p = self.uplanner.lock().unwrap();
                    return p.execute_create_index(cidata, tx);
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use std::sync::{Arc, Mutex};
    use std::{fs, path::Path};

    use crate::{
        metadata::manager::MetadataMgr,
        plan::{basicqueryplanner::BasicQueryPlanner, basicupdateplanner::BasicUpdatePlanner},
        server::simpledb::SimpleDB,
    };

    use super::*;

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/planner").exists() {
            fs::remove_dir_all("_test/planner")?;
        }

        let simpledb = SimpleDB::new_with("_test/planner", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        let mdm = Arc::new(Mutex::new(MetadataMgr::new(true, Arc::clone(&tx))?));

        let qp = Arc::new(Mutex::new(BasicQueryPlanner::new(Arc::clone(&mdm))));
        let up = Arc::new(Mutex::new(BasicUpdatePlanner::new(Arc::clone(&mdm))));
        let mut planner = Planner::new(qp, up);

        // Setting Schema and Insert Init data
        let sqls = vec![
            // DDL
            "CREATE TABLE STUDENT (SId int32, SName varchar(10), GradYear int32, MajorId int32)",
            "CREATE TABLE DEPT (DId int32, DName varchar(10))",
            "CREATE TABLE COURSE (CId int32, Title varchar(16), DeptId int32)",
            "CREATE TABLE SECTION (SectId int32, CourseId int32, Prof varchar(10), YearOffered int32)",
            "CREATE TABLE ENROLL (EId int32, StudentId int32, SectionId int32, Grade varchar(2))",
	    "CREATE VIEW name_dep AS SELECT SName, DName, GradYear, MajorId FROM STUDENT, DEPT WHERE MajorId = DId",
            "CREATE INDEX idx_grad_year ON STUDENT (GradYear)",
            // STUDENT
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (1, 'joe', 2021, 10)",
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (2, 'amy', 2020, 20)",
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (3, 'max', 2022, 10)",
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (4, 'sue', 2022, 20)",
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (5, 'bob', 2020, 30)",
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (6, 'kim', 2020, 20)",
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (7, 'art', 2021, 30)",
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (8, 'pat', 2019, 20)",
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (9, 'lee', 2021, 10)",
            // DEPT
            "INSERT INTO DEPT (DId, DName) VALUES (10, 'compsci')",
            "INSERT INTO DEPT (DId, DName) VALUES (20, 'math')",
            "INSERT INTO DEPT (DId, DName) VALUES (30, 'drama')",
            // COURSE
            "INSERT INTO COURSE (CId, Title, DeptId) VALUES (12, 'db systems', 10)",
            "INSERT INTO COURSE (CId, Title, DeptId) VALUES (22, 'compilers', 10)",
            "INSERT INTO COURSE (CId, Title, DeptId) VALUES (32, 'calculus', 20)",
            "INSERT INTO COURSE (CId, Title, DeptId) VALUES (42, 'algebra', 20)",
            "INSERT INTO COURSE (CId, Title, DeptId) VALUES (52, 'acting', 30)",
            "INSERT INTO COURSE (CId, Title, DeptId) VALUES (62, 'elocution', 30)",
            // SECTION
            "INSERT INTO SECTION (SectId, CourseId, Prof, YearOffered) VALUES (13, 12, 'turing', 2018)",
            "INSERT INTO SECTION (SectId, CourseId, Prof, YearOffered) VALUES (23, 12, 'turing', 2016)",
            "INSERT INTO SECTION (SectId, CourseId, Prof, YearOffered) VALUES (33, 32, 'newton', 2017)",
            "INSERT INTO SECTION (SectId, CourseId, Prof, YearOffered) VALUES (43, 32, 'einstein', 2018)",
            "INSERT INTO SECTION (SectId, CourseId, Prof, YearOffered) VALUES (53, 62, 'brando', 2017)",
	    // ENROLL
            "INSERT INTO ENROLL (EId, StudentId, SectionId, Grade) VALUES (14, 1, 13, 'A')",
            "INSERT INTO ENROLL (EId, StudentId, SectionId, Grade) VALUES (24, 1, 43, 'C')",
            "INSERT INTO ENROLL (EId, StudentId, SectionId, Grade) VALUES (34, 2, 43, 'B+')",
            "INSERT INTO ENROLL (EId, StudentId, SectionId, Grade) VALUES (44, 4, 33, 'B')",
            "INSERT INTO ENROLL (EId, StudentId, SectionId, Grade) VALUES (54, 4, 53, 'A')",
            "INSERT INTO ENROLL (EId, StudentId, SectionId, Grade) VALUES (64, 6, 53, 'A')",
        ];
        for sql in sqls {
            print!("Execute: {} ... ", sql);
            planner.execute_update(sql, Arc::clone(&tx))?;
            println!("Done");
        }

        // SELECT Table
        let query = "SELECT SName, DName, GradYear FROM STUDENT, DEPT WHERE MajorId = DId";
        println!("Query: {}", query);
        let plan = planner.create_query_plan(query, Arc::clone(&tx))?;
        let scan = plan.open()?;
        let mut rows = 0;
        let mut iter = scan.lock().unwrap();
        println!("SName     DName     GradYear");
        println!("----------------------------");
        while iter.next() {
            rows += 1;
            let name = iter.get_string("SName")?;
            let dep = iter.get_string("DName")?;
            let year = iter.get_i32("GradYear")?;
            println!("{:<10}{:<10}{:>8}", name, dep, year);
        }
        println!("Rows = {}", rows);

        // SELECT View
        let query = "SELECT SName, DName FROM name_dep WHERE GradYear = 2020";
        println!("Query: {}", query);
        let plan = planner.create_query_plan(query, Arc::clone(&tx))?;
        let scan = plan.open()?;
        let mut rows = 0;
        let mut iter = scan.lock().unwrap();
        println!("SName     DName");
        println!("-------------------");
        while iter.next() {
            rows += 1;
            let name = iter.get_string("SName")?;
            let dep = iter.get_string("DName")?;
            println!("{:<10}{:<10}", name, dep);
        }
        println!("Rows = {}", rows);

        // SELECT View + Table
        let query = "SELECT SName, DName, Title FROM name_dep, COURSE WHERE MajorId = DeptId";
        println!("Query: {}", query);
        let plan = planner.create_query_plan(query, Arc::clone(&tx))?;
        let scan = plan.open()?;
        let mut rows = 0;
        let mut iter = scan.lock().unwrap();
        println!("SName     DName    Title");
        println!("-----------------------------------");
        while iter.next() {
            rows += 1;
            let name = iter.get_string("SName")?;
            let dep = iter.get_string("DName")?;
            let title = iter.get_string("Title")?;
            println!("{:<10}{:<10}{:<16}", name, dep, title);
        }
        println!("Rows = {}", rows);

        // UPDATE
        let update = "UPDATE STUDENT SET MajorId = 30 WHERE GradYear = 2020";
        print!("Execute: {} ... ", update);
        let c = planner.execute_update(update, Arc::clone(&tx))?;
        println!("Affected rows = {}", c);
        // SELECT After UPDATE
        let query = "SELECT SName, DName, GradYear FROM STUDENT, DEPT WHERE MajorId = DId";
        println!("Query: {}", query);
        let plan = planner.create_query_plan(query, Arc::clone(&tx))?;
        let scan = plan.open()?;
        let mut rows = 0;
        let mut iter = scan.lock().unwrap();
        println!("SName     DName     GradYear");
        println!("----------------------------");
        while iter.next() {
            rows += 1;
            let name = iter.get_string("SName")?;
            let dep = iter.get_string("DName")?;
            let year = iter.get_i32("GradYear")?;
            println!("{:<10}{:<10}{:>8}", name, dep, year);
        }
        println!("Rows = {}", rows);

        // DELETE
        let update = "DELETE FROM STUDENT WHERE MajorId = 30";
        print!("Execute: {} ... ", update);
        let c = planner.execute_update(update, Arc::clone(&tx))?;
        println!("Affected rows = {}", c);
        // SELECT After DELETE
        let query = "SELECT SName, DName, GradYear FROM STUDENT, DEPT WHERE MajorId = DId";
        println!("Query: {}", query);
        let plan = planner.create_query_plan(query, Arc::clone(&tx))?;
        let scan = plan.open()?;
        let mut rows = 0;
        let mut iter = scan.lock().unwrap();
        println!("SName     DName     GradYear");
        println!("----------------------------");
        while iter.next() {
            rows += 1;
            let name = iter.get_string("SName")?;
            let dep = iter.get_string("DName")?;
            let year = iter.get_i32("GradYear")?;
            println!("{:<10}{:<10}{:>8}", name, dep, year);
        }
        println!("Rows = {}", rows);

        // important
        tx.lock().unwrap().commit()?;
        // tx.lock().unwrap().rollback()?;

        Ok(())
    }
}
