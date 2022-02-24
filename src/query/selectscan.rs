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
    fn to_update_scan(&mut self) -> Result<&mut dyn UpdateScan> {
        Err(From::from(SelectScanError::DowncastError))
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
}

impl SelectScan {
    pub fn new(s: Arc<Mutex<dyn Scan>>, pred: Predicate) -> Self {
        Self { s, pred }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use anyhow::Result;

    use crate::{
        metadata::manager::MetadataMgr,
        query::{expression::Expression, term::Term},
        record::{layout::Layout, schema::Schema, tablescan::TableScan},
        server::simpledb::SimpleDB,
        tx::transaction::Transaction,
    };

    use super::*;

    struct Student<'a> {
        s_id: i32,
        s_name: &'a str,
        grad_year: i32,
        major_id: i32,
    }
    impl<'a> Student<'a> {
        fn new(s_id: i32, s_name: &'a str, grad_year: i32, major_id: i32) -> Self {
            Self {
                s_id,
                s_name,
                grad_year,
                major_id,
            }
        }
    }

    struct Dept<'a> {
        d_id: i32,
        d_name: &'a str,
    }
    impl<'a> Dept<'a> {
        fn new(d_id: i32, d_name: &'a str) -> Self {
            Self { d_id, d_name }
        }
    }

    struct Course<'a> {
        c_id: i32,
        title: &'a str,
        dept_id: i32,
    }
    impl<'a> Course<'a> {
        fn new(c_id: i32, title: &'a str, dept_id: i32) -> Self {
            Self {
                c_id,
                title,
                dept_id,
            }
        }
    }

    struct Section<'a> {
        sect_id: i32,
        course_id: i32,
        prof: &'a str,
        year_offered: i32,
    }
    impl<'a> Section<'a> {
        fn new(sect_id: i32, course_id: i32, prof: &'a str, year_offered: i32) -> Self {
            Self {
                sect_id,
                course_id,
                prof,
                year_offered,
            }
        }
    }

    struct Enroll<'a> {
        e_id: i32,
        student_id: i32,
        section_id: i32,
        grade: &'a str,
    }
    impl<'a> Enroll<'a> {
        fn new(e_id: i32, student_id: i32, section_id: i32, grade: &'a str) -> Self {
            Self {
                e_id,
                student_id,
                section_id,
                grade,
            }
        }
    }

    fn init_sampledb(mdm: &mut MetadataMgr, tx: Arc<Mutex<Transaction>>) -> Result<()> {
        init_student(mdm, Arc::clone(&tx))?;
        init_dept(mdm, Arc::clone(&tx))?;
        init_course(mdm, Arc::clone(&tx))?;
        init_section(mdm, Arc::clone(&tx))?;
        init_enroll(mdm, Arc::clone(&tx))?;

        Ok(())
    }
    fn init_student(mdm: &mut MetadataMgr, tx: Arc<Mutex<Transaction>>) -> Result<()> {
        // Create STUDENT Table
        let mut sch = Schema::new();
        sch.add_i32_field("SId");
        sch.add_string_field("SName", 10);
        sch.add_i32_field("GradYear");
        sch.add_i32_field("MajorId");
        let asch = Arc::new(sch);
        mdm.create_table("STUDENT", Arc::clone(&asch), Arc::clone(&tx))?;
        // INSERT STUDENT Records
        let layout = Arc::new(Layout::new(Arc::clone(&asch)));
        let mut ts = TableScan::new(Arc::clone(&tx), "STUDENT", layout)?;
        let students = vec![
            Student::new(1, "joe", 2021, 10),
            Student::new(2, "amy", 2020, 20),
            Student::new(3, "max", 2022, 10),
            Student::new(4, "sue", 2022, 20),
            Student::new(5, "bob", 2020, 30),
            Student::new(6, "kim", 2020, 20),
            Student::new(7, "art", 2021, 30),
            Student::new(8, "pat", 2019, 20),
            Student::new(9, "lee", 2021, 10),
        ];
        ts.before_first()?;
        for s in students {
            ts.insert()?;
            ts.set_i32("SId", s.s_id)?;
            ts.set_string("SName", s.s_name.to_string())?;
            ts.set_i32("GradYear", s.grad_year)?;
            ts.set_i32("MajorId", s.major_id)?;
        }

        Ok(())
    }
    fn init_dept(mdm: &mut MetadataMgr, tx: Arc<Mutex<Transaction>>) -> Result<()> {
        // Create DEPT Table
        let mut sch = Schema::new();
        sch.add_i32_field("DId");
        sch.add_string_field("DName", 10);
        let asch = Arc::new(sch);
        mdm.create_table("DEPT", Arc::clone(&asch), Arc::clone(&tx))?;
        // INSERT DEPT Records
        let layout = Arc::new(Layout::new(Arc::clone(&asch)));
        let mut ts = TableScan::new(Arc::clone(&tx), "DEPT", layout)?;
        let depts = vec![
            Dept::new(10, "compsci"),
            Dept::new(20, "math"),
            Dept::new(30, "drama"),
        ];
        ts.before_first()?;
        for d in depts {
            ts.insert()?;
            ts.set_i32("DId", d.d_id)?;
            ts.set_string("DName", d.d_name.to_string())?;
        }

        Ok(())
    }
    fn init_course(mdm: &mut MetadataMgr, tx: Arc<Mutex<Transaction>>) -> Result<()> {
        // Create COURSE Table
        let mut sch = Schema::new();
        sch.add_i32_field("CId");
        sch.add_string_field("Title", 16);
        sch.add_i32_field("DeptId");
        let asch = Arc::new(sch);
        mdm.create_table("COURSE", Arc::clone(&asch), Arc::clone(&tx))?;
        // INSERT COURSE Records
        let layout = Arc::new(Layout::new(Arc::clone(&asch)));
        let mut ts = TableScan::new(Arc::clone(&tx), "COURSE", layout)?;
        let courses = vec![
            Course::new(12, "db systems", 10),
            Course::new(22, "compilers", 10),
            Course::new(32, "calculus", 20),
            Course::new(42, "algebra", 20),
            Course::new(52, "acting", 30),
            Course::new(62, "elocution", 30),
        ];
        ts.before_first()?;
        for c in courses {
            ts.insert()?;
            ts.set_i32("CId", c.c_id)?;
            ts.set_string("Title", c.title.to_string())?;
            ts.set_i32("DeptId", c.dept_id)?;
        }

        Ok(())
    }
    fn init_section(mdm: &mut MetadataMgr, tx: Arc<Mutex<Transaction>>) -> Result<()> {
        // Create SECTION Table
        let mut sch = Schema::new();
        sch.add_i32_field("SectId");
        sch.add_i32_field("CourseId");
        sch.add_string_field("Prof", 10);
        sch.add_i32_field("YearOffered");
        let asch = Arc::new(sch);
        mdm.create_table("SECTION", Arc::clone(&asch), Arc::clone(&tx))?;
        // INSERT SECTION Records
        let layout = Arc::new(Layout::new(Arc::clone(&asch)));
        let mut ts = TableScan::new(Arc::clone(&tx), "SECTION", layout)?;
        let sections = vec![
            Section::new(13, 12, "turing", 2018),
            Section::new(23, 12, "turing", 2016),
            Section::new(33, 32, "newton", 2017),
            Section::new(43, 32, "einstein", 2018),
            Section::new(53, 62, "brando", 2017),
        ];
        ts.before_first()?;
        for s in sections {
            ts.insert()?;
            ts.set_i32("SectId", s.sect_id)?;
            ts.set_i32("CourseId", s.course_id)?;
            ts.set_string("Prof", s.prof.to_string())?;
            ts.set_i32("YearOffered", s.year_offered)?;
        }

        Ok(())
    }
    fn init_enroll(mdm: &mut MetadataMgr, tx: Arc<Mutex<Transaction>>) -> Result<()> {
        // Create ENROLL Table
        let mut sch = Schema::new();
        sch.add_i32_field("EId");
        sch.add_i32_field("StudentId");
        sch.add_i32_field("SectionId");
        sch.add_string_field("Grade", 2);
        let asch = Arc::new(sch);
        mdm.create_table("ENROLL", Arc::clone(&asch), Arc::clone(&tx))?;
        // INSERT ENROLL Records
        let layout = Arc::new(Layout::new(Arc::clone(&asch)));
        let mut ts = TableScan::new(Arc::clone(&tx), "ENROLL", layout)?;
        let enrolls = vec![
            Enroll::new(14, 1, 13, "A"),
            Enroll::new(24, 1, 43, "C"),
            Enroll::new(34, 2, 43, "B+"),
            Enroll::new(44, 4, 33, "B"),
            Enroll::new(54, 4, 53, "A"),
            Enroll::new(64, 6, 53, "A"),
        ];
        ts.before_first()?;
        for e in enrolls {
            ts.insert()?;
            ts.set_i32("EId", e.e_id)?;
            ts.set_i32("StudentId", e.student_id)?;
            ts.set_i32("SectionId", e.section_id)?;
            ts.set_string("Grade", e.grade.to_string())?;
        }

        Ok(())
    }

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_selectscan").exists() {
            fs::remove_dir_all("_selectscan")?;
        }

        let simpledb = SimpleDB::new_with("_selectscan", 400, 8);

        let tx = Arc::new(Mutex::new(simpledb.new_tx()?));
        let mut mdm = MetadataMgr::new(true, Arc::clone(&tx))?;

        init_sampledb(&mut mdm, Arc::clone(&tx))?;

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
        println!("SELECT SName, GradYear FROM STUDENT WHERE GradYear = 2020 AND MajorId = 20");
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
