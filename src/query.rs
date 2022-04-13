pub mod constant;
pub mod expression;
pub mod predicate;
pub mod productscan;
pub mod projectscan;
pub mod scan;
pub mod selectscan;
pub mod term;
pub mod updatescan;

#[cfg(test)]
pub(crate) mod tests {
    use anyhow::Result;
    use std::sync::{Arc, Mutex};

    use crate::{
        metadata::manager::MetadataMgr,
        query::{constant::Constant, scan::Scan, updatescan::UpdateScan},
        record::{layout::Layout, schema::Schema, tablescan::TableScan},
        tx::transaction::Transaction,
    };

    pub(crate) struct Student<'a> {
        s_id: i32,
        s_name: &'a str,
        grad_year: i32,
        major_id: i32,
    }
    impl<'a> Student<'a> {
        pub(crate) fn new(s_id: i32, s_name: &'a str, grad_year: i32, major_id: i32) -> Self {
            Self {
                s_id,
                s_name,
                grad_year,
                major_id,
            }
        }
    }

    pub(crate) struct Dept<'a> {
        d_id: i32,
        d_name: &'a str,
    }
    impl<'a> Dept<'a> {
        pub fn new(d_id: i32, d_name: &'a str) -> Self {
            Self { d_id, d_name }
        }
    }

    pub(crate) struct Course<'a> {
        c_id: i32,
        title: &'a str,
        dept_id: i32,
    }
    impl<'a> Course<'a> {
        pub(crate) fn new(c_id: i32, title: &'a str, dept_id: i32) -> Self {
            Self {
                c_id,
                title,
                dept_id,
            }
        }
    }

    pub(crate) struct Section<'a> {
        sect_id: i32,
        course_id: i32,
        prof: &'a str,
        year_offered: i32,
    }
    impl<'a> Section<'a> {
        pub(crate) fn new(sect_id: i32, course_id: i32, prof: &'a str, year_offered: i32) -> Self {
            Self {
                sect_id,
                course_id,
                prof,
                year_offered,
            }
        }
    }

    pub(crate) struct Enroll<'a> {
        e_id: i32,
        student_id: i32,
        section_id: i32,
        grade: &'a str,
    }
    impl<'a> Enroll<'a> {
        pub(crate) fn new(e_id: i32, student_id: i32, section_id: i32, grade: &'a str) -> Self {
            Self {
                e_id,
                student_id,
                section_id,
                grade,
            }
        }
    }

    pub(crate) fn init_sampledb(mdm: &mut MetadataMgr, tx: Arc<Mutex<Transaction>>) -> Result<()> {
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
        // CREATE INDEX
        mdm.create_index("IDX_GradYear", "STUDENT", "GradYear", Arc::clone(&tx))?;
        mdm.create_index("IDX_MajorId", "STUDENT", "MajorId", Arc::clone(&tx))?;
        // Open the index on GradYear and MajorId
        let indexes = mdm.get_index_info("STUDENT", Arc::clone(&tx))?;
        let ii1 = indexes.get("GradYear").unwrap().clone();
        let idx1 = ii1.open();
        let ii2 = indexes.get("MajorId").unwrap().clone();
        let idx2 = ii2.open();

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
            idx1.lock()
                .unwrap()
                .insert(Constant::I32(s.grad_year), ts.get_rid()?)?;
            idx2.lock()
                .unwrap()
                .insert(Constant::I32(s.major_id), ts.get_rid()?)?;
        }
        tx.lock().unwrap().commit()?;

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
        tx.lock().unwrap().commit()?;

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
        tx.lock().unwrap().commit()?;

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
        tx.lock().unwrap().commit()?;

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
        tx.lock().unwrap().commit()?;

        Ok(())
    }
}
