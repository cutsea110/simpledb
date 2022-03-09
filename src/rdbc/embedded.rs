pub mod connection;
pub mod driver;
pub mod resultset;
pub mod resultsetmetadata;
pub mod statement;

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use anyhow::Result;

    use super::super::{
        connectionadapter::ConnectionAdapter,
        driveradapter::DriverAdapter,
        resultsetadapter::ResultSetAdapter,
        resultsetmetadataadapter::{DataType, ResultSetMetaDataAdapter},
        statementadapter::StatementAdapter,
    };
    use super::driver::EmbeddedDriver;

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/rdbc").exists() {
            fs::remove_dir_all("_test/rdbc")?;
        }

        let d = EmbeddedDriver::new();
        let conn = d.connect("_test/rdbc")?;

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
            let current_tx = conn.borrow_mut().get_transaction()?;
            println!("Tx {}", current_tx.lock().unwrap().tx_num());
            println!("Execute: {}", sql);

            let stmt = conn.borrow_mut().create(sql)?;
            stmt.borrow_mut().execute_update()?;
        }

        Ok(())
    }
}