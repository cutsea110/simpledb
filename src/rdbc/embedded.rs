pub mod connection;
pub mod driver;
pub mod metadata;
pub mod planrepr;
pub mod resultset;
pub mod statement;

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use std::{fs, path::Path};

    use super::{
        super::{
            super::server::config::{BufferMgr, QueryPlanner, SimpleDBConfig},
            connectionadapter::ConnectionAdapter,
            driveradapter::DriverAdapter,
            resultsetadapter::ResultSetAdapter,
            resultsetmetadataadapter::{DataType, ResultSetMetaDataAdapter},
            statementadapter::StatementAdapter,
        },
        driver::EmbeddedDriver,
    };

    #[test]
    fn unit_test() -> Result<()> {
        if Path::new("_test/rdbc").exists() {
            fs::remove_dir_all("_test/rdbc")?;
        }

        // Setting Schema and Insert Init data
        let sqls = vec![
            // DDL
            "CREATE TABLE STUDENT (SId integer, SName varchar(10), GradYear integer, MajorId integer);",
            "CREATE TABLE DEPT (DId integer, DName varchar(10));",
            "CREATE TABLE COURSE (CId integer, Title varchar(16), DeptId integer);",
            "CREATE TABLE SECTION (SectId integer, CourseId integer, Prof varchar(10), YearOffered integer);",
            "CREATE TABLE ENROLL (EId integer, StudentId integer, SectionId integer, Grade varchar(2));",
	    "CREATE VIEW name_dep AS SELECT SName, DName, GradYear, MajorId FROM STUDENT, DEPT WHERE MajorId = DId",
            "CREATE INDEX idx_grad_year ON STUDENT (GradYear);",
            // STUDENT
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (1, 'joe', 2021, 10);",
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (2, 'amy', 2020, 20);",
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (3, 'max', 2022, 10);",
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (4, 'sue', 2022, 20);",
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (5, 'bob', 2020, 30);",
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (6, 'kim', 2020, 20);",
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (7, 'art', 2021, 30);",
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (8, 'pat', 2019, 20);",
            "INSERT INTO STUDENT (SId, SName, GradYear, MajorId) VALUES (9, 'lee', 2021, 10);",
            // DEPT
            "INSERT INTO DEPT (DId, DName) VALUES (10, 'compsci');",
            "INSERT INTO DEPT (DId, DName) VALUES (20, 'math');",
            "INSERT INTO DEPT (DId, DName) VALUES (30, 'drama');",
            // COURSE
            "INSERT INTO COURSE (CId, Title, DeptId) VALUES (12, 'db systems', 10);",
            "INSERT INTO COURSE (CId, Title, DeptId) VALUES (22, 'compilers', 10);",
            "INSERT INTO COURSE (CId, Title, DeptId) VALUES (32, 'calculus', 20);",
            "INSERT INTO COURSE (CId, Title, DeptId) VALUES (42, 'algebra', 20);",
            "INSERT INTO COURSE (CId, Title, DeptId) VALUES (52, 'acting', 30);",
            "INSERT INTO COURSE (CId, Title, DeptId) VALUES (62, 'elocution', 30);",
            // SECTION
            "INSERT INTO SECTION (SectId, CourseId, Prof, YearOffered) VALUES (13, 12, 'turing', 2018);",
            "INSERT INTO SECTION (SectId, CourseId, Prof, YearOffered) VALUES (23, 12, 'turing', 2016);",
            "INSERT INTO SECTION (SectId, CourseId, Prof, YearOffered) VALUES (33, 32, 'newton', 2017);",
            "INSERT INTO SECTION (SectId, CourseId, Prof, YearOffered) VALUES (43, 32, 'einstein', 2018);",
            "INSERT INTO SECTION (SectId, CourseId, Prof, YearOffered) VALUES (53, 62, 'brando', 2017);",
	    // ENROLL
            "INSERT INTO ENROLL (EId, StudentId, SectionId, Grade) VALUES (14, 1, 13, 'A');",
            "INSERT INTO ENROLL (EId, StudentId, SectionId, Grade) VALUES (24, 1, 43, 'C');",
            "INSERT INTO ENROLL (EId, StudentId, SectionId, Grade) VALUES (34, 2, 43, 'B+');",
            "INSERT INTO ENROLL (EId, StudentId, SectionId, Grade) VALUES (44, 4, 33, 'B');",
            "INSERT INTO ENROLL (EId, StudentId, SectionId, Grade) VALUES (54, 4, 53, 'A');",
            "INSERT INTO ENROLL (EId, StudentId, SectionId, Grade) VALUES (64, 6, 53, 'A');",
        ];

        // driver
        let d = EmbeddedDriver::new(SimpleDBConfig {
            block_size: 400,
            num_of_buffers: 8,
            buffer_manager: BufferMgr::Naive,
            query_planner: QueryPlanner::Basic,
        });
        // connect database
        let mut conn = d.connect("_test/rdbc")?;
        // init database
        for sql in sqls {
            println!("< {}", sql);
            if let Ok(n) = conn.create_statement(sql)?.execute_update() {
                println!("> Affected {}", n);
            }
        }
        // close connection
        conn.close()?;

        // new connect
        let mut conn = d.connect("_test/rdbc")?;
        let qry = "select SId, SName, DId, DName, GradYear from STUDENT, DEPT where MajorId = DId;";
        println!("> {}", qry);
        // statement
        let mut stmt = conn.create_statement(qry)?;
        // resultset
        let mut results = stmt.execute_query()?;
        // resultset metadata
        let meta = results.get_meta_data()?;

        // print header
        for i in 0..meta.get_column_count() {
            let name = meta.get_column_name(i).unwrap();
            let w = meta.get_column_display_size(i).unwrap();
            print!("{:width$} ", name, width = w);
        }
        println!();
        // separater
        for i in 0..meta.get_column_count() {
            let w = meta.get_column_display_size(i).unwrap();
            print!("{:-<width$}", "", width = w + 1);
        }
        println!();

        // scan record
        let mut c = 0;
        while results.next() {
            c += 1;
            for i in 0..meta.get_column_count() {
                let fldname = meta.get_column_name(i).unwrap();
                let w = meta.get_column_display_size(i).unwrap();
                match meta.get_column_type(i).unwrap() {
                    DataType::Int16 => {
                        print!("{:width$} ", results.get_i16(fldname)?, width = w);
                    }
                    DataType::Int32 => {
                        print!("{:width$} ", results.get_i32(fldname)?, width = w);
                    }
                    DataType::Varchar => {
                        print!("{:width$} ", results.get_string(fldname)?, width = w);
                    }
                    DataType::Bool => {
                        print!("{:width$} ", results.get_bool(fldname)?, width = w);
                    }
                    DataType::Date => {
                        print!("{:width$} ", results.get_date(fldname)?, width = w);
                    }
                }
            }
            println!();
        }
        println!("({} Rows)", c);

        // update
        let cmd = "update STUDENT set GradYear = 2023 where MajorId = 30;";
        println!("> {}", cmd);
        // statement
        let mut stmt = conn.create_statement(cmd)?;
        // affected
        let affected = stmt.execute_update()?;
        println!("> Affected {}", affected);

        // close connection
        conn.close()?;

        Ok(())
    }
}
