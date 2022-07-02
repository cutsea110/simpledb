#!/bin/sh

set -xu

LOG_DIR=./logs
mkdir -p ${LOG_DIR}

for bm in naive naivebis fifo lru clock
do
    for qp in basic heuristic
    do
	# clear db
	rm -rf data
	
	RUST_LOG=trace \
		cargo run --bin esql -- \
		-d demo \
		--buffer-manager ${bm} \
		--query-planner ${qp} \
		2> ${LOG_DIR}/${bm}_${qp}_init.log <<EOM
yes

CREATE TABLE student (sid integer, sname varchar(10), grad_year smallint, major_id integer, birth date, sex bool);
CREATE TABLE dept (did integer, dname varchar(10));
CREATE TABLE course (cid integer, title varchar(16), dept_id integer);
CREATE TABLE section (sect_id integer, course_id integer, prof varchar(10), year_offered integer);
CREATE TABLE enroll (eid integer, student_id integer, section_id integer, grade varchar(2));
CREATE INDEX idx_grad_year ON student (grad_year);
CREATE INDEX idx_major_id ON student (major_id);
CREATE INDEX idx_sex ON student (sex);

CREATE TABLE sex (sex_name varchar(10), value bool);
INSERT INTO sex (sex_name, value) VALUES ('male', true);
INSERT INTO sex (sex_name, value) VALUES ('female', false);


INSERT INTO student (sid, sname, grad_year, major_id, birth, sex) VALUES (1, 'joe', 2021, 10, '2002-06-22', true);
INSERT INTO student (sid, sname, grad_year, major_id, birth, sex) VALUES (2, 'amy', 2020, 20, '2001-09-13', false);
INSERT INTO student (sid, sname, grad_year, major_id, birth, sex) VALUES (3, 'max', 2022, 10, '2000-11-09', true);
INSERT INTO student (sid, sname, grad_year, major_id, birth, sex) VALUES (4, 'sue', 2022, 20, '2000-03-01', false);
INSERT INTO student (sid, sname, grad_year, major_id, birth, sex) VALUES (5, 'bob', 2020, 30, '2001-04-19', true);
INSERT INTO student (sid, sname, grad_year, major_id, birth, sex) VALUES (6, 'kim', 2020, 20, '2001-08-31', false);
INSERT INTO student (sid, sname, grad_year, major_id, birth, sex) VALUES (7, 'art', 2021, 30, '2002-10-10', true);
INSERT INTO student (sid, sname, grad_year, major_id, birth, sex) VALUES (8, 'pat', 2019, 20, '2000-07-23', false);
INSERT INTO student (sid, sname, grad_year, major_id, birth, sex) VALUES (9, 'lee', 2021, 10, '2002-12-29', true);

INSERT INTO dept (did, dname) VALUES (10, 'compsci');
INSERT INTO dept (did, dname) VALUES (20, 'math');
INSERT INTO dept (did, dname) VALUES (30, 'drama');

INSERT INTO course (cid, title, dept_id) VALUES (12, 'db systems', 10);
INSERT INTO course (cid, title, dept_id) VALUES (22, 'compilers', 10);
INSERT INTO course (cid, title, dept_id) VALUES (32, 'calculus', 20);
INSERT INTO course (cid, title, dept_id) VALUES (42, 'algebra', 20);
INSERT INTO course (cid, title, dept_id) VALUES (52, 'acting', 30);
INSERT INTO course (cid, title, dept_id) VALUES (62, 'elocution', 30);

INSERT INTO section (sect_id, course_id, prof, year_offered) VALUES (13, 12, 'turing', 2018);
INSERT INTO section (sect_id, course_id, prof, year_offered) VALUES (23, 12, 'turing', 2016);
INSERT INTO section (sect_id, course_id, prof, year_offered) VALUES (33, 32, 'newton', 2017);
INSERT INTO section (sect_id, course_id, prof, year_offered) VALUES (43, 32, 'einstein', 2018);
INSERT INTO section (sect_id, course_id, prof, year_offered) VALUES (53, 62, 'brando', 2017);

INSERT INTO enroll (eid, student_id, section_id, grade) VALUES (14, 1, 13, 'A');
INSERT INTO enroll (eid, student_id, section_id, grade) VALUES (24, 1, 43, 'C');
INSERT INTO enroll (eid, student_id, section_id, grade) VALUES (34, 2, 43, 'B+');
INSERT INTO enroll (eid, student_id, section_id, grade) VALUES (44, 4, 33, 'B');
INSERT INTO enroll (eid, student_id, section_id, grade) VALUES (54, 4, 53, 'A');
INSERT INTO enroll (eid, student_id, section_id, grade) VALUES (64, 6, 53, 'A');

CREATE VIEW einstein AS SELECT sect_id FROM section WHERE prof = 'einstein';

:q
EOM
	RUST_LOG=trace \
		cargo run --bin esql -- \
		-d demo \
		--buffer-manager ${bm} \
		--query-planner ${qp} \
		2> ${LOG_DIR}/${bm}_${qp}_query.log <<EOM
SELECT sid, sname, dname, grad_year, birth FROM student, dept WHERE did = major_id;
SELECT tblname, fldname, type, length FROM fldcat WHERE tblname = 'student';
SELECT sname, grade, prof FROM student, section, enroll WHERE sid = student_id AND sect_id = section_id;
:q
EOM
    done
done
