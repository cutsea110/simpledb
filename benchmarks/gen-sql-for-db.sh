#!/bin/sh

#
# This program requires shuf command
#
RAND() {
    shuf -i $1-$2 | head -n $3
}

DBSIZE=${1:-tiny}

cat <<EOM
CREATE TABLE student (sid integer, sname varchar(10), grad_year smallint, major_id integer, birth date, sex bool);
CREATE TABLE dept (did integer, dname varchar(10));
CREATE TABLE course (cid integer, title varchar(16), dept_id integer);
CREATE TABLE section (sect_id integer, course_id integer, prof varchar(10), year_offered integer);
CREATE TABLE enroll (eid integer, student_id integer, section_id integer, grade varchar(2));
CREATE TABLE sex (sex_name varchar(10), value bool);

CREATE INDEX idx_grad_year ON student (grad_year);
CREATE INDEX idx_major_id ON student (major_id);
CREATE INDEX idx_sex ON student (sex);

CREATE INDEX idx_prof ON section (prof);

CREATE INDEX idx_student_id ON enroll (student_id);
CREATE INDEX idx_grade ON enroll (grade);

INSERT INTO sex (sex_name, value) VALUES ('male', true);
INSERT INTO sex (sex_name, value) VALUES ('female', false);

EOM

case "${DBSIZE}" in
    "tiny")
	year_range=4
	student_num=20
	dept_num=3
	course_num=5
	section_num=5
	prof_num=10
	enroll_num=100
	;;
    "small")
	year_range=4
	student_num=100
	dept_num=5
	course_num=10
	section_num=20
	prof_num=10
	enroll_num=1000
	;;
    "medium")
	year_range=4
	student_num=600
	dept_num=5
	course_num=20
	section_num=50
	prof_num=20
	enroll_num=10000
	;;
    "large")
	year_range=6
	student_num=1200
	dept_num=10
	course_num=80
	section_num=600
	prof_num=50
	enroll_num=100000
	;;
    "x-large")
	year_range=20
	student_num=10000
	dept_num=20
	course_num=160
	section_num=1200
	prof_num=100
	enroll_num=10000000
	;;
esac

#
# STUDENT
#
for i in `seq 1 ${student_num}`
do
    major_id=$((`RAND 1 ${dept_num} 1` * 10))
    year=$((2020 + `RAND 0 ${year_range} 1`))
    birth_year=$((${year}-20))
    birth_month=`printf "%02d" $(($i % 12 + 1))`
    birth_day=`printf "%02d" $(($i % 28 + 1))` # safely
    
    sex=`RAND 0 1 1`
    if [ ${sex} -eq 0 ]
    then
	sex="true"
    else
	sex="false"
    fi
    
    echo "INSERT INTO student (sid, sname, grad_year, major_id, birth, sex) VALUES (${i}, 'name-${i}', ${year}, ${major_id}, '${birth_year}-${birth_month}-${birth_day}', ${sex});"
done

#
# DEPT
#
for i in `seq 1 ${dept_num}`
do
    did=$(((`RAND 1 ${dept_num} 1` + 1) * 10))
    
    echo "INSERT INTO dept (did, dname) VALUES (${did}, 'dept-${did}');"
done

#
# COURSE
#
for i in `seq 1 ${course_num}`
do
    mod=$(($i % ${dept_num}))
    did=$(((${mod}+1)*10))
    
    echo "INSERT INTO course (cid, title, dept_id) VALUES (${i}, 'course-${i}', ${did});"
done

#
# SECTION
#
for i in `seq 1 ${section_num}`
do
    mod=$(($i % ${dept_num}))
    did=$(((${mod}+1)*10))
    
    year=$((2016+${mod}))
    cid=`RAND 1 ${course_num} 1`
    pid=`RAND 1 ${prof_num} 1`
    echo "INSERT INTO section (sect_id, course_id, prof, year_offered) VALUES (${i}, ${cid}, 'prof-${pid}', ${year});"
done

#
# ENROLL
#
for i in `seq 1 ${enroll_num}`
do
    sid=`RAND 1 ${student_num} 1`
    sect_id=`RAND 1 ${section_num} 1`
    grade=`RAND 0 9 1`
    case $grade in
	0) grade="A+";;
	1) grade="A" ;;
	2) grade="A-";;
	3) grade="B+";;
	4) grade="B" ;;
	5) grade="B-";;
	6) grade="C" ;;
	7) grade="D" ;;
	8) grade="E" ;;
	9) grade="F" ;;
    esac

    echo "INSERT INTO enroll (eid, student_id, section_id, grade) VALUES (${i}, ${sid}, ${sect_id}, '${grade}');"
done
