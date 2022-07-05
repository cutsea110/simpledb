SELECT sid, sname, dname, grad_year, birth FROM student, dept WHERE did = major_id;
SELECT tblname, fldname, type, length FROM fldcat WHERE tblname = 'student';
SELECT sname, grade, prof FROM student, enroll, section WHERE sid = student_id AND sect_id = section_id;
SELECT sname, grade, prof FROM student, enroll, section WHERE sid = student_id AND sect_id = section_id AND grad_year = 2022;
SELECT sid, sname, dname, title, prof FROM student, dept, course, section WHERE did = major_id AND did = dept_id AND cid = course_id AND sname = 'name-2';
SELECT sid, sname, dname, grad_year, birth, sex_name FROM student, dept, sex WHERE did = major_id AND sex=value AND grad_year = 2020;
