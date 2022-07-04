SELECT sid, sname, dname, grad_year, birth FROM student, dept WHERE did = major_id;
SELECT tblname, fldname, type, length FROM fldcat WHERE tblname = 'student';
SELECT sname, grade, prof FROM student, enroll, section WHERE sid = student_id AND sect_id = section_id;
SELECT sect_id FROM einstein;
SELECT sname, grade, prof FROM student, enroll, section WHERE sid = student_id AND sect_id = section_id AND prof = 'einstein';
SELECT sid, sname, dname, grad_year, birth FROM student, dept WHERE did = major_id AND grad_year = 2020;
