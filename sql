iUSE COLLEGE
INSERT INTO MATH_STUDENTS VALUES(1,'MO',25);
INSERT INTO MATH_STUDENTS VALUES(2,'MO_T',35);
SELECT * FROM MATH_STUDENTS;

--EXEC sp_databases;
--EXEC sp_tables_rowset;

drop table student;
create table student(
num int primary key,
name varchar(20)
);

select * from student;
insert into student
(num,name)
values
(1,'mo'),
(2,'tom');

insert into student
(num,name)
values
(3,'mo1');

insert into student values (4,'mo2');

create database xyz;
use xyz;

create table employee_info(
id int primary key,
name varchar(39) not null,
salary int default 3000
);
insert into employee_info values (1, 'maaz',2000)
insert into employee_info values (2, 'baaz',3000)
insert into employee_info values (3, 'naaz',4000)
insert into employee_info values (3, 'faaz')
select * from employee_info


create table city(
id int primary key,
city varchar(20) default,
age int not null
constraint age check (age >= 18)
);

insert into city values (1,'delhi',21);
insert into city values (2,'delhi',22);
select * from city

drop table if exists student
use college;
create table studnet(
rollno int primary key,
name varchar(50),
marks int not null,
grade varchar(2),
city varchar(50)
);

insert into studnet
(rollno, name, marks, grade, city)
values 
(101,'mo',50,'B','HYD'),
(102,'moT',60,'A','DELHI'),
(103,'moTA',70,'A','MUMBAI'),
(104,'moTAM',90,'A','MP'),
(105,'moTAMJ',99,'A','KASHMIR');

SELECT * FROM studnet



