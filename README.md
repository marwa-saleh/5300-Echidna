# 5300-Instructor
Instructor's DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2022

Usage (argument is database directory):
<pre>
$ ./sql5300 ~/cpsc5300/data
</pre>

## Tags
- <code>Milestone1</code> is playing around with the AST returned by the HyLine parser and general setup of the command loop.
- <code>Milestone2h</code> has the intructor-provided files for Milestone2. (Note that heap_storage.cpp is just a stub.)
- <code>Milestone2</code> is the instructor's attempt to complete the Milestone 2 assignment.
- <code>Milestone3_prep</code> has the instructor-provided files for Milestone 3. The students' work is in <code>SQLExec.cpp</code> labeled with <code>FIXME</code>.
- <code>Milestone4_prep</code> has the instructor-provided files for Milestone 4. The students' work is in <code>SQLExec.cpp</code> labeled with <code>FIXME</code>.
- <code>Milestone4</code> has the instructor's attempt to complete both the Milestone 3 and Milestone 4 assignments.
- <code>Milestone5_prep</code> has the instructor-provided files for Milestone5.
- <code>Milestone6_prep</code> has the instructor-provided files for Milestone6.

## Sprint Invierno

Team: Marwa Saleh, Ramya Anupoju

### **Milestone 5**

We did implementation of the following:

* INSERT TABLE
```sql 
SQL> INSERT INTO table (col_1, col_2, col_n) VALUES (1, 2, "three");
```
* DELETE TABLE
```sql 
SQL> DELETE FROM table WHERE col_1 = 1;
```
* SELECT ALL
```sql
SQL> SELECT * FROM table WHERE col_1 = 1 AND col_n = "three";
```
* SELECT SPECIFIC COLUMN
```sql
SQL> SELECT col_1, col_2 FROM table;
```

### **Milestone 6**

Implementation of B+ Tree Index -- just insert and lookup.
Range and del aren't implemented.
* To test:
```sql
SQL> test
```

## **Hand-off video
https://www.loom.com/share/669770858bd941c1993ef7cf2f21a71a 

## Unit Tests
There are some tests for SlottedPage and HeapTable. They can be invoked from the <code>SQL</code> prompt:
```sql
SQL> test
```
Be aware that failed tests may leave garbage Berkeley DB files lingering in your data directory. If you don't care about any data in there, you are advised to just delete them all after a failed test.
```sh
$ rm -f data/*
```

## Valgrind (Linux)
To run valgrind (files must be compiled with <code>-ggdb</code>):
```sh
$ valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 data
```
Note that we've added suppression for the known issues with the Berkeley DB library <em>vis-à-vis</em> valgrind.
