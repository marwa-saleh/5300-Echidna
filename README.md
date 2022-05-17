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
rm -f ~/cpsc5300/data/*
```
## Milestone 3:

Rudimentary implementation of a Schema Storage that support the following commands:
* CREATE TABLE
#### Syntax:
```
<table_definition> ::= CREATE TABLE <table_name> (<column_definitions> )
<column_definitions> ::= <column_definition> | <column_definition>, <column_definitions>
<column_definition> ::= <column_name> <datatype>
```
* DROP TABLE
#### Syntax:
```
<drop_table_statement> ::= DROP TABLE <table_name>
```
* SHOW TABLES
#### Syntax:
```
<show_tables_statement> ::= SHOW TABLES
```
* SHOW COLUMNS
#### Syntax:
```
<show_columns_statement> ::= SHOW COLUMNS FROM <table_name>
```

## Milestone 4:

Rudimentary implementation of SQL index commands. Supports the following commands:
* CREATE INDEX
#### Syntax:
```
CREATE INDEX index_name ON table_name [USING {BTREE | HASH}] (col1, col2, ...)
```
* SHOW INDEX
#### Syntax:
```
SHOW INDEX FROM table_name
```
* DROP INDEX
#### Syntax:
```
DROP INDEX index_name ON table_name
```
