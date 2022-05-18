# 5300-Echidna

_Sprint Otoño_

Sprint 2 Team:
Thomas Bakken& Fangsheng Xu

Sprint 1 Team:
Meryll& Darin

## Comments from Sprint Verano

Milestone3 and 4 are fully functional to the test cases provided. Best assumptions were made to handle error cases with no functionality provided. There may be some rem The team was mindful of memory leaks, being careful to delete pointers after use. The code base is from github repository klundeen/5300-Instructor. Errors remaining in the sql parser provided for Milestone4 were fixed. SQLExec was written by the sprint Verano Team. 

Video Link: https://www.youtube.com/watch?v=ZrI10mtJ7NY (sorry the video is blurry, not sure how to up the resolution in my screen recorder... hope the audio walkthrough still helps. 

## Milestone 1:

Program written in C++ that runs from the command line and prompts the user for SQL statements and then executes them one at a time, just like the mysql program.

To build the program:
$ make

To run the program:
$ ./sql5300 ~/cpsc5300/data

## Milestone 2:

The storage engine is made up of three layers: DbBlock, DbFile, and DbRelation.
Created a program for the implementations for the Heap Storage Engine's version of each: SlottedPage, HeapFile, and HeapTable.


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
