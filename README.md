# 5300-Echidna

Group Members for sprint 1:
Meryll Cruz, Darin Hui

Group Members for sprin 2:
Thomas Bakken, Fangsheng Xu

## Sprint Verano
### Milestone 1
Parses command line input query to AST returned by the HyLine parser.

Use to build and run the program:
```
make
./sql5300 ~/cpsc5300/data
```

### Milestone 2
Implement a rudimentary storage engine.


Use to build and run the program:
```
make
./sql5300 ~/cpsc5300/data
```

Use to test:
```
SQL> test
```

Use the following command to clear folder for retesting:
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
