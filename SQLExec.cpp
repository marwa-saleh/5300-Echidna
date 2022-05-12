/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2022"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

/**
 * Query Result is comprised of column names, column attributes, and rows. (maybe messages too? python example)
 */
QueryResult::~QueryResult() {
    // FIXME
    ColumnNames this->cn 
    ColumnAttributes this->ca 
    vector<ValueDict> this->rows

}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (tables == nullptr) {
        initialize_schema_tables();
        this->tables.create()
    }

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

//Milestone 3 Code Below. 
typedef vector<ValueDict> Rows;

void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    column_attribute.set_data_type(col->type.data_type); //type is hsql ColumnType, which has DataType data_type
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    //initialize message
    string message = "Success"
    //table name
    Identifier table_name = str(statement->tableName);
    //insert new table row to _tables
    ValueDict new_tables_row;
    new_tables_row["table_name"] = Value(table_name);
    try {
        Handle tables_handle = this->table.insert(new_tables_row);
        this->tables.get_table(table_name); //creates the new table if
        try {
            //get column definitions (name and attributes)
            Handles columns_handles;
            ValueDict new_columns_row;
            new_columns_row["table_name"] = Value(table_name);
            for (ColumnDefinition *col: statement.columns) {
                Identifier column_name;
                ColumnAttribute column_attribute;
                column_definition(col, column_name, column_attribute);
                new_columns_row["column_name"] = Value(column_name);
                new_columns_row["data_type"] = Value(ColumnAttribute.get_data_type()); //enum to str error?
                Handle columns_handle = this->tables.columns_table.insert(new_columns_row);
                columns_handles.push_back(columns_handle);
            }
        }
        catch DbRelationError(&exc) {
            //rollback changes in _tables and _columns
            this->table.del(handle)
            for (Handle handle: columns_handles) {
                this->table.columns_table.del(handle)
            }
            message = "Failure: " + exc.what();
        }
    }
    catch DbRelationError(&exc) {
        message = "Failure: " + exc.what();
    }
    //delete stuff
    //return message
    return new QueryResult(message);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    Identifier table_name = str(statement->name);
    //drop the table
    this->tables.get_table().drop() //get_table creates a new table if it doesn't exist

    //delete row from tables
    this->tables.del(tables_handle) //deletes table from cache before deleting handle from table
    //delete rows from columns
    for (Handle columns_handle: columns_handles) {
        this->tables.columns_table.del(handle);
    }
    //return success without checking if table_name existed
    return new QueryResult("Success"); 
}

QueryResult *SQLExec::show(const ShowStatement *statement) { 
    switch(statement->type) {
        case kShowTables {
            return show_tables();
        }
        case kShowColumns {
            return show_columns();
        }
        default {
            return new QueryResult("Not Possible")
        }
    }
}

QueryResult *SQLExec::show_tables() {
    Handles *handles = this->tables.select();
    ValueDicts *rows;
    for (Handle handle: handles) {
        ValueDict *row = this->tables.project(handle);
        //make sure row isn't _table or _columns, the two metadata tables
        if (row->at("table_name") == "_tables" || row->at("table_name") != "_columns") { //should be handled in select where
            rows->push_back(row);
        }
    }
    //hard_coding unnecessary, but easy
    ColumnNames column_names{ "table_name" };
    ColumnAttributes column_attributes{ DataType::TEXT };
    
    return new QueryResult(&column_names, &column_attributes, rows, "Success"); 
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    Identifier table_name = statement->name //select from

    ColumnNames *column_names;
    ColumnAttributes *column_attributes;
    this->tables.get_columns(table_name, column_names, column_attributes);
    ValueDicts *rows = new ValueDicts();
    return new QueryResult(&column_names, column_attributes, rows, "Success"); // FIXME

    /**
    Columns columns = this->tables.get_table("_columns"); //Columns::table_name might work
    Handles handles = columns.select();
    ValueDicts *rows;
    for (Handle handle: handles) {
        ValueDict row = this->tables.project(handle);
        rows->push_back(row);
    }
    */
}

