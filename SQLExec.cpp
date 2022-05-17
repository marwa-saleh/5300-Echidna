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
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
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

QueryResult::~QueryResult() {
    // FIXME
    delete column_names;
    delete column_attributes;
    delete rows;
    message.clear();
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // FIXME: initialize _tables table, if not yet present
    if (tables == nullptr) {
        initialize_schema_tables();
        tables = new Tables();
        // tables->open();
        // DbRelation *col_table = &tables->get_table(Columns::TABLE_NAME);
        // col_table->open();
    }
    // if (SQLExec::tables == nullptr)
    // {
    //     SQLExec::tables = new Tables();
    // }
    // if (SQLExec::indices == nullptr)
    // {
    //     SQLExec::indices = new Indices();
    // }

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

void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    //convert from hsql::ColumnType::DataType to ColumnDefinition::DataType
    switch (col->type) {
        case hsql::ColumnDefinition::DataType::TEXT:
            column_attribute.set_data_type(ColumnAttribute::DataType::TEXT);
            break;
        case hsql::ColumnDefinition::DataType::INT:
            column_attribute.set_data_type(ColumnAttribute::DataType::INT);
            break;
        default:
            //other types not implemented
            throw SQLExecError(string("Data Type not implemented"));
    }
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type)
    {
        case CreateStatement::CreateType::kTable:
            return create_table(statement);
        // case CreateStatement::CreateType::kIndex:
        //     return create_index(statement);
        default:
            return new QueryResult("not implemented");
    }
}

// helper method
QueryResult *SQLExec::create_table(const CreateStatement *statement)
{
    switch (statement->type) {
        case CreateStatement::kTable :
            break; //do the stuff below, will break out later with create index
        default :
            throw SQLExecError(string("Only create table implemented"));
    }
    //initialize message
    string message = "Success";
    //table name
    Identifier table_name = statement->tableName;
    //insert new table row to _tables
    ValueDict new_tables_row;
    new_tables_row["table_name"] = Value(table_name);

    try {
        Handle tables_handle = tables->insert(&new_tables_row);
        DbRelation *table = &tables->get_table(table_name); //creates the new table if
        table->create_if_not_exists();
        DbRelation *col_table = &tables->get_table(Columns::TABLE_NAME);
        Handles columns_handles;

        try {
            //get column definitions (name and attributes)
            ValueDict new_columns_row;
            new_columns_row["table_name"] = Value(table_name);
            for (ColumnDefinition *col: *statement->columns) {
                Identifier column_name;
                ColumnAttribute column_attribute;
                column_definition(col, column_name, column_attribute);
                new_columns_row["column_name"] = Value(column_name);
                if (column_attribute.get_data_type() == ColumnAttribute::DataType::TEXT) {
                    new_columns_row["data_type"] = Value("TEXT");
                }
                else if (column_attribute.get_data_type() == ColumnAttribute::DataType::INT) {
                    new_columns_row["data_type"] = Value("INT");
                }

                Handle columns_handle = col_table->insert(&new_columns_row); //calls dbrelation insert?
                columns_handles.push_back(columns_handle);
            }
        }
        catch (DbRelationError &exc) {
            //rollback changes in _tables and _columns
            tables->del(tables_handle);
            for (Handle handle: columns_handles) {
                col_table->del(handle);
            }
            //cout << exc.what() << endl; need to specify failure type
            message = "Failure";
        }
    }
    catch (DbRelationError &exc) {
        //cout << "exc2" << endl; need to specify failure type
        message = exc.what();
    }

    return new QueryResult(message);
}

// helper method
QueryResult *SQLExec::create_index(const CreateStatement *statement)
{


    return new QueryResult("not implemented");
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::kTable :
            break; //do the stuff below, will break out later with create index
        default :
            throw SQLExecError(string("Only drop table implemented"));
    }
    Identifier table_name = statement->name;
    //drop the table
    tables->get_table(table_name).drop(); //get_table creates a new table if it doesn't exist

    //delete row from tables
    ValueDict where;
    where["table_name"] = Value(table_name);
    Handles *tables_handle = tables->select(&where);
    tables->del(tables_handle->front()); //deletes table from cache before deleting handle from table
    //delete rows from columns

    DbRelation *col_tables = &tables->get_table(Columns::TABLE_NAME);
    Handles columns_handles = *col_tables->select(&where);
    for (Handle columns_handle: columns_handles) {
        col_tables->del(columns_handle);
    }
    return new QueryResult(string("Success"));
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch(statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        default:
            throw SQLExecError(string("Not Possible"));
    }
}

QueryResult *SQLExec::show_tables() {
    Handles *handles = tables->select();
    ValueDicts *rows = new ValueDicts();

    ColumnNames *column_names = new ColumnNames;
    for (const Handle &handle: *handles) {
        ValueDict* row = tables->project(handle, column_names);
        if (((*row)["table_name"]) != Value(Tables::TABLE_NAME)
         && ((*row)["table_name"]) != Value(Columns::TABLE_NAME)
         && ((*row)["table_name"]) != Value(Indices::TABLE_NAME) ) {
            rows->push_back(row);
        }
    }
    //hard_coding unnecessary, but easy
    ColumnAttributes *column_attributes = new ColumnAttributes();
    tables->get_columns(Tables::TABLE_NAME, *column_names, *column_attributes);

    delete handles;
    string message = "successfully returned " + to_string(rows->size()) + " rows";
    return new QueryResult(column_names, column_attributes, rows, message);
}


QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    Identifier table_name = statement->tableName;

    ColumnNames *column_names = new ColumnNames();
    ColumnAttributes *column_attributes = new ColumnAttributes();
    tables->get_columns(Columns::TABLE_NAME, *column_names, *column_attributes);

    //get the rows for the specified table
    ValueDicts *rows = new ValueDicts();
    ValueDict where = *(new ValueDict());
    where["table_name"] = Value(table_name);
    DbRelation *col_table = &tables->get_table(Columns::TABLE_NAME);

    Handles *handles = col_table->select(&where);
    for (Handle handle : *handles) {
        rows->push_back(col_table->project(handle));
    }
    int columns_count = handles->size();

    delete handles;
    return new QueryResult(column_names, column_attributes, rows, " successfully returned " + to_string(columns_count)+ " rows");
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
     return new QueryResult("show index not implemented"); // FIXME
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    return new QueryResult("drop index not implemented");  // FIXME
}

