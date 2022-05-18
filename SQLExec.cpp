/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Thomas& Fangsheng
 * @see "Seattle University, CPSC5300, Spring 2022"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr; // milestone3
Indices *SQLExec::indices = nullptr; // milestone 4

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

// deletor
QueryResult::~QueryResult() {
    delete column_names;
    delete column_attributes;
    delete rows;
    message.clear();
}

// execute sql statement
QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (tables == nullptr) {
        tables = new Tables();
    }
    // initialize _indices, if not yet present
    if (SQLExec::indices == nullptr)
    {
        SQLExec::indices = new Indices();
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

// set column attribute
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

// handle create statement
QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type)
    {
        case CreateStatement::CreateType::kTable:
            return create_table(statement);
        case CreateStatement::CreateType::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("not implemented");
    }
}

// helper method: create table
QueryResult *SQLExec::create_table(const CreateStatement *statement)
{
    //initialize message
    string message = "Success";
    //table name
    Identifier table_name = statement->tableName;
    //insert new table row to _tables
    ValueDict row;
    row["table_name"] = Value(table_name);

    Handle tables_handle = tables->insert(&row); // insert the new _tables row

    DbRelation *col_table = &tables->get_table(Columns::TABLE_NAME);
    Handles columns_handles;

    try {
        //get column definitions (name and attributes) to insert _columns rows
        for (ColumnDefinition *col: *statement->columns) {
            Identifier column_name;
            ColumnAttribute column_attribute;
            column_definition(col, column_name, column_attribute);
            // row tablename is already set
            row["column_name"] = Value(column_name);
            if (column_attribute.get_data_type() == ColumnAttribute::DataType::TEXT) {
                row["data_type"] = Value("TEXT");
            }
            else if (column_attribute.get_data_type() == ColumnAttribute::DataType::INT) {
                row["data_type"] = Value("INT");
            }

            Handle columns_handle = col_table->insert(&row); //calls dbrelation insert?
            columns_handles.push_back(columns_handle);
        }
    }
    catch (DbRelationError &exc) {
        //rollback changes in _tables and _columns
        tables->del(tables_handle);
        for (Handle handle: columns_handles) {
            col_table->del(handle);
        }
        throw exc;
    }
    DbRelation *table = &tables->get_table(table_name); //creates the new table if
    table->create_if_not_exists(); // could this be create() ?
    return new QueryResult(message);
}

// helper method: create index
QueryResult *SQLExec::create_index(const CreateStatement *statement)
{
    // create and calidate 6 columns
    Identifier table_name = statement->tableName; //column 2

    ColumnNames *column_names = new ColumnNames();
    ColumnAttributes *column_attributes = new ColumnAttributes();
    tables->get_columns(table_name, *column_names, *column_attributes);
    delete column_attributes; //only need column_names to validate

    Identifier index_name = statement->indexName; //column 1
    Identifier index_type = statement->indexType; //column 5

    bool is_unique; //set column 6
    if (index_type == "BTREE") {
        is_unique = true;
    }
    else if (index_type == "HASH") {
        is_unique = false;
    }
    else {
        throw SQLExecError("Index type not implemented");
    }

    int seq_in_index = 0;
    Handles handles; //to store in case of rollback
    ValueDict row;
    //set row values
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(index_name);
    row["index_type"] = Value(index_type);
    row["is_unique"] = Value(is_unique);
    try {
        for (auto const &col : *statement->indexColumns) {
            Identifier column_name = string(col); //column 3
            bool valid = false;
            for (Identifier name : *column_names) {
                if (column_name == name) {
                    valid = true;
                    break;
                }
            }
            if (!valid) {
                throw DbRelationError("Column not in Table");
            }
            seq_in_index++; //column 4
            //set 2 variable values
            row["column_name"] = Value(column_name);
            row["seq_in_index"] = Value(seq_in_index);

            Handle handle = indices->insert(&row);
            handles.push_back(handle);
        }
    }
    catch (DbRelationError &exc) {
        for (Handle handle: handles) {
            indices->del(handle);
        }
        throw exc;
    }
    DbIndex *new_index = &indices->get_index(table_name, index_name);
    new_index->create();
    return new QueryResult("created index " + index_name);
}

// handle drop statement
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type)
    {
        case DropStatement::EntityType::kTable:
            return drop_table(statement);
        case DropStatement::EntityType::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("DropStatement not implemented");
    }
}

// handle drop table
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    ValueDict where;
    where["table_name"] = Value(table_name);
    Handles *tables_handle = tables->select(&where);
    //make sure table_name exists
    if (tables_handle->size() == 0) {
        throw DbRelationError("Table doesn't exist");
    }
    //drop the table
    tables->get_table(table_name).drop(); //get_table creates a new table if it doesn't exist

    //delete row from tables
    tables->del(tables_handle->front()); //deletes table from cache before deleting handle from table

    //delete rows from columns
    DbRelation *col_tables = &tables->get_table(Columns::TABLE_NAME);
    Handles columns_handles = *col_tables->select(&where);
    for (Handle columns_handle: columns_handles) {
        col_tables->del(columns_handle);
    }

    //delete indices on table
    IndexNames index_names = indices->get_index_names(table_name);
    for (Identifier index_name : index_names) {
        //drop index
        indices->get_index(table_name, index_name).drop();
        //delete rows in _indices
        where["index_name"] = Value(index_name);
        Handles *handles = indices->select(&where);
        for (Handle handle : *handles) {
            indices->del(handle);
        }
    }
    return new QueryResult(string("drop table " + table_name));
}

// drop index
QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;

    ValueDict where;
    where["table_name"] = Value(table_name);
    Handles *tables_handle = tables->select(&where);
    //make sure table_name exists
    if (tables_handle->size() == 0) {
        throw DbRelationError("Table doesn't exist");
    }
    where["index_name"] = Value(index_name);
    Handles *handles = indices->select(&where);

    if (tables_handle->size() == 0) {
        throw DbRelationError("Index doesn't exist");
    }
    // call get_index to get a reference to the index
    // and then invoke the drop method on it
    indices->get_index(table_name, index_name).drop();
    // remove all the rows from _indices for this index
    for (Handle handle : *handles) {
        indices->del(handle);
    }
    //delete index; //drop is undefined for DummyIndex

    return new QueryResult(std::string("dropped index ") + index_name);
}

// handle show statement
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type)
    {
    case ShowStatement::EntityType::kTables:
        return show_tables();
    case ShowStatement::EntityType::kColumns:
        return show_columns(statement);
    case ShowStatement::EntityType::kIndex:
        return show_index(statement);
    default:
        return new QueryResult(std::string("Showstatement type not implemented"));
    }
}

// handle show tables
QueryResult *SQLExec::show_tables() {
    Handles *handles = tables->select();
    ValueDicts *rows = new ValueDicts();

    ColumnNames *column_names = new ColumnNames;
    for (Handle &handle: *handles) {
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

// show columns
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

// shows all indices for a given table
QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    Identifier table_name = statement->tableName;

    ColumnNames *column_names = new ColumnNames(); // QueryResult arg 1
    ColumnAttributes *column_attributes = new ColumnAttributes(); // QueryResult arg 2
    tables->get_columns(Indices::TABLE_NAME, *column_names, *column_attributes);

    ValueDicts *rows = new ValueDicts();
    ValueDict where = *(new ValueDict());

    where["table_name"] = Value(table_name);
    Handles *handles = indices->select(&where);

    for (Handle handle : *handles) {
        rows->push_back(indices->project(handle));
    }

    std::string message = "Successfully returned " + to_string(handles->size()) + " rows";
    return new QueryResult(column_names, column_attributes, rows, message);
}