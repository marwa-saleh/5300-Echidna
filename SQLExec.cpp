/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
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

QueryResult::~QueryResult() {
    delete column_names;
    delete column_attributes;
    delete rows;
    message.clear();
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (tables == nullptr) {
        initialize_schema_tables();
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
        case CreateStatement::CreateType::kIndex:
            return create_index(statement);
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
            throw;
        }
    }
    catch (DbRelationError &exc) {
        //cout << "exc2" << endl; need to specify failure type

        message = exc.what();
        // throw this error
        throw;
    }

    return new QueryResult(message);
}

// helper method
QueryResult *SQLExec::create_index(const CreateStatement *statement)
{
    // parser's result
    // Identifier table_name = statement->tableName;
    std::string table_name(statement->tableName);
    Identifier index_name = statement->indexName;
    Identifier index_type = statement->indexType;
    std::vector<char*>* index_columns = statement->indexColumns;

    // check if the table exists
    try {
        tables->get_table(table_name);
    } catch (...) {
        return new QueryResult("Error: table " + table_name + " does not exist");
    }

    // add new index to _indices
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    // tables->get_columns(table_name, column_names, column_attributes); // FIXME: DOES NOT COMPILE
    ValueDict row;
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(index_name);
    row["index_type"] = Value(index_type);
    row["is_unique"] = index_type == "BTREE" ? Value(1) : Value(0);

    Handles index_handles;
    int seq = 1;

    // insert a row for each colum in index key into _indices
    // use static reference to _indices

    for (auto column: *index_columns) {
        // TODO check that all the index coumns exits in the table
        row["column_name"] = Value(column);
        row["seq_in_index"] = Value(seq++);

        try {
            index_handles.push_back(indices->insert(&row));
        } catch (DbRelationError &exc) {
            // roll-back inserts

        }
    }

    // call get_index to get a reference to the new index and then invoke the create method on it
    DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
    index.create();
    return new QueryResult("created index " + index_name);
    // return new QueryResult("not implemented");
}

// DROP ...
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

// TODO before dropping the table, drop each index on the table
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
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
    return new QueryResult(string("drop table " + table_name));
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;

    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);

    // call get_index to get a reference to the index
    // and then invoke the drop method on it
    DbIndex &index = indices->get_index(table_name, index_name);

    // remove all the rows from _indices for this index
    Handles *handles = indices->select(&where);
    for (auto const &handle: *handles){
        indices->del(handle);
    }
    index.drop();

    return new QueryResult(std::string("dropped index ") + index_name);
}

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

// shows all indices for a given table
QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    ColumnNames *column_names = new ColumnNames;
    ColumnAttributes *column_attributes = new ColumnAttributes;

    column_names->push_back("table_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("index_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("column_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("seq_in_index");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::INT));

    column_names->push_back("index_type");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("is_unique");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));

    ValueDict where;
    where["table_name"] = Value(std::string(statement->tableName));

    Handles* handles = SQLExec::indices->select(&where);
    int count = handles->size();

    ValueDicts *rows = new ValueDicts();
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::indices->project(handle, column_names);
        rows->push_back(row);
    }

    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                            "succesfully returned " + to_string(count) + " rows");
}