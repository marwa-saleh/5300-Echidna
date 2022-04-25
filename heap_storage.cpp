//Meryll Cruz and Darin Hui
//CPSC 4300/5300 
//heap_storage.cpp
//4-13-2022

#include <vector>
#include "db_cxx.h"
#include <cstring>
#include <map>
#include <algorithm>
#include <iostream>
#include "storage_engine.h"
#include "heap_storage.h"
#include <cstdint>

using namespace std;
typedef uint16_t u16;

/**
 * @class Slotted Page
 */
typedef u_int16_t u16;

//Provided constructor
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

//Get given record from block, throws error if record not found
Dbt* SlottedPage::get(RecordID record_id) {
    u16 size, loc;
    get_header(size, loc, record_id);

    if (size == 0)
        throw ("Record id is not a record: " + record_id);

    return new Dbt(address(loc), size);
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

//Put given data in record provided, throws error if no room in block
void SlottedPage::put(RecordID record_id, const Dbt &data) {
    u16 size, loc;
    get_header(size, loc, record_id);
    u16 size_new = data.get_size();

    if (size_new > size) { //do we need to slide records to make space
        if (!has_room(size_new - size)) { //is there enough room in block
                throw DbBlockNoRoomError("not enough room for new record");
        }
        //slide records, put record on new index
        slide(loc, loc - size_new);
        memcpy(this->address(loc - size_new), data.get_data(), size_new);
    } else {
        //put record on index of loc
        memcpy(this->address(loc), data.get_data(), size_new);
    }
    put_header(); //store block header
    put_header(record_id, size_new, loc);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

//Get the header of given record id, and change its size and loc to 0
//Slide the rest of the data in that block
void SlottedPage::del(RecordID record_id) {
    u16 size, loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);
    u16 end = loc + size;
    slide(loc, end);
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

//Return size and offset (location) for a provided record id
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) {
    if (id > num_records)
        throw ("Record id is not a record: " + id);

    size = get_n(4 * id);
    loc = get_n((4 * id) + 2);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

//Return if there is room to store a record with given size (includes 4 bytes)
bool SlottedPage::has_room(u16 size) {
    u16 room = end_free - (4 * num_records);
    return (room >= size);
}

//Returns all record ids
RecordIDs* SlottedPage::ids(void) {
    u16 size, loc;
    RecordIDs* all = new RecordIDs;
    
    for (RecordID i = 1; i < this->num_records; i++) {
        get_header(size, loc, i);
        if (loc != 0) all->push_back(i);
    }
    return all;
}

void SlottedPage::slide(u16 start, u16 end) {
    u16 slide = end - start;
    if (slide == 0) return;

    //slide data 
    memcpy(this->address(this->end_free + slide + 1), address(this->end_free + 1), slide);

    RecordIDs* curr = this->ids();
    for (RecordID& id : *curr) {
        u16 size, loc;
        get_header(size, loc, id);
        if (loc <= start) {
            loc += slide;
            put_header(id, size, loc);
        }
    }
    end_free += slide;
    put_header();
}
/**
 * @class HeapFile - heap file implementation of DbFile
 *
 * Heap file organization. Built on top of Berkeley DB RecNo file. There is one of our
        database blocks for each Berkeley DB record in the RecNo file. In this way we are using Berkeley DB
        for buffer management and file management.
        Uses SlottedPage for storing records within blocks.
 */

void HeapFile::create(void) {
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage *blockPage = this->get_new();
    delete blockPage;
}

void HeapFile::drop(void) {
    this->close();
    this->closed = true;
}

void HeapFile::open(void) {
    this->db_open();
}

void HeapFile::close(void) {
    this->db.close(0);
    this->closed = true;
}

SlottedPage *HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage *page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

SlottedPage *HeapFile::get(BlockID block_id) {
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, block_id, false);
}

void HeapFile::put(DbBlock *block) {
    int block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));
    this->db.put(nullptr, &key, block->get_block(), 0);
}

BlockIDs* HeapFile::block_ids(){
    BlockIDs* blockIds(0);
    for(BlockID i = 1; i < (BlockID)this->last + 1; i++){
        blockIds->push_back(i);
    }
    return blockIds;
}

void HeapFile::db_open(uint flags) {
    if (!this->closed) 
        return;

    this->db.set_re_len(DbBlock::BLOCK_SZ);
    this->dbfilename = this->name + ".db";
    this->db.open(nullptr, (this->dbfilename).c_str(), nullptr, DB_RECNO, flags, 0644);
    DB_BTREE_STAT *stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    this->last = flags ? 0 : stat->bt_ndata;
    this->closed = false;
}

/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : DbRelation(table_name, column_names, column_attributes), file(table_name) {}

void HeapTable::create() {
    file.create();
}

void HeapTable::create_if_not_exists() {
    try {
        open();
    } catch (DbException &e) {
        create();
    }
}

/**
 * Execute: DROP TABLE <table_name>
 */
void HeapTable::drop() {
    file.drop();
}

/**
 * Open existing table. Enables: insert, update, delete, select, project
 */
void HeapTable::open() {
    file.open();
}

/**
 * Closes the table. Disables: insert, update, delete, select, project
 */
void HeapTable::close() {
    file.close();
}

/**
 * Execute: INSERT INTO <table_name> (<row_keys>) VALUES (<row_values>)
 * @param row a dictionary with column name keys
 * @return the handle of the inserted row
 */
Handle HeapTable::insert(const ValueDict *row) {
    open();
    ValueDict *full_row = validate(row);
    Handle handle = append(full_row);
    delete full_row;
    return handle;
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    throw DbRelationError("Not implemented");
}

void HeapTable::del(const Handle handle) {
    open();
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = this->file.get(block_id);
    block->del(record_id);
    this->file.put(block);
    delete block;
}

/**
 * Execute: SELECT <handle> FROM <table_name> WHERE 1
 * @return a list of handles for qualifying rows
 */
Handles *HeapTable::select() {
    return select(nullptr);
}

/**
 * The select command
 * @param where predicates to match
 * @return list of handles of the selected rows
 */
Handles *HeapTable::select(const ValueDict *where) {
    open();
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();
    for (auto const &block_id: *block_ids) {
        SlottedPage *block = file.get(block_id);
        RecordIDs *record_ids = block->ids();
        for (auto const &record_id: *record_ids) {
            Handle handle(block_id, record_id);
            if (selected(handle, where))
                handles->push_back(Handle(block_id, record_id));
        }
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

/**
 * Project all columns from a given row.
 * @param handle row to be projected
 * @return a sequence of all values for handle
 */
ValueDict *HeapTable::project(Handle handle) {
    return project(handle, &this->column_names);
}

/**
 * Project given columns from a given row.
 * @param handle row to be projected
 * @param column_names of columns to be included in the result
 * @return a sequence of values for handle given by column_names
 */
ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = file.get(block_id);
    Dbt *data = block->get(record_id);
    ValueDict *row = unmarshal(data);
    delete data;
    delete block;
    if (column_names->empty())
        return row;
    ValueDict *result = new ValueDict();
    for (auto const &column_name: *column_names) {
        if (row->find(column_name) == row->end())
            throw DbRelationError("table does not have column named '" + column_name + "'");
        (*result)[column_name] = (*row)[column_name];
    }
    delete row;
    return result;
}

/**
 * Check if the given row is acceptable to insert.
 * @param row to be validated
 * @return the full row dictionary
 * @throws DbRelationError if not valid
 */
ValueDict *HeapTable::validate(const ValueDict *row) const {
    ValueDict *full_row = new ValueDict();
    for (auto const &column_name: this->column_names) {
        Value value;
        ValueDict::const_iterator column = row->find(column_name);
        if (column == row->end())
            throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
        else
            value = column->second;
        (*full_row)[column_name] = value;
    }
    return full_row;
}

Handle HeapTable::append(const ValueDict *row) {
    Dbt *data = marshal(row);
    SlottedPage *block = this->file.get(this->file.get_last_block_id());
    RecordID record_id;
    try {
        record_id = block->add(data);
    } catch (DbBlockNoRoomError &e) {
        // need a new block
        delete block;
        block = this->file.get_new();
        record_id = block->add(data);
    }
    this->file.put(block);
    delete block;
    delete[] (char *) data->get_data();
    delete data;
    return Handle(this->file.get_last_block_id(), record_id);
}

/**
 * Figure out the bits to go into the file.
 * The caller is responsible for freeing the returned Dbt and its enclosed ret->get_data().
 * @param row data for the tuple
 * @return bits of the record as it should appear on disk
 */
Dbt *HeapTable::marshal(const ValueDict *row) const {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;

        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            if (offset + 4 > DbBlock::BLOCK_SZ - 4)
                throw DbRelationError("row too big to marshal");
            *(int32_t *) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u_long size = value.s.length();
            if (size > UINT16_MAX)
                throw DbRelationError("text field too long to marshal");
            if (offset + 2 + size > DbBlock::BLOCK_SZ)
                throw DbRelationError("row too big to marshal");
            *(u16 *) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else if (ca.get_data_type() == ColumnAttribute::DataType::BOOLEAN) {
            if (offset + 1 > DbBlock::BLOCK_SZ - 1)
                throw DbRelationError("row too big to marshal");
            *(uint8_t *) (bytes + offset) = (uint8_t) value.n;
            offset += sizeof(uint8_t);
        } else {
            throw DbRelationError("Only know how to marshal INT, TEXT, and BOOLEAN");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

/**
 * Figure out the memory data structures from the given bits gotten from the file.
 * @param data file data for the tuple
 * @return row data for the tuple
 */
ValueDict *HeapTable::unmarshal(Dbt *data) const {
    ValueDict *row = new ValueDict();
    Value value;
    char *bytes = (char *) data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        value.data_type = ca.get_data_type();
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            value.n = *(int32_t *) (bytes + offset);
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u16 size = *(u16 *) (bytes + offset);
            offset += sizeof(u16);
            char buffer[DbBlock::BLOCK_SZ];
            memcpy(buffer, bytes + offset, size);
            buffer[size] = '\0';
            value.s = string(buffer);  // assume ascii for now
            offset += size;
        } else if (ca.get_data_type() == ColumnAttribute::DataType::BOOLEAN) {
            value.n = *(uint8_t *) (bytes + offset);
            offset += sizeof(uint8_t);
        } else {
            throw DbRelationError("Only know how to unmarshal INT, TEXT, and BOOLEAN");
        }
        (*row)[column_name] = value;
    }
    return row;
}

/**
 * See if the row at the given handle satisfies the given where clause
 * @param handle  row to check
 * @param where   conditions to check
 * @return        true if conditions met, false otherwise
 */
bool HeapTable::selected(Handle handle, const ValueDict *where) {
    if (where == nullptr)
        return true;
    ValueDict *row = this->project(handle, where);
    bool is_selected = *row == *where;
    delete row;
    return is_selected;
}

/**
 * Test helper. Sets the row's a and b values.
 * @param row to set
 * @param a column value
 * @param b column value
 */
void test_set_row(ValueDict &row, int a, string b) {
    row["a"] = Value(a);
    row["b"] = Value(b);
    row["c"] = Value(a % 2 == 0);  // true for even, false for odd
}

/**
 * Test helper. Compares row to expected values for columns a and b.
 * @param table    relation where row is
 * @param handle   row to check
 * @param a        expected column value
 * @param b        expected column value
 * @return         true if actual == expected for both columns, false otherwise
 */
bool test_compare(DbRelation &table, Handle handle, int a, string b) {
    ValueDict *result = table.project(handle);
    Value value = (*result)["a"];
    if (value.n != a) {
        delete result;
        return false;
    }
    value = (*result)["b"];
    if (value.s != b) {
		delete result;
        return false;
	}
    value = (*result)["c"];
	delete result;
    if (value.n != (a % 2 == 0))
        return false;
    return true;

}

/**
 * Testing function for heap storage engine.
 * @return true if the tests all succeeded
 */
bool test_heap_storage() {
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    column_names.push_back("c");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::BOOLEAN);
    column_attributes.push_back(ca);

    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    cout << "create ok" << endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    cout << "drop ok" << endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    cout << "create_if_not_exists ok" << endl;

    ValueDict row;
    string b = "Four score and seven years ago our fathers brought forth on this continent, a new nation, conceived in Liberty, and dedicated to the proposition that all men are created equal.";
    test_set_row(row, -1, b);
    table.insert(&row);
    cout << "insert ok" << endl;
    Handles *handles = table.select();
    if (!test_compare(table, (*handles)[0], -1, b))
        return false;
    cout << "select/project ok " << handles->size() << endl;
    delete handles;

    Handle last_handle;
    for (int i = 0; i < 1000; i++) {
        test_set_row(row, i, b);
        last_handle = table.insert(&row);
    }
    handles = table.select();
    if (handles->size() != 1001)
        return false;
    int i = -1;
    for (auto const &handle: *handles) {
        if (!test_compare(table, handle, i++, b))
            return false;
    }
    cout << "many inserts/select/projects ok" << endl;
    delete handles;

    table.del(last_handle);
    handles = table.select();
    if (handles->size() != 1000)
        return false;
    i = -1;
    for (auto const &handle: *handles) {
        if (!test_compare(table, handle, i++, b))
            return false;
    }
    cout << "del ok" << endl;
    table.drop();
    delete handles;
    return true;
}