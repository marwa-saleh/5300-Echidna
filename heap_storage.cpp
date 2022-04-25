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

void HeapTable::create(void) {
    this->file.create();
}

void HeapTable::create_if_not_exists(void) {
    try {
        this->open();
    } catch (DbException &e)//(DbRelationError const&)
    {
        this->create();
    }
}

void HeapTable::drop(void) {
    this->file.drop();
}

void HeapTable::open(void) {
    this->file.open();
}

void HeapTable::close(void) {
    this->file.close();
}

Handle HeapTable::insert(const ValueDict *row) {
    this->open();
    return this->append(this->validate(row));
}

Handles* HeapTable::select(void) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

Handles* HeapTable::select(const ValueDict *where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

ValueDict* HeapTable::validate(const ValueDict *row) {
    ValueDict tempRow = *row;
    ValueDict* fullRow = new ValueDict();
    Value v;
    for (Identifier column_name : this->column_names) {
        if (tempRow.find(column_name) == tempRow.end()) {
            throw new DbRelationError("DbRelationError");
        } else {
            v = tempRow[column_name];
        }
        fullRow->insert(pair<Identifier , Value>(column_name, v));
    }
    return fullRow;
}

Handle HeapTable::append(const ValueDict *row) {
    Dbt *data = this->marshal(row);
    SlottedPage * block = this->file.get(this->file.get_last_block_id());
    u_int16_t record_id;
    try {
        record_id = block->add(data);
    } catch (DbRelationError const&) {
        block = this->file.get_new();
        record_id = block->add(data);
    }
    this->file.put(block);
    unsigned int id = this->file.get_last_block_id();
    std::pair<BlockID, RecordID> Handle (id, record_id);
    return Handle;
}

Dbt* HeapTable::marshal(const ValueDict *row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;

}

ValueDict * HeapTable::unmarshal(Dbt *data){
    std::map<Identifier, Value> * row = {};
    char *bytes = new char[DbBlock::BLOCK_SZ];
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            value.n = *(int32_t*) (bytes + offset);
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {

        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
        (*row)[column_name] = value;
    }
    delete[] bytes;
    return row;
}

void HeapTable::del(const Handle handle) {
    open();
    BlockID blk_id = handle.first;
    RecordID rec_id = handle.second;

    SlottedPage* block = this->file.get(blk_id);
    block->del(rec_id);
    this->file.put(block);
    delete block;
}

ValueDict* HeapTable::project(Handle handle) {
    return project(handle, &this->column_names);
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names) {
    BlockID blk_id = handle.first;
    RecordID rec_id = handle.second;
    SlottedPage* block = file.get(blk_id);
    Dbt* data = block->get(rec_id);
    ValueDict* row = unmarshal(data);

    if (column_names->empty()) return row;
    ValueDict* output = new ValueDict();

    for (auto const& column_name : *column_names) {
        (*output)[column_name] = (*row)[column_name];
    }

    delete row;
    delete block;
    delete data;
    return output;
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    throw ("Need to implement");
}

//from provided test_heap_storage.cpp
bool test_heap_storage() {
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    cout << "create ok" << endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    cout << "drop ok" << endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    cout << "create_if_not_exsts ok" << endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    cout << "try insert" << endl;
    table.insert(&row);
    cout << "insert ok" << endl;
    Handles* handles = table.select();
    cout << "select ok " << handles->size() << endl;
    ValueDict *result = table.project((*handles)[0]);
    cout << "project ok" << endl;
    Value value = (*result)["a"];
    if (value.n != 12)
        delete handles;
        delete result;
        return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
        delete handles;
        delete result;
        return false;
    table.drop();
    delete handles;
    delete result;
    return true;
}