//Meryll Cruz and Darin Hui
//CPSC 4300/5300 
//sql5300.cpp
//4-24-2022

#include <cstdlib>
#include <iostream>
#include <string>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sql_parser.h"
#include "SQLExec.h"

using namespace std;
using namespace hsql;

/*
 * _DB_ENV global variable
 */
DbEnv *_DB_ENV;

/**
 * Main entry point of the sql5300 program
 * @args dbenvpath  the path to the BerkeleyDB database environment
 */
int main(int argc, char *argv[]) {

    // Open/create the db enviroment
    if (argc != 2) {
        cerr << "Usage: cpsc5300: dbenvpath" << endl;
        return 1;
    }

    char *env_home = argv[1];
    cout << "(sql5300: running with database environment at " << env_home << ")" << endl;
    DbEnv env(0U);
    env.set_message_stream(&cout);
    env.set_error_stream(&cerr);
    try {
        env.open(env_home, DB_CREATE | DB_INIT_MPOOL, 0);
    } catch (DbException &exc) {
        cerr << "(sql5300: " << exc.what() << ")";
        exit(1);
    }
    _DB_ENV = env;
    initialize_schema_tables();

    // Enter the SQL shell loop
    while (true) {
        cout << "SQL> ";
        string query;
        getline(cin, query);
        if (query.length() == 0)
            continue;  
        if (query == "quit")
            break;  
        if (query == "test") {
            cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
            continue;
        }

        // use the sql parser to get us our AST
        SQLParserResult *parse_tree = SQLParser::parseSQLString(query);
        if (!parse_tree->isValid()) {
            cout << "invalid SQL: " << query << endl;
            cout << parse_tree->errorMsg() << endl;
        }
        else {
            for (uint i = 0; i < parse_tree->size(); ++i) {
                const SQLStatement *statement = parse_tree->getStatement(i);
                try {
                    cout << ParseTreeToString::statement(statement) << endl;
                    QueryResult *query_result = SQLExec::execute(statement);
                    cout << *query_result << endl;
                    delete query_result;
                }
                catch (SQLExecError &e) {
                    cout << "Error: " << e.what() << endl;
                }
                cout << SQLExec::execute(parse_tree->getStatement(i)) << endl;
            }
        }
        delete parse_tree;
    }
    return EXIT_SUCCESS;
}