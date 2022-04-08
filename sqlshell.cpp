//Meryll Cruz and Darin Hui
//CPSC 4300/5300 
//sqlshell.cpp
//4-6-2022

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <iostream>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"

using namespace std;
using namespace hsql;

const char *HOME = "cpsc5300/data";
const char *SQLSHELL = "sqlshell.db";
const unsigned int BLOCK_SZ = 4096;

const char *EXIT = "quit";

/**
 * Main entry point of sqlshell program
 **/

//TO DO: add arguments in main func
int main(int argc, char **argv) {
	//TO DO: create/open Berkeley DB env (probably in different file)
	
	const char *home = std::getenv("HOME");
	string envdir = std::string(home) + "/" + HOME;

	DbEnv env(0U);
	env.set_message_stream(&cout);
	env.set_error_stream(&cerr);
	env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

	Db db(&env, 0);
	db.set_message_stream(env.get_message_stream());
	db.set_error_stream(env.get_error_stream());
	db.set_re_len(BLOCK_SZ); // Set record length to 4K
	db.open(NULL, SQLSHELL, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644); // Erases anything already there

	//TODO: Consider using while(true) and break when input is quit
    //user-input loop for SQL 
    std::string input = "";
	cout << "SQL> \n";
	getline(cin, input);

	while (input != EXIT) {
		//TODO: parse/execute SQL
		
		cout << "SQL> \n";
		getline(cin, input);
	}

	return 0;
}
