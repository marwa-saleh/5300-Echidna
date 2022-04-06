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

using namespace std;

const string EXIT = "quit";


//TO DO: add arguments in main func
int main() {
	//TO DO: create/open Berkeley DB env (probably in different file)
	

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
