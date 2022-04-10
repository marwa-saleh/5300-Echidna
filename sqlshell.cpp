//Meryll Cruz and Darin Hui
//CPSC 4300/5300 
//sqlshell.cpp
//4-6-2022

#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <string>
#include <iostream>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"

using namespace std;
using namespace hsql;

string expressionToString(const Expr * expression);
string operatorToString(const Expr *opExpr);
string columnToString(const ColumnDefinition *col);
string tableRefToString(const TableRef *table);
string executeSelect(const SelectStatement *statement);
string executeCreate(const CreateStatement *statement);
string executeInsert(const InsertStatement *statement);
string execute(const SQLStatement *statement);

string expressionToString(const Expr * expression) {
  string result;

  switch (expression->type) {
    case kExprStar:
      result += "*";
      break;
    case kExprColumnRef:
      if (expression->table != NULL) {
        result += string(expression->table) + ".";
      }
    case kExprLiteralString:
      result += expression->name;
      break;
    case kExprLiteralFloat:
      result += to_string(expression->fval);
      break;
    case kExprLiteralInt:
      result += to_string(expression->ival);
      break;
    case kExprFunctionRef:
      result += string(expression->name) + " " + expression->expr->name;
      break;
    case kExprOperator:
      result += operatorToString(expression);
      break;
    default:
      result += "Unknown expression.";
      break;
  }

  if (expression->alias != NULL) {
    result += string(" AS ") + expression->alias;
  }

  return result;
}

string operatorToString(const Expr *opExpr){
  string result;

  if (opExpr == NULL) {
    return "NULL";
  }

  result += expressionToString(opExpr->expr) + " ";

  switch (opExpr->opType) {
    case Expr::SIMPLE_OP:
      result += opExpr->opChar;
      break;
    case Expr::AND:
      result += "AND";
      break;
    case Expr::OR:
      result += "OR";
      break;
    case Expr::NOT:
      result += "NOT";
      break;
    default:
      break;
  }
  if (opExpr->expr2 != NULL) {
    result += " " + expressionToString(opExpr->expr2);
  }

  return result;
}

string columnToString(const ColumnDefinition *col){

  string result = string(col->name) + " ";

  switch(col->type){
    case ColumnDefinition::DOUBLE:
      result += "DOUBLE ";
      break;
    case ColumnDefinition::INT:
      result += "INT ";
      break;
    case ColumnDefinition::TEXT:
      result += "TEXT ";
      break;
    default:
      result += "...";
      break;
  }

  return result;
}


string tableRefToString(const TableRef *table) {
  string result;
  switch (table->type) {
    case kTableSelect:
      result += "kTableSelect not implemented"; 
      break;
    case kTableName:
      result += table->name;
      if (table->alias != NULL){
        result += string(" AS ") + table->alias;
      }
      break;
    case kTableJoin:
      result += tableRefToString(table->join->left);
      switch (table->join->type) {
        case kJoinCross:
          result += " JOIN CROSS NOT IMPLEMENTED ";
          break;
        case kJoinInner:
          result += " JOIN ";
          break;
        case kJoinOuter:
          result += " JOIN OUTER NOT IMPLEMENTED ";
          break;
        case kJoinLeftOuter:
          result += " JOIN LEFT OUTER NOT IMPLEMENTED ";
          break;
        case kJoinLeft:
          result += " LEFT JOIN ";
          break;
        case kJoinRightOuter:
          result += " JOIN RIGHT OUTER NOT IMPLEMENTED ";
          break;
        case kJoinRight:
          result += " RIGHT JOIN ";
          break;
        case kJoinNatural:
          result += " NATURAL JOIN ";
          break;
      }
      result += tableRefToString(table->join->right);
      if (table->join->condition != NULL){
        result += " ON " + expressionToString(table->join->condition);
      }
      break;
    case kTableCrossProduct:
      bool comma = false;
      for (TableRef *tbl : *table->list) {
        if (comma)
          result += ", ";
        result += tableRefToString(tbl);
        comma = true;
      }
      break;
  }
  return result;
}


string executeSelect(const SelectStatement *statement) {
  string result = "SELECT ";
  bool comma = false;
  for (Expr *expr : *statement->selectList) {
    if (comma) {
      result += ", ";
    }
    result += expressionToString(expr);
    comma = true;
  }
  result += " FROM " + tableRefToString(statement->fromTable);
  if (statement->whereClause != NULL) {
    result += " WHERE " + expressionToString(statement->whereClause);
  }
  return result;
}

string executeCreate(const CreateStatement *statement) {
  string result = "CREATE TABLE ";
  bool ifComma = false;

  if(statement->ifNotExists){
    return result + "DOES NOT EXIST ";
  }

  result += string(statement->tableName) + " (";

  for(ColumnDefinition *column: *statement->columns){
    if(ifComma){
      result += ", ";
    }

    result += columnToString(column);
    ifComma = true;
  }

  result += ")";
  return result;
}

string executeInsert(const InsertStatement *statement) {
  string result = " INSERT NOT IMPLEMENTED";
  return result;
}

string execute(const SQLStatement *statement) {
  switch (statement->type()) {
    case kStmtSelect:
      return executeSelect((const SelectStatement *) statement);
    case kStmtInsert:
      return executeInsert((const InsertStatement *) statement);
    case kStmtCreate:
      return executeCreate((const CreateStatement *) statement);
    default:
      return "Statement not implemented.";
  }
}

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

	while (true) {
		//TODO: parse/execute SQL
		cout << "SQL> \n";
		string input = "";
		getline(cin, input);

		if (input == EXIT) {
			break;
		}
		parsedResult = SQLParser::parseSQLString(input);
    	if (parsedResult->isValid()) {
      		for (uint i = 0; i < parsedResult->size(); i++) {
      		  cout << execute(parsedResult->getStatement(i)) << endl;
    		}
  		}
   		 else {
      		cout << "ERROR: Invalid SQL" << endl;
    	}
    	delete parsedResult; //saves memory
	}

	return 0;
}
