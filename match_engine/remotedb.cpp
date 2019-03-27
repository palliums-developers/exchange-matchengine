#include <iostream>
#include <string>
#include <vector>
#include <list>
#include <set>
#include <map>
#include <queue>
#include <functional>
#include <algorithm>
#include <mutex>
#include <thread>
#include <atomic>
#include <condition_variable>

#include <unistd.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#include <mariadb/mysql.h>

#include "../utils/utilities.h"

#include "remotedb.h"


bool RemoteDB::connect()
{
  if(!do_connect())
    return false;

  std::vector<std::vector<std::string>> vv;

  {
    auto query = "select COLUMN_NAME from information_schema.COLUMNS where table_name = 'exchange_order'";
    if(!do_query(query, &vv))
      return false;
    for(auto & v : vv)
      _orderKeys.push_back(v[0]);
  }
    
  {
    auto query = "select COLUMN_NAME from information_schema.COLUMNS where table_name = 'exchange_transaction'";
    if(!do_query(query, &vv))
      return false;
    for(auto & v : vv)
      _txKeys.push_back(v[0]);
  }
    
  return true;
}

std::vector<std::map<std::string, std::string>>
RemoteDB::get_orders(long idx)
{
  std::vector<std::map<std::string, std::string>> vv;
    
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM exchange_order WHERE idx >= %ld", idx);
  do_select(query, _orderKeys, &vv);
    
  return vv;
}

std::vector<std::map<std::string, std::string>>
RemoteDB::get_transactions(long idx)
{
  std::vector<std::map<std::string, std::string>> vv;
    
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM exchange_transaction WHERE idx >= %ld", idx);
  do_select(query, _txKeys, &vv);
    
  return vv;
}

bool RemoteDB::append_order(const std::map<std::string, std::string> & v)
{
  std::vector<std::string> keys;
  std::vector<std::string> vals;
  for(auto & a : v)
    {
      keys.push_back(a.first);
      vals.push_back(a.second);
    }
    
  char query[1024];
  snprintf(query, sizeof(query), "INSERT INTO exchange_order (%s) VALUES (%s)",
	   string_join(keys, ",").c_str(), string_join(vals, ",").c_str());
  printf("%s\n", query);
  return do_query(query, NULL);
}
  
bool RemoteDB::append_transaction(const std::map<std::string, std::string> & v, long idx)
{
  std::vector<std::string> keys;
  std::vector<std::string> vals;
  
  for(auto & a : v)
    {
      keys.push_back(a.first);
      if(a.first == "idx")
	vals.push_back(std::to_string(idx));
      else
	vals.push_back(a.second);
    }
  
  char query[1024];
  snprintf(query, sizeof(query), "INSERT INTO exchange_transaction (%s) VALUES (%s)",
	   string_join(keys, ",").c_str(), string_join(vals, ",").c_str());

  return do_query(query, NULL);
}
  
bool RemoteDB::do_connect()
{
  auto mysql = mysql_real_connect(_mysql, "47.106.208.207", "root", "1234qwer", "test", 0, NULL, 0);
  if(NULL == mysql)
    {
      print_mysql_error();
      return false;
    }

  _connected = true;
    
  return true;
}
  
bool RemoteDB::do_query(const char* query, std::vector<std::vector<std::string>>* vv)
{
  if(mysql_real_query(_mysql, query, strlen(query)))
    {
      print_mysql_error();
      return false;
    }
    
  if(vv != NULL)
    {
      vv->clear();
	
      auto result= mysql_store_result(_mysql);
      if(result == NULL)
	{
	  print_mysql_error();
	  return false;
	}
      auto rowscnt = mysql_num_rows(result);
      auto fldscnt = mysql_num_fields(result);
	
      for(int i=0; i<rowscnt; ++i)
	{
	  std::vector<std::string> v;
	  MYSQL_ROW row = mysql_fetch_row(result);
	  for(int j = 0; j < fldscnt; j++)
	    v.push_back(row[j] ? row[j] : "");
	  vv->push_back(v);
	}

      mysql_free_result(result);
    }
    
  return true;
}

bool RemoteDB::do_select(const char* query, std::vector<std::string> keys, std::vector<std::map<std::string, std::string>>* vv)
{
  if(mysql_real_query(_mysql, query, strlen(query)))
    {
      print_mysql_error();
      return false;
    }
    
  if(vv != NULL)
    {
      vv->clear();
	
      auto result= mysql_store_result(_mysql);
      if(result == NULL)
	{
	  print_mysql_error();
	  return false;
	}
      auto rowscnt = mysql_num_rows(result);
      auto fldscnt = mysql_num_fields(result);
	
      for(int i=0; i<rowscnt; ++i)
	{
	  std::map<std::string, std::string> v;
	  MYSQL_ROW row = mysql_fetch_row(result);
	  for(int j = 0; j < fldscnt; j++)
	    v[keys[j]] = (row[j] ? row[j] : "");
	  vv->push_back(v);
	}
	
      mysql_free_result(result);
    }
    
  return true;
}

void RemoteDB::run(volatile bool* alive)
{
  LOG(INFO, "remotedb start running!");
  
  while(*alive)
    {
      {
	auto v = get_orders(_startOrderIdx);
	for(auto & a : v)
	  {
	    auto order = map2json(a);
	    _qorders->push(std::pair<long, std::string>(_startOrderIdx++, order));
	    
	    LOG(INFO, "got a order: %ld, %s", _startOrderIdx-1, order.c_str());
	  }
	
	if(!v.empty())
	  LOG(INFO, "remotedb found %u orders", v.size());
      }
      
      {
	auto v = get_transactions(_startTxIdx);
	for(auto & a : v)
	  {
	    auto tx = map2json(a);
	    _qtxs->push(std::pair<long, std::string>(_startTxIdx++, tx));
	    
	    LOG(INFO, "got a transaction: %ld, %s", _startTxIdx-1, tx.c_str());
	  }
	
	if(!v.empty())
	  LOG(INFO, "remotedb found %u transactions", v.size());
      }

      long idx = _startTxIdx;
      while(!_qtxins.empty())
	{
	  auto tx = _qtxins.pop();
	  if(false == append_transaction(json2map(tx), idx++))
	     _qtxFails->push(tx);
	}
      
      sleep(1);
    }
  
  LOG(INFO, "remotedb exit!");
}
