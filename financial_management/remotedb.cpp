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
#include <chrono>
#include <unordered_map>

using namespace std::chrono_literals;

#include <mariadb/mysql.h>

#include "../utils/utilities.h"

#include "remotedb.h"
#include "financial_management.h"

bool RemoteDB::connect()
{
  if(!do_connect())
    return false;

  std::vector<std::vector<std::string>> vv;

  {
    auto query = "select COLUMN_NAME from information_schema.COLUMNS where table_name = 'financial_management_projects'";
    if(do_query(query, &vv))
      return false;
    for(auto & v : vv)
      _projectKeys.push_back(v[0]);
  }

  {
    auto query = "select COLUMN_NAME from information_schema.COLUMNS where table_name = 'financial_management_users'";
    if(do_query(query, &vv))
      return false;
    for(auto & v : vv)
      _userKeys.push_back(v[0]);
  }
  
  {
    auto query = "select COLUMN_NAME from information_schema.COLUMNS where table_name = 'financial_management_orders'";
    if(do_query(query, &vv))
      return false;
    for(auto & v : vv)
      _orderKeys.push_back(v[0]);
  }

  LOG(INFO, "remotedb connect success...");
  
  return true;
}

std::shared_ptr<Project> RemoteDB::get_project(long id)
{
  std::vector<std::map<std::string, std::string>> vv;
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM financial_management_projects WHERE id=%ld", id);
  do_select(query, _projectKeys, &vv);

  std::shared_ptr<Project> o;
  if(!vv.empty())
    o = Project::create(vv[0]);
  return o;
}

std::shared_ptr<Project> RemoteDB::get_project_by_no(std::string no)
{
  std::vector<std::map<std::string, std::string>> vv;
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM financial_management_projects WHERE no='%s'", no.c_str());
  do_select(query, _projectKeys, &vv);

  std::shared_ptr<Project> o;
  if(!vv.empty())
    o = Project::create(vv[0]);
  return o;
}

std::shared_ptr<User> RemoteDB::get_user(long id)
{
  std::vector<std::map<std::string, std::string>> vv;
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM financial_management_users WHERE id=%ld", id);
  do_select(query, _userKeys, &vv);

  std::shared_ptr<User> o;
  if(!vv.empty())
    o = User::create(vv[0]);
  return o;
}

std::shared_ptr<User> RemoteDB::get_user_by_publickey(std::string publickey)
{
  std::vector<std::map<std::string, std::string>> vv;
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM financial_management_users WHERE publickey='%s'", publickey.c_str());
  do_select(query, _userKeys, &vv);
  
  std::shared_ptr<User> o;
  if(!vv.empty())
    o = User::create(vv[0]);
  return o;  
}

std::shared_ptr<Order> RemoteDB::get_order(long id)
{
  std::vector<std::map<std::string, std::string>> vv;
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM financial_management_orders WHERE id=%ld", id);
  do_select(query, _orderKeys, &vv);

  std::shared_ptr<Order> o;
  if(!vv.empty())
    o = Order::create(vv[0]);
  return o;
}

void zip_map(const std::map<std::string, std::string> & v, std::string & keys, std::string & vals)
{
  std::vector<std::string> ks;
  std::vector<std::string> vs;
  for(auto & a : v)
    {
      ks.push_back("`" + a.first + "`");
      vs.push_back("'" + a.second + "'");
    }
  keys = string_join(ks, ",");
  vals = string_join(vs, ",");
}

int RemoteDB::add_user(std::shared_ptr<User> o)
{
  auto v = o->to_map();
  
  std::string keys;
  std::string vals;
  zip_map(v, keys, vals);

  char query[1024];
  snprintf(query, sizeof(query), "INSERT INTO financial_management_users (%s) VALUES (%s)", keys.c_str(), vals.c_str());
  //printf("%s\n", query);

  return do_query(query, NULL);
}

int RemoteDB::add_order(std::shared_ptr<Order> o)
{
  auto v = o->to_map();
  
  std::string keys;
  std::string vals;
  zip_map(v, keys, vals);
  
  char query[1024];
  snprintf(query, sizeof(query), "INSERT INTO financial_management_orders (%s) VALUES (%s)", keys.c_str(), vals.c_str());
  //printf("%s\n", query);
  
  return do_query(query, NULL);
}

std::vector<std::shared_ptr<Project>>
RemoteDB::get_projects(long id)
{
  std::vector<std::map<std::string, std::string>> vv;
    
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM financial_management_projects WHERE id >= %ld", id);
  do_select(query, _projectKeys, &vv);

  std::vector<std::shared_ptr<Project>> v;

  for(auto & a : vv)
    v.push_back(Project::create(a));
  
  return v;
}

long RemoteDB::get_projects_cnt()
{
  return get_rows_count("financial_management_projects");
}

long RemoteDB::get_users_cnt()
{
  return get_rows_count("financial_management_users");
}

long RemoteDB::get_orders_cnt()
{
  return get_rows_count("financial_management_orders");
}

std::vector<std::shared_ptr<Project>>
RemoteDB::get_projects_by_limit(long start, long cnt)
{
  std::vector<std::shared_ptr<Project>> v;
  auto vv = get_record_by_limit("financial_management_projects", _projectKeys, start, cnt);
  for(auto & a : vv)
    v.push_back(Project::create(a));
  return v;
}

std::vector<std::shared_ptr<User>>
RemoteDB::get_users_by_limit(long start, long cnt)
{
  std::vector<std::shared_ptr<User>> v;
  auto vv = get_record_by_limit("financial_management_users", _userKeys, start, cnt);
  for(auto & a : vv)
    v.push_back(User::create(a));
  return v;
}

std::vector<std::shared_ptr<Order>>
RemoteDB::get_orders_by_limit(long start, long cnt)
{
  std::vector<std::shared_ptr<Order>> v;
  auto vv = get_record_by_limit("financial_management_orders", _orderKeys, start, cnt);
  for(auto & a : vv)
    v.push_back(Order::create(a));
  return v;
}

int RemoteDB::update_order_status(long orderid, int status)
{
  char query[1024];
  snprintf(query, sizeof(query), "UPDATE financial_management_orders SET status=%d WHERE id=%ld", status, orderid);
  return do_query(query, NULL);
}

int RemoteDB::update_project_status(long projectid, int status)
{
  char query[1024];
  snprintf(query, sizeof(query), "UPDATE financial_management_projects SET status=%d WHERE id=%ld", status, projectid);
  return do_query(query, NULL);
}

int RemoteDB::update_order_txid(long orderid, std::string txid, std::string investment_return_addr, int payment_timestamp)
{
  char query[1024];
  snprintf(query, sizeof(query), "UPDATE financial_management_orders SET payment_txid='%s', investment_return_addr='%s', payment_timestamp=%d WHERE id=%ld", txid.c_str(), investment_return_addr.c_str(), payment_timestamp, orderid);
  return do_query(query, NULL);
}

int RemoteDB::update_order_confirm_timestamp(long orderid, int now)
{
  char query[1024];
  snprintf(query, sizeof(query), "UPDATE financial_management_orders SET audited_timestamp=%d WHERE id=%ld", now, orderid);
  return do_query(query, NULL);
}

int RemoteDB::update_collections(long userid, std::string collections)
{
  char query[1024];
  snprintf(query, sizeof(query), "UPDATE financial_management_users SET collections='%s' WHERE id=%ld", collections.c_str(), userid);
  return do_query(query, NULL);
}

bool RemoteDB::do_connect()
{
  auto ip = Config::instance()->get("remotedb_ip");
  auto usr = Config::instance()->get("remotedb_usr");
  auto pwd = Config::instance()->get("remotedb_pwd");
  auto dbname = Config::instance()->get("remotedb_dbname");

  LOG(INFO, "start to connect mariadb...");
  
  //auto mysql = mysql_real_connect(_mysql, "47.106.208.207", "root", "1234qwer", "test", 0, NULL, 0);
  auto mysql = mysql_real_connect(_mysql, ip.c_str(), usr.c_str(), pwd.c_str(), dbname.c_str(), 0, NULL, 0);
  if(NULL == mysql)
    {
      print_mysql_error();
      return false;
    }

  LOG(INFO, "connect mariadb success...");
  
  _connected = true;
    
  return true;
}
  
int RemoteDB::do_query(const char* query, std::vector<std::vector<std::string>>* vv)
{
  if(mysql_real_query(_mysql, query, strlen(query)))
    {
      print_mysql_error();
      return ERROR_MYSQL_REAL_QUERY_FAILED;
    }
    
  if(vv != NULL)
    {
      vv->clear();
	
      auto result= mysql_store_result(_mysql);
      if(result == NULL)
	{
	  print_mysql_error();
	  return ERROR_MYSQL_STORE_RESULT_FAILED;
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
    
  return 0;
}

int RemoteDB::do_select(const char* query, std::vector<std::string> keys, std::vector<std::map<std::string, std::string>>* vv)
{
  if(mysql_real_query(_mysql, query, strlen(query)))
    {
      print_mysql_error();
      return ERROR_MYSQL_REAL_QUERY_FAILED;
    }
    
  if(vv != NULL)
    {
      vv->clear();
	
      auto result= mysql_store_result(_mysql);
      if(result == NULL)
	{
	  print_mysql_error();
	  return ERROR_MYSQL_STORE_RESULT_FAILED;
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
    
  return 0;
}

long RemoteDB::get_rows_count(const char* table)
{
  char query[256];
  snprintf(query, sizeof(query), "SELECT COUNT(*) FROM %s", table);

  std::vector<std::vector<std::string>> vv;
  
  auto ret = do_query(query, &vv);
  
  if(ret != 0 || vv.empty())
    return 0;

  return atol(vv[0][0].c_str());
};

std::map<std::string, std::string> RemoteDB::get_first_record(const char* table, const std::vector<std::string> & keys)
{
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM %s LIMIT 0, 1", table);
  
  std::map<std::string, std::string> v;
  std::vector<std::map<std::string, std::string>> vv;
  
  auto ret = do_select(query, keys, &vv);
  
  if(ret != 0 || vv.empty())
    return v;
  
  return vv[0];
}

std::vector<std::map<std::string, std::string>>
RemoteDB::get_record_by_limit(const char* table, const std::vector<std::string> & keys, long start, long cnt)
{
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM %s LIMIT %ld, %ld", table, start, cnt);
  
  std::vector<std::map<std::string, std::string>> vv;
  
  auto ret = do_select(query, keys, &vv);
  
  return vv;
}

