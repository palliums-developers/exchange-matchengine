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
#include <memory>

#include <unistd.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <chrono>
#include <unordered_map>

#include <assert.h>

using namespace std::chrono_literals;

#include <mariadb/mysql.h>

#include "../utils/utilities.h"

#include "remotedb.h"
#include "financial_management.h"

std::vector<std::string> RemoteDB::_projectKeys;
std::vector<std::string> RemoteDB::_userKeys;
std::vector<std::string> RemoteDB::_orderKeys;


bool RemoteDB::connect()
{
  if(!do_connect())
    return false;

  std::vector<std::vector<std::string>> vv;

  static bool done = false;
  if(!done)
    {
      done = true;
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
    }

  dot("*");
  //LOG(INFO, "remotedb connect success...");
  
  return true;
}

std::shared_ptr<Project> RemoteDB::get_project(long id)
{
  return get_project_impl(_mysql, id);
}

std::shared_ptr<Project> RemoteDB::get_project_impl(MYSQL* mysql, long id)
{
  std::vector<std::map<std::string, std::string>> vv;
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM financial_management_projects WHERE id=%ld", id);
  do_select_impl(mysql, query, _projectKeys, &vv);

  std::shared_ptr<Project> o;
  if(!vv.empty())
    o = Project::create(vv[0]);
  return o;
}

std::shared_ptr<Project> RemoteDB::get_project_by_no(std::string no)
{
  return get_project_by_no_impl(_mysql, no);
}

std::shared_ptr<Project> RemoteDB::get_project_by_no_impl(MYSQL* mysql, std::string no)
{
  std::vector<std::map<std::string, std::string>> vv;
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM financial_management_projects WHERE no='%s'", no.c_str());
  do_select_impl(mysql, query, _projectKeys, &vv);

  std::shared_ptr<Project> o;
  if(!vv.empty())
    o = Project::create(vv[0]);
  return o;
}

std::shared_ptr<User> RemoteDB::get_user(long id)
{
  return get_user_impl(_mysql, id);
}

std::shared_ptr<User> RemoteDB::get_user_impl(MYSQL* mysql, long id)
{
  std::vector<std::map<std::string, std::string>> vv;
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM financial_management_users WHERE id=%ld", id);
  do_select_impl(mysql, query, _userKeys, &vv);

  std::shared_ptr<User> o;
  if(!vv.empty())
    o = User::create(vv[0]);
  return o;
}

std::shared_ptr<User> RemoteDB::get_user_by_publickey(std::string publickey)
{
  return get_user_by_publickey_impl(_mysql, publickey);
}

std::shared_ptr<User> RemoteDB::get_user_by_publickey_impl(MYSQL* mysql, std::string publickey)
{
  std::vector<std::map<std::string, std::string>> vv;
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM financial_management_users WHERE publickey='%s'", publickey.c_str());
  do_select_impl(mysql, query, _userKeys, &vv);
  
  std::shared_ptr<User> o;
  if(!vv.empty())
    o = User::create(vv[0]);
  return o;  
}

std::shared_ptr<Order> RemoteDB::get_order(long id)
{
  return get_order_impl(_mysql, id);
}

std::shared_ptr<Order> RemoteDB::get_order_impl(MYSQL* mysql, long id)
{
  std::vector<std::map<std::string, std::string>> vv;
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM financial_management_orders WHERE id=%ld", id);
  do_select_impl(mysql, query, _orderKeys, &vv);

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
  return add_user_impl(_mysql, o);
}

int RemoteDB::add_user_impl(MYSQL* mysql, std::shared_ptr<User> o)
{
  auto v = o->to_map();
  
  std::string keys;
  std::string vals;
  zip_map(v, keys, vals);

  char query[1024];
  snprintf(query, sizeof(query), "INSERT INTO financial_management_users (%s) VALUES (%s)", keys.c_str(), vals.c_str());
  //printf("%s\n", query);

  return do_query_impl(mysql, query, NULL);
}

int RemoteDB::add_order(std::shared_ptr<Order> o)
{
  //raii r("add_order");
  return add_order_impl(_mysql, o);
}

int RemoteDB::add_order_impl(MYSQL* mysql, std::shared_ptr<Order> o)
{
  auto v = o->to_map();
  
  std::string keys;
  std::string vals;
  zip_map(v, keys, vals);
  
  char query[1024];
  snprintf(query, sizeof(query), "INSERT INTO financial_management_orders (%s) VALUES (%s)", keys.c_str(), vals.c_str());
  //printf("%s\n", query);
  
  return do_query_impl(mysql, query, NULL);
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

std::shared_ptr<Order>
RemoteDB::get_last_order()
{
  auto v = get_last_record("financial_management_orders", _orderKeys);
  if(v.empty())
    return std::shared_ptr<Order>();
  return Order::create(v);
}

std::shared_ptr<Project>
RemoteDB::get_last_project()
{
  auto v = get_last_record("financial_management_projects", _projectKeys);
  if(v.empty())
    return std::shared_ptr<Project>();
  return Project::create(v);
}

int RemoteDB::update_order_status(long orderid, int status)
{
  return update_order_status_impl(_mysql, orderid, status);
}

int RemoteDB::update_order_status_impl(MYSQL* mysql, long orderid, int status)
{
  char query[1024];
  snprintf(query, sizeof(query), "UPDATE financial_management_orders SET status=%d WHERE id=%ld", status, orderid);
  return do_query_impl(mysql, query, NULL);
}

int RemoteDB::update_project_status(long projectid, int status)
{
  return update_project_status_impl(_mysql, projectid, status);
}

int RemoteDB::update_project_status_impl(MYSQL* mysql, long projectid, int status)
{
  char query[1024];
  snprintf(query, sizeof(query), "UPDATE financial_management_projects SET status=%d WHERE id=%ld", status, projectid);
  return do_query_impl(mysql, query, NULL);
}

int RemoteDB::update_project_received_crowdfunding(long projectid, double amount)
{
  return update_project_received_crowdfunding_impl(_mysql, projectid, amount);
}

int RemoteDB::update_project_received_crowdfunding_impl(MYSQL* mysql, long projectid, double amount)
{
  char query[512];
  snprintf(query, sizeof(query), "UPDATE financial_management_projects SET received_crowdfunding=%f WHERE id=%ld", amount, projectid);
  return do_query_impl(mysql, query, NULL);
}

int RemoteDB::update_crowdfunding_payment_txid(long projectid, std::string crowdfunding_payment_txid)
{
  return update_crowdfunding_payment_txid_impl(_mysql, projectid, crowdfunding_payment_txid);
}

int RemoteDB::update_crowdfunding_payment_txid_impl(MYSQL* mysql, long projectid, std::string crowdfunding_payment_txid)
{
  char query[512];
  snprintf(query, sizeof(query), "UPDATE financial_management_projects SET crowdfunding_payment_txid='%s' WHERE id=%ld", crowdfunding_payment_txid.c_str(), projectid);
  return do_query_impl(mysql, query, NULL);
}

int RemoteDB::update_order_txid(long orderid, std::string txid, std::string investment_return_addr, int payment_timestamp, std::string oracle_publickey, std::string contract_address, std::string contract_script, int vout, int mc, int ms, int version, int locktime, std::string script_oracle_address)
{
  return update_order_txid_impl(_mysql, orderid, txid, investment_return_addr, payment_timestamp, oracle_publickey, contract_address, contract_script, vout, mc, ms, version, locktime, script_oracle_address);
}

int RemoteDB::update_order_txid_impl(MYSQL* mysql, long orderid, std::string txid, std::string investment_return_addr, int payment_timestamp, std::string oracle_publickey, std::string contract_address, std::string contract_script, int vout, int mc, int ms, int version, int locktime, std::string script_oracle_address)
{
  char query[2048];
  snprintf(query, sizeof(query), "UPDATE financial_management_orders SET payment_txid='%s', investment_return_addr='%s', payment_timestamp=%d, oracle_publickey='%s', contract_address='%s', contract_script='%s', vout=%d, mc=%d, ms=%d, version=%d, locktime=%d, script_oracle_address='%s' WHERE id=%ld", txid.c_str(), investment_return_addr.c_str(), payment_timestamp, oracle_publickey.c_str(), contract_address.c_str(), contract_script.c_str(), vout, mc, ms, version, locktime, script_oracle_address.c_str(), orderid);
  return do_query_impl(mysql, query, NULL);
}

int RemoteDB::update_order_confirm_timestamp(long orderid, int now)
{
  return update_order_confirm_timestamp_impl(_mysql, orderid, now);
}

int RemoteDB::update_order_confirm_timestamp_impl(MYSQL* mysql, long orderid, int now)
{
  char query[1024];
  snprintf(query, sizeof(query), "UPDATE financial_management_orders SET audited_timestamp=%d WHERE id=%ld", now, orderid);
  return do_query_impl(mysql, query, NULL);
}

int RemoteDB::update_collections(long userid, std::string collections)
{
  return update_collections_impl(_mysql, userid, collections);
}

int RemoteDB::update_collections_impl(MYSQL* mysql, long userid, std::string collections)
{
  char query[1024];
  snprintf(query, sizeof(query), "UPDATE financial_management_users SET collections='%s' WHERE id=%ld", collections.c_str(), userid);
  return do_query_impl(mysql, query, NULL);
}

std::vector<long> RemoteDB::get_project_orders(long projectid)
{
  std::vector<long> v;

  int idx = 0;
  int cnt = 200;

  for(;;)
    {
      std::vector<std::map<std::string, std::string>> vv;
    
      char query[256];
      snprintf(query, sizeof(query), "SELECT id FROM financial_management_orders WHERE project_id = %ld LIMIT %d,%d", projectid, idx, cnt);
  
      std::vector<std::string> keys{"id"};
  
      do_select(query, keys, &vv);

      for(auto & a : vv)
	v.push_back(atol(a["id"].c_str()));
      
      if(vv.size() < cnt)
	break;
      
      idx += cnt;
    }

  return v;
}

std::vector<long> RemoteDB::get_user_orders(long userid)
{
  std::vector<long> v;

  int idx = 0;
  int cnt = 200;
  
  for(;;)
    {
      std::vector<std::map<std::string, std::string>> vv;
      
      char query[256];
      snprintf(query, sizeof(query), "SELECT id FROM financial_management_orders WHERE user_id = %ld LIMIT %d,%d", userid, idx, cnt);
  
      std::vector<std::string> keys{"id"};
  
      do_select(query, keys, &vv);

      for(auto & a : vv)
	v.push_back(atol(a["id"].c_str()));

      if(vv.size() < cnt)
	break;
      
      idx += cnt;
    }

  return v;  
}

std::vector<std::shared_ptr<Order>> 
RemoteDB::get_user_orders_limit(std::shared_ptr<User> user, std::vector<int> statuses, int offset, int count, int direction)
{
  auto userid = user->_id;

  std::vector<std::shared_ptr<Order>> v;
  
  std::vector<std::map<std::string, std::string>> vv;
  
  char query[256];
  
  const char* dir = (direction==0 ? "DESC": "");

  std::string status;
  for(int i=0; i<statuses.size(); ++i)
    {
      if(i > 0)
	status += " OR ";
      status += "status=" + std::to_string(statuses[i]);
    }

  if(statuses.empty())
    snprintf(query, sizeof(query), "SELECT * FROM financial_management_orders WHERE user_id=%ld ORDER BY id %s LIMIT %d,%d", userid, dir, offset, count);
  else
    snprintf(query, sizeof(query), "SELECT * FROM financial_management_orders WHERE user_id=%ld AND (%s) ORDER BY id %s LIMIT %d,%d", userid, status.c_str(), dir, offset, count);
  
  do_select(query, _orderKeys, &vv);

  for(auto & a : vv)
    v.push_back(Order::create(a));

  return v;
}

std::vector<std::shared_ptr<Order>> 
RemoteDB::get_project_orders_limit(std::shared_ptr<Project> project, std::vector<int> statuses, int offset, int count, int direction)
{
  auto projectid = project->_id;

  std::vector<std::shared_ptr<Order>> v;
  
  std::vector<std::map<std::string, std::string>> vv;
  
  char query[256];
  
  const char* dir = (direction==0 ? "DESC": "");

  std::string status;
  for(int i=0; i<statuses.size(); ++i)
    {
      if(i > 0)
	status += " OR ";
      status += "status=" + std::to_string(statuses[i]);
    }

  if(statuses.empty())
    snprintf(query, sizeof(query), "SELECT * FROM financial_management_orders WHERE project_id=%ld ORDER BY id %s LIMIT %d,%d", projectid, dir, offset, count);
  else
    snprintf(query, sizeof(query), "SELECT * FROM financial_management_orders WHERE project_id=%ld AND (%s) ORDER BY id %s LIMIT %d,%d", projectid, status.c_str(), dir, offset, count);
  
  do_select(query, _orderKeys, &vv);

  for(auto & a : vv)
    v.push_back(Order::create(a));

  return v;
}


bool RemoteDB::do_connect()
{
  auto ip = Config::instance()->get("remotedb_ip");
  auto usr = Config::instance()->get("remotedb_usr");
  auto pwd = Config::instance()->get("remotedb_pwd");
  auto dbname = Config::instance()->get("remotedb_dbname");

  //LOG(INFO, "start to connect mariadb...");
  
  //auto mysql = mysql_real_connect(_mysql, "47.106.208.207", "root", "1234qwer", "test", 0, NULL, 0);
  auto mysql = mysql_real_connect(_mysql, ip.c_str(), usr.c_str(), pwd.c_str(), dbname.c_str(), 0, NULL, 0);
  if(NULL == mysql)
    {
      print_mysql_error();
      return false;
    }

  char value = 1;
  mysql_options(_mysql, MYSQL_OPT_RECONNECT, (char *)&value);
  
  //LOG(INFO, "connect mariadb success...");
  
  _connected = true;
    
  return true;
}
  
int RemoteDB::do_query(const char* query, std::vector<std::vector<std::string>>* vv)
{
  return do_query_impl(_mysql, query, vv);
}

int RemoteDB::do_query_impl(MYSQL* mysql, const char* query, std::vector<std::vector<std::string>>* vv)
{
  //raii r1("do_query_impl");

  auto ret = mysql_real_query(mysql, query, strlen(query));
  if(ret != 0)
    {
      LOG(WARNING, "do_query query:\n%s", query);
      print_mysql_error_impl(mysql);
      return ERROR_MYSQL_REAL_QUERY_FAILED;
    }
    
  if(vv != NULL)
    {
      vv->clear();
	
      auto result= mysql_store_result(mysql);
      if(result == NULL)
	{
	  print_mysql_error_impl(mysql);
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
  return do_select_impl(_mysql, query, keys, vv);
}

int RemoteDB::do_select_impl(MYSQL* mysql, const char* query, std::vector<std::string> keys, std::vector<std::map<std::string, std::string>>* vv)
{
  if(mysql_real_query(mysql, query, strlen(query)))
    {
      print_mysql_error_impl(mysql);
      LOG(WARNING, "do_select query:\n%s", query);
      return ERROR_MYSQL_REAL_QUERY_FAILED;
    }
    
  if(vv != NULL)
    {
      vv->clear();
	
      auto result= mysql_store_result(mysql);
      if(result == NULL)
	{
	  print_mysql_error_impl(mysql);
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

std::map<std::string, std::string> RemoteDB::get_last_record(const char* table, const std::vector<std::string> & keys)
{
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM %s ORDER BY id DESC LIMIT 0, 1", table);
  
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
  snprintf(query, sizeof(query), "SELECT * FROM %s WHERE id>=%ld AND id<%ld", table, start, start+cnt);
  
  std::vector<std::map<std::string, std::string>> vv;
  
  auto ret = do_select(query, keys, &vv);
  
  return vv;
}

  
