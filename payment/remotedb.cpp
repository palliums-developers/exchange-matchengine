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
#include "payment.h"

//---------------------------------------------------------------------------------------------------------------

std::string RemoteDBBase::escape_string(std::string str)
{
  char buf[256];
  memset(buf, 0, sizeof(buf));
  mysql_real_escape_string(_mysql, buf, str.c_str(), str.length());
  return std::string(buf);
}

void RemoteDBBase::zip_map(const std::map<std::string, std::string> & v, std::string & keys, std::string & vals)
{
  std::vector<std::string> ks;
  std::vector<std::string> vs;
  for(auto & a : v)
    {
      ks.push_back("`" + a.first + "`");
      vs.push_back("'" + escape_string(a.second) + "'");
    }
  keys = string_join(ks, ",");
  vals = string_join(vs, ",");
}

std::string RemoteDBBase::join_map(const std::map<std::string, std::string> & v)
{
  std::vector<std::string> kvs;
  for(auto & a : v)
    {
      auto key = "`" + a.first + "`";
      auto value = "'" + escape_string(a.second) + "'";
      kvs.push_back(key+"="+value);
    }
  return string_join(kvs, ",");
}

void RemoteDBBase::free_result()
{
  do
    {
      MYSQL_RES* res = mysql_store_result(_mysql);
      mysql_free_result(res);
    }
  while ( (0 == mysql_next_result(_mysql)) );
}

bool RemoteDBBase::do_connect()
{
  auto ip = Config::instance()->get("remotedb_ip");
  auto usr = Config::instance()->get("remotedb_usr");
  auto pwd = Config::instance()->get("remotedb_pwd");
  auto dbname = Config::instance()->get("remotedb_dbname");

  //LOG(INFO, "start to connect mariadb...");
  
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

  auto ret = mysql_autocommit(_mysql, 0);
  if(ret != 0)
    {
      LOG(WARNING, "mysql_autocommit failed");
      print_mysql_error();
      return false;
    }
  
  //free_result();
  
  return true;
}
  
int RemoteDBBase::do_query(const char* query, std::vector<std::vector<std::string>>* vv, bool do_commit = true)
{
  auto mysql = _mysql;
  
  auto ret = mysql_real_query(mysql, query, strlen(query));
  if(ret != 0)
    {
      LOG(WARNING, "do_query query:\n%s", query);
      print_mysql_error();
      if(do_commit)
	mysql_rollback(mysql);
      return ERROR_MYSQL_REAL_QUERY_FAILED;
    }

  if(vv != NULL)
    {
      vv->clear();
	
      auto result= mysql_store_result(mysql);
      if(result == NULL)
	{
	  print_mysql_error();
	  if(do_commit)
	    mysql_rollback(mysql);
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

  if(do_commit)
    mysql_commit(mysql);
  
  return 0;
}

int RemoteDBBase::do_select(const char* query, std::vector<std::string> keys, std::vector<std::map<std::string, std::string>>* vv, bool do_commit = true)
{
  auto mysql = _mysql;
  
  if(mysql_real_query(mysql, query, strlen(query)))
    {
      print_mysql_error();
      LOG(WARNING, "do_select query:\n%s", query);
      if(do_commit)
	mysql_rollback(mysql);
      return ERROR_MYSQL_REAL_QUERY_FAILED;
    }

  if(vv != NULL)
    {
      vv->clear();
	
      auto result= mysql_store_result(mysql);
      if(result == NULL)
	{
	  print_mysql_error();
	  if(do_commit)
	    mysql_rollback(mysql);
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

  if(do_commit)
    mysql_commit(mysql);
  
  return 0;
}

long RemoteDBBase::get_rows_count(const char* table)
{
  char query[256];
  snprintf(query, sizeof(query), "SELECT COUNT(*) FROM %s", table);

  std::vector<std::vector<std::string>> vv;
  
  auto ret = do_query(query, &vv);
  
  if(ret != 0 || vv.empty())
    return 0;

  return atol(vv[0][0].c_str());
};

std::map<std::string, std::string>
RemoteDBBase::get_last_record(const char* table, const std::vector<std::string> & keys)
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
RemoteDBBase::get_record_by_limit(const char* table, const std::vector<std::string> & keys, long start, long cnt)
{
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM %s WHERE id>=%ld AND id<%ld", table, start, start+cnt);
  
  std::vector<std::map<std::string, std::string>> vv;
  
  auto ret = do_select(query, keys, &vv);
  
  return vv;
}

//---------------------------------------------------------------------------------------------------------------

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
	auto query = "select COLUMN_NAME from information_schema.COLUMNS where table_name = 'payment_users'";
	if(do_query(query, &vv))
	  return false;
	for(auto & v : vv)
	  _userKeys.push_back(v[0]);
      }
  
      {
	auto query = "select COLUMN_NAME from information_schema.COLUMNS where table_name = 'payment_orders'";
	if(do_query(query, &vv))
	  return false;
	for(auto & v : vv)
	  _orderKeys.push_back(v[0]);
      }
    }

  dot("*");
  
  return true;
}

std::shared_ptr<User> RemoteDB::get_user(long id)
{
  return get_user_impl(id, false, true);
}

std::shared_ptr<User> RemoteDB::get_user_impl(long id, bool forupate, bool do_commit)
{
  // lmf
  forupate = false;
  
  std::vector<std::map<std::string, std::string>> vv;
  char query[256];
  if(forupate)
    snprintf(query, sizeof(query), "SELECT * FROM payment_users WHERE id=%ld FOR UPDATE", id); 
  else
    snprintf(query, sizeof(query), "SELECT * FROM payment_users WHERE id=%ld", id); 
  do_select(query, _userKeys, &vv, do_commit);

  std::shared_ptr<User> o;
  if(!vv.empty())
    o = User::create(vv[0]);
  return o;
}

std::shared_ptr<User> RemoteDB::get_user_by_user_phone_mail(std::string name, std::string value)
{
  return get_user_by_user_phone_mail_impl(name, value, true);
}

std::shared_ptr<User> RemoteDB::get_user_by_user_phone_mail_impl(std::string name, std::string value, bool do_commit)
{
  std::vector<std::map<std::string, std::string>> vv;
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM payment_users WHERE %s='%s'", name.c_str(), value.c_str());
  do_select(query, _userKeys, &vv, do_commit);
  
  std::shared_ptr<User> o;
  if(!vv.empty())
    o = User::create(vv[0]);
  return o;
}

std::shared_ptr<Order> RemoteDB::get_order(long id)
{
  return get_order_impl(id, true);
}

std::shared_ptr<Order> RemoteDB::get_order_impl(long id, bool do_commit)
{
  std::vector<std::map<std::string, std::string>> vv;
  char query[256];
  snprintf(query, sizeof(query), "SELECT * FROM payment_orders WHERE id=%ld", id);
  do_select(query, _orderKeys, &vv, do_commit);

  std::shared_ptr<Order> o;
  if(!vv.empty())
    o = Order::create(vv[0]);
  return o;
}

int RemoteDB::add_user(std::shared_ptr<User> o)
{
  return add_user_impl(o, true);
}

int RemoteDB::add_user_impl(std::shared_ptr<User> o, bool do_commit)
{
  auto v = o->to_map();
  
  std::string keys;
  std::string vals;
  zip_map(v, keys, vals);

  char query[1024];
  snprintf(query, sizeof(query), "INSERT INTO payment_users (%s) VALUES (%s)", keys.c_str(), vals.c_str());
  
  return do_query(query, NULL, do_commit);
}


int RemoteDB::add_order_impl(std::shared_ptr<Order> order, bool do_commit)
{
  auto v = order->to_map();
  
  std::string keys;
  std::string vals;
  zip_map(v, keys, vals);
  
  char query[1024];
  snprintf(query, sizeof(query), "INSERT INTO payment_orders (%s) VALUES (%s)", keys.c_str(), vals.c_str());
  
  return do_query(query, NULL, do_commit);
  
}

int RemoteDB::add_order(std::shared_ptr<Order> order, std::shared_ptr<User> from_user, std::shared_ptr<User> to_user)
{
  auto mysql = _mysql;

  int ret = 0;
  
  char query[1024];

  if(order->_type == ORDER_TYPE_RECHARGE)
    {
      //auto from_user = get_user_impl(order->_from, false, false);

      if(!from_user) {
	mysql_rollback(mysql);
	return ERROR_NOT_EXIST_USER;
      }
      
      ret = add_order_impl(order, false);
      if(ret != 0) {
	mysql_rollback(mysql);
	return ret;
      }

      if(order->_utxo_confirmed) {
	snprintf(query, sizeof(query), "UPDATE payment_users SET balance=balance+%f WHERE id=%ld", order->_amount, from_user->_id);
	ret = do_query(query, NULL, false);
	if(ret != 0) {
	  mysql_rollback(mysql);
	  return ret;
	}
      }
      
      mysql_commit(mysql);      
    }

  if(order->_type == ORDER_TYPE_WITHDRAW)
    {
      //auto from_user = get_user_impl(order->_from, true, false);
      
      auto feestr = Config::instance()->get("min_withdraw_fee");
      double min_withdraw_fee = atof(feestr.c_str());
      
      if(!from_user) {
	mysql_rollback(mysql);
	return ERROR_NOT_EXIST_USER;
      }

      //printf("balance:%f, amount:%f, fee:%f\n", from_user->_balance, order->_amount, order->_withdraw_fee);

      if(double_less(order->_withdraw_fee, min_withdraw_fee)) {
	mysql_rollback(mysql);
	return ERROR_INSUFFICIENT_FEE;
      }
      
      if(double_less(from_user->_balance, (order->_amount + order->_withdraw_fee))) {
	mysql_rollback(mysql);
	return ERROR_INSUFFICIENT_AMOUNT; 
      }

      ret = add_order_impl(order, false);
      if(ret != 0) {
	mysql_rollback(mysql);
	return ret;
      }

      snprintf(query, sizeof(query), "UPDATE payment_users SET balance=balance-%f WHERE id=%ld", order->_amount+order->_withdraw_fee, from_user->_id);
      ret = do_query(query, NULL, false);
      if(ret != 0) {
	mysql_rollback(mysql);
	return ret;
      }
      
      mysql_commit(mysql);      
    }
  
  if(order->_type == ORDER_TYPE_TRANSACTION)
    {
      //auto from_user = get_user_impl(order->_from, true, false);
      //auto to_user = get_user_impl(order->_to, false, false);

      if(!from_user || !to_user) {
	mysql_rollback(mysql);
	return ERROR_NOT_EXIST_USER;
      }
      
      if(double_less(from_user->_balance, order->_amount)) {
	mysql_rollback(mysql);
	return ERROR_INSUFFICIENT_AMOUNT;
      }

      ret = add_order_impl(order, false);
      if(ret != 0) {
      	mysql_rollback(mysql);
      	return ret;
      }

      // snprintf(query, sizeof(query), "UPDATE payment_users SET balance = CASE id WHEN %ld THEN balance-%f WHEN %ld THEN balance+%f END WHERE id IN (%ld,%ld)",
      // 	       from_user->_id, order->_amount, to_user->_id, order->_amount, from_user->_id, to_user->_id);
      // ret = do_query(query, NULL, false);
      // if(ret != 0) {
      // 	mysql_rollback(mysql);
      // 	return ret;
      // }
      
      snprintf(query, sizeof(query), "UPDATE payment_users SET balance=balance-%f WHERE id=%ld", order->_amount, from_user->_id);
      ret = do_query(query, NULL, false);
      if(ret != 0) {
      	mysql_rollback(mysql);
      	return ret;
      }

      snprintf(query, sizeof(query), "UPDATE payment_users SET balance=balance+%f WHERE id=%ld", order->_amount, to_user->_id);
      ret = do_query(query, NULL, false);
      if(ret != 0) {
      	mysql_rollback(mysql);
      	return ret;
      }
 
      mysql_commit(mysql);
    }
  
  return 0;
}

int RemoteDB::update_order(std::shared_ptr<Order> order, std::map<std::string, std::string>& kvs)
{
  auto mysql = _mysql;
  char query[1024];
  int ret;
  
  if(order->_type == ORDER_TYPE_RECHARGE)
    {
      if(kvs["utxo_confirmed"] == "1") {
	auto from_user = get_user_impl(order->_from, false, false);

	if(!from_user) {
	  mysql_rollback(mysql);
	  return ERROR_NOT_EXIST_USER;
	}
	
	snprintf(query, sizeof(query), "UPDATE payment_users SET balance=balance+%f WHERE id=%ld", order->_amount, from_user->_id);
	ret = do_query(query, NULL, false);
	if(ret != 0) {
	  mysql_rollback(mysql);
	  return ret;
	}
	
	mysql_commit(mysql);
	return 0;
      }
    }

  snprintf(query, sizeof(query), "UPDATE payment_orders SET %s WHERE id=%ld", join_map(kvs).c_str(), order->_id);
  ret = do_query(query, NULL, false);
  if(ret != 0) {
    mysql_rollback(mysql);
    return ret;
  }
	
  mysql_commit(mysql);
  return 0;
}

int RemoteDB::update_user(std::shared_ptr<User> user, std::map<std::string, std::string>& kvs)
{
  auto mysql = _mysql;
  char query[1024];
  int ret;
  
  snprintf(query, sizeof(query), "UPDATE payment_users SET %s WHERE id=%ld", join_map(kvs).c_str(), user->_id);
  ret = do_query(query, NULL, false);
  if(ret != 0) {
    mysql_rollback(mysql);
    return ret;
  }
	
  mysql_commit(mysql);
  return 0;
}

long RemoteDB::get_users_cnt()
{
  return get_rows_count("payment_users");
}

long RemoteDB::get_orders_cnt()
{
  return get_rows_count("payment_orders");
}

std::vector<std::shared_ptr<User>>
RemoteDB::get_users_by_limit(long start, long cnt)
{
  std::vector<std::shared_ptr<User>> v;
  auto vv = get_record_by_limit("payment_users", _userKeys, start, cnt);
  for(auto & a : vv)
    v.push_back(User::create(a));
  return v;
}

std::vector<std::shared_ptr<Order>>
RemoteDB::get_orders_by_limit(long start, long cnt)
{
  std::vector<std::shared_ptr<Order>> v;
  auto vv = get_record_by_limit("payment_orders", _orderKeys, start, cnt);
  for(auto & a : vv)
    v.push_back(Order::create(a));
  return v;
}

std::shared_ptr<Order>
RemoteDB::get_last_order()
{
  auto v = get_last_record("payment_orders", _orderKeys);
  if(v.empty())
    return std::shared_ptr<Order>();
  return Order::create(v);
}

std::shared_ptr<User>
RemoteDB::get_last_user()
{
  auto v = get_last_record("payment_users", _userKeys);
  if(v.empty())
    return std::shared_ptr<User>();
  return User::create(v);
}

int RemoteDB::update_order_status(long orderid, int status)
{
  char query[1024];
  snprintf(query, sizeof(query), "UPDATE payment_orders SET status=%d WHERE id=%ld", status, orderid);
  return do_query(query, NULL);
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
      snprintf(query, sizeof(query), "SELECT id FROM payment_orders WHERE user_id = %ld LIMIT %d,%d", userid, idx, cnt);
  
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

std::vector<long> RemoteDB::get_user_history_orders(long userid, long end_orderid)
{
  std::vector<long> v;
  
  std::vector<std::map<std::string, std::string>> vv;
      
  char query[256];
  snprintf(query, sizeof(query), "SELECT id FROM payment_orders WHERE id<%ld AND user_id = %ld", end_orderid, userid);
  
  std::vector<std::string> keys{"id"};
  
  do_select(query, keys, &vv);

  for(auto & a : vv)
    v.push_back(atol(a["id"].c_str()));

  return v;  
}

std::vector<std::shared_ptr<Order>> 
RemoteDB::get_user_orders_limit(std::shared_ptr<User> user, std::vector<int> statuses, int offset, int count, int direction, int start_timestamp, long last_orderid)
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
      status += "type=" + std::to_string(statuses[i]);
    }

  if(statuses.empty())
    snprintf(query, sizeof(query),
	     "SELECT * FROM payment_orders WHERE (from=%ld OR to=%ld) AND timestamp>%d AND id<%ld ORDER BY id %s LIMIT %d,%d",
	     userid, userid, start_timestamp, last_orderid, dir, offset, count);
  else
    snprintf(query, sizeof(query),
	     "SELECT * FROM payment_orders WHERE (from=%ld OR to=%ld) AND timestamp>%d AND id<%ld AND (%s) ORDER BY id %s LIMIT %d,%d",
	     userid, userid, start_timestamp, last_orderid, status.c_str(), dir, offset, count);
  
  do_select(query, _orderKeys, &vv);
  
  for(auto & a : vv)
    v.push_back(Order::create(a));
  
  return v;
}

