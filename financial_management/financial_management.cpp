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
#include <initializer_list>
#include <memory>

#include <unistd.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <chrono>
#include <ctime>
#include <ratio>
#include <unordered_map>

#include <assert.h>
#include <math.h>

using namespace std::chrono_literals;


#include <netinet/in.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <stdio.h>     
#include <stdlib.h>    
#include <string.h>

#include <mariadb/mysql.h>

#include "../utils/utilities.h"

#include "remotedb.h"
#include "financial_management.h"

const char * linesplit = "@#$";

bool double_equal(double a, double b)
{
  return fabs(a-b) < 0.00001;
}

bool double_great(double a, double b)
{
  return a > b && !double_equal(a, b);
}

bool double_less(double a, double b)
{
  return a < b && !double_equal(a, b);
}

bool double_great_equal(double a, double b)
{
  return !double_less(a, b);
}

bool double_less_equal(double a, double b)
{
  return !double_great(a, b);
}

void write_file(const std::string & str)
{
  static auto fh = fopen("a.txt", "a");
  if(fh)
    {
      fwrite(str.c_str(), 1, str.length(), fh);
      fwrite("\n\n", 1, 2, fh);
      fflush(fh);
      //fclose(fh);
    }
}

struct TimeElapsed
{
  TimeElapsed(const char* name)
  {
    start = std::chrono::system_clock::now();
    this->name = name;
  }
  
  ~TimeElapsed()
  {
    auto end   = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    int milliseconds = duration.count()/1000;

    char buf[512];
    sprintf(buf, "%s cost %d milliseconds", name, milliseconds);
    write_file(buf);

    if(milliseconds >= 100)
      LOG(WARNING, buf);
  }
  
  std::chrono::system_clock::time_point start;
  const char* name;
};

std::string json(const char* name, const std::string & value, bool last)
{
  std::string str;
  str = str + "\"" + name + "\":\"" + value + "\"";
  if(!last) str += ",";
  return str;
}
  
template<class T>
std::string json(const char* name, const T & value, bool last)
{
  std::string str;
  str = str + "\"" + name + "\":" + std::to_string(value);
  if(!last) str += ",";
  return str;
}

int extract_request(const std::string & req, std::string & command, std::map<std::string, std::string> & paras, int & msn)
{
  auto v = json_get_object(req);

  if(0 == v.count("command"))
    return 1;
  command = v["command"];

  if(0 == v.count("seq"))
    return 2;
  msn = atoi(v["seq"].c_str());

  if(0 == v.count("paras"))
    return 3;

  paras = json_get_object(v["paras"]);
  
  if(paras.empty())
    return 4;
  
  return 0;
}

std::string gen_rsp(const std::string & command, int msn, int ret, const std::vector<std::string> & v)
{
  char str[8192];
  auto data = string_join(v, ",");
  snprintf(str, sizeof(str), "{\"command\":\"%s\", \"status\":%d, \"seq\":%d, \"data\":[%s]}", command.c_str(), ret, msn, data.c_str());
  return str;
}

std::shared_ptr<Project> Project::create(std::map<std::string, std::string> & kvs)
{
  auto o = std::make_shared<Project>();
    
  if(kvs.count("id"))
    o->_id = atol(kvs["id"].c_str());
    
  if(kvs.count("no"))
    o->_no = kvs["no"];
  if(kvs.count("name"))
    o->_name = kvs["name"];
    
  if(kvs.count("annualized_return_rate"))
    o->_annualized_return_rate = atof(kvs["annualized_return_rate"].c_str());
  if(kvs.count("investment_period"))
    o->_investment_period = atoi(kvs["investment_period"].c_str());

  if(kvs.count("min_invest_amount"))
    o->_min_invest_amount = atof(kvs["min_invest_amount"].c_str());
  if(kvs.count("max_invest_amount"))
    o->_max_invest_amount = atof(kvs["max_invest_amount"].c_str());
  if(kvs.count("total_crowdfunding_amount"))
    o->_total_crowdfunding_amount = atof(kvs["total_crowdfunding_amount"].c_str());
    
  if(kvs.count("booking_starting_time"))
    o->_booking_starting_time = atoi(kvs["booking_starting_time"].c_str());
  if(kvs.count("crowdfunding_ending_time"))
    o->_crowdfunding_ending_time = atoi(kvs["crowdfunding_ending_time"].c_str());

  if(kvs.count("interest_type"))
    o->_interest_type = atoi(kvs["interest_type"].c_str());
  if(kvs.count("borrower_info"))
    o->_borrower_info = atoi(kvs["borrower_info"].c_str());

  if(kvs.count("crowdfunding_address"))
    o->_crowdfunding_address = kvs["crowdfunding_address"];
  if(kvs.count("product_description"))
    o->_product_description = kvs["product_description"];

  if(kvs.count("status"))
    o->_status = atoi(kvs["status"].c_str());
  
  return o;
}

int Project::check_valid()
{
  if(_no.empty())
    return ERROR_INVALID_PROJECT_NO;
  if(_annualized_return_rate <= 0)
    return ERROR_INVALID_ANNUALIZED_RETURN_RATE;
  if(_investment_period <=0 || _investment_period > 365*10)
    return ERROR_INVALID_INVESTMENT_PERIOD;
  if(_min_invest_amount <= 0)
    return ERROR_INVALID_MIN_INVEST_AMOUNT;
  if(_max_invest_amount <= 0)
    return ERROR_INVALID_MAX_INVEST_AMOUNT;
  if(_total_crowdfunding_amount <= 0)
    return ERROR_INVALID_TOTAL_CROWDFUNDING_AMOUNT;
  if(_booking_starting_time <= 0)
    return ERROR_INVALID_BOOKING_STARTING_TIME;
  if(_crowdfunding_ending_time <= 0)
    return ERROR_INVALID_CROWDFUNDING_ENDING_TIME;
  if(_interest_type < 0 || _interest_type >= INTEREST_TYPE_CNT)
    return ERROR_INVALID_INTEREST_TYPE;
  if(_borrower_info < 0 || _borrower_info >= BORROWER_INFO_CNT)
    return ERROR_INVALID_BORROWER_INFO;
  if(_crowdfunding_address.empty())
    return ERROR_INVALID_CROWDFUNDING_ADDRESS;
  
  return 0;
}

std::map<std::string, std::string> Project::to_map()
{
  std::map<std::string, std::string> v;
  v["id"] = std::to_string(_id);
  v["no"] = _no;
  v["name"] = _name;
  v["annualized_return_rate"] = std::to_string(_annualized_return_rate);
  v["investment_period"] = std::to_string(_investment_period);
  v["min_invest_amount"] = std::to_string(_min_invest_amount);
  v["max_invest_amount"] = std::to_string(_max_invest_amount);
  v["total_crowdfunding_amount"] = std::to_string(_total_crowdfunding_amount);
  v["booking_starting_time"] = std::to_string(_booking_starting_time);
  v["crowdfunding_ending_time"] = std::to_string(_crowdfunding_ending_time);
  v["status"] = std::to_string(_status);
  v["received_crowdfunding"] = std::to_string(_received_crowdfunding);
  v["interest_type"] = std::to_string(_interest_type);
  v["borrower_info"] = std::to_string(_borrower_info);
  v["crowdfunding_address"] = _crowdfunding_address;
  v["product_description"] = _product_description;
  return v;
}

  
std::string Project::to_json()
{
  std::string str = "{";
  str += json("id", _id, false);
  str += json("no", _no, false);
  str += json("name", _name, false);
  str += json("annualized_return_rate", _annualized_return_rate, false);
  str += json("investment_period", _investment_period, false);
  str += json("min_invest_amount", _min_invest_amount, false);
  str += json("max_invest_amount", _max_invest_amount, false);
  str += json("total_crowdfunding_amount", _total_crowdfunding_amount, false);
  str += json("booking_starting_time", _booking_starting_time, false);
  str += json("crowdfunding_ending_time", _crowdfunding_ending_time, false);
  str += json("status", _status, false);
  str += json("received_crowdfunding", _received_crowdfunding, false);
  str += json("interest_type", _interest_type, false);
  str += json("borrower_info", _borrower_info, false);
  str += json("crowdfunding_address", _crowdfunding_address, false);
  str += json("product_description", _product_description, true);
  str += "}";
  return str;
}


std::shared_ptr<Order> Order::create(std::map<std::string, std::string> & kvs)
{
  auto o = std::make_shared<Order>();
    
  if(kvs.count("id"))
    o->_id = atol(kvs["id"].c_str());

  if(kvs.count("project_id"))
    o->_project_id = atol(kvs["project_id"].c_str());
  if(kvs.count("user_id"))
    o->_user_id = atol(kvs["user_id"].c_str());
  if(kvs.count("booking_amount"))
    o->_booking_amount = atof(kvs["booking_amount"].c_str());
  if(kvs.count("investment_return_addr"))
    o->_investment_return_addr = kvs["investment_return_addr"];

  if(kvs.count("payment_txid"))
    o->_payment_txid = kvs["payment_txid"];
  if(kvs.count("accumulated_gain"))
    o->_accumulated_gain = atof(kvs["accumulated_gain"].c_str());
  if(kvs.count("status"))
    o->_status = atoi(kvs["status"].c_str());

  if(kvs.count("booking_timestamp"))
    o->_booking_timestamp = atoi(kvs["booking_timestamp"].c_str());
  else
    o->_booking_timestamp = (int)uint32_t(time(NULL));
  
  return o;
}

int Order::check_valid()
{
  if(_project_id < 0)
    return ERROR_INVALID_PROJECT_ID;
  if(_user_id < 0)
    return ERROR_INVALID_USER_ID;
  if(_booking_amount <= 0)
    return ERROR_INVALID_BOOKING_AMOUNT;
  
  return 0;
}

std::map<std::string, std::string> Order::to_map()
{
  std::map<std::string, std::string> v;
  v["id"] = std::to_string(_id);
  v["project_id"] = std::to_string(_project_id);
  v["user_id"] = std::to_string(_user_id);
  v["booking_timestamp"] = std::to_string(_booking_timestamp);
  v["booking_amount"] = std::to_string(_booking_amount);
  v["payment_txid"] = _payment_txid;
  v["payment_timestamp"] = std::to_string(_payment_timestamp);
  v["audited_timestamp"] = std::to_string(_audited_timestamp);
  v["investment_return_addr"] = _investment_return_addr;
  v["accumulated_gain"] = std::to_string(_accumulated_gain);
  v["status"] = std::to_string(_status);
  return v;
}

std::string Order::to_json()
{
  std::string str = "{";
  str += json("id", _id, false);
  str += json("project_id", _project_id, false);
  str += json("user_id", _user_id, false);
  str += json("booking_timestamp", _booking_timestamp, false);
  str += json("booking_amount", _booking_amount, false);
  str += json("payment_txid", _payment_txid, false);
  str += json("payment_timestamp", _payment_timestamp, false);
  str += json("audited_timestamp", _audited_timestamp, false);
  str += json("investment_return_addr", _investment_return_addr, false);
  str += json("accumulated_gain", _accumulated_gain, false);
  str += json("project_no", _project_no, false);
  str += json("user_publickey", _user_publickey, false);
  str += json("status", _status, true);
  str += "}";
  return str;
}


std::shared_ptr<User> User::create(std::map<std::string, std::string> & kvs)
{
  auto o = std::make_shared<User>();
    
  if(kvs.count("id"))
    o->_id = atol(kvs["id"].c_str());

  if(kvs.count("publickey"))
    o->_publickey = kvs["publickey"];
  if(kvs.count("name"))
    o->_name = kvs["name"];
  if(kvs.count("collections"))
    o->_collections = kvs["collections"];
    
  return o;
}

int User::check_valid()
{
  if(_publickey.empty())
    return ERROR_INVALID_PUBLICKEY;
    
  return 0;
}

std::map<std::string, std::string> User::to_map()
{
  std::map<std::string, std::string> v;
  v["id"] = std::to_string(_id);
  v["publickey"] = _publickey;
  v["name"] = _name;
  v["collections"] = _collections;
  return v;
}

std::string User::to_json()
{
  std::string str = "{";
  str += json("id", _id, false);
  str += json("publickey", _publickey, false);
  str += json("name", _name, false);
  str += json("collections", _collections, true);
  str += "}";
  return str;
}

int FinancialManagement::add_project(std::shared_ptr<Project> project)
{
  if(project->_id != _projectcnt)
    return ERROR_ADD_EXISTING_PROJECT;
  
  long id = _projectcnt;
  long ida = _projectcnt % maxcnt;

  project->_id = id;
    
  auto no = project->_no;
  
  if(_cache_project_no2id.count(no))
    return ERROR_ADD_EXISTING_PROJECT;

  /* ret = _remotedb.add_project(id, kvs); */
  /* if(ret != 0) */
  /*   return ret; */
    
  _cache_project_no2id[no] = id;

  if(_projects[ida])
    _cache_project_no2id.erase(_projects[ida]->_no);
    
  _projects[ida] = project;

  _projectcnt++;
  return 0;
}
  
int FinancialManagement::add_user(std::string publickey)
{
  if(_cache_user_publickey2id.count(publickey))
    return 0;
    
  long id = _usercnt;
  long ida = _usercnt % maxcnt;
    
  std::map<std::string, std::string> kvs;
    
  kvs["id"] = std::to_string(id);
  kvs["publickey"] = publickey;
    
  auto user = User::create(kvs);
    
  auto ret = _remotedb->add_user(user);
  if(ret != 0)
    return ret;

  _cache_user_publickey2id[publickey] = id;

  if(_users[ida])
    _cache_user_publickey2id.erase(_users[ida]->_publickey);
  _users[ida] = user;
    
  _usercnt++;
  return 0;    
}
  
int FinancialManagement::add_order(std::map<std::string, std::string> & kvs)
{
  auto now = (int)uint32_t(time(NULL));

  long id = _ordercnt;
  long ida = _ordercnt % maxcnt;

  kvs["id"] = std::to_string(id);

  auto order = Order::create(kvs);
    
  auto no = kvs["project_no"];
  auto publickey = kvs["user_publickey"];
  double amount = atof(kvs["booking_amount"].c_str());

  auto project = get_project_by_no(no);
  if(!project)
    return ERROR_NOT_EXIST_PROJECT_NO;
  
  if(project->_status == PROJECT_STATUS_CROWDFUND_SUCCESS)
    return ERROR_ADD_ORDER_CROWDFUNDING_FULL;
  
  if(double_great_equal(project->_received_crowdfunding, project->_total_crowdfunding_amount))
    return ERROR_ADD_ORDER_CROWDFUNDING_FULL;
    
  if(now < project->_booking_starting_time)
    return ERROR_ADD_ORDER_CROWDFUNDING_NOT_START;
  
  if(now > (project->_crowdfunding_ending_time - 7200))
    return ERROR_ADD_ORDER_CROWDFUNDING_FINISHED;

  if(double_less(amount, project->_min_invest_amount))
    return ERROR_ADD_ORDER_TOO_LOW_AMOUNT;
  
  if(double_great(amount, project->_max_invest_amount))
    return ERROR_ADD_ORDER_TOO_HIGH_AMOUNT;

  if(double_great(amount, (project->_total_crowdfunding_amount - project->_booked_crowdfunding)))
    return ERROR_ADD_ORDER_CROWDFUNDING_BOOK_FULL;

  auto user = get_user_by_publickey(publickey);
  if(!user)
    {
      add_user(publickey);
      user = get_user_by_publickey(publickey);
    }
  
  order->_project_id = project->_id;
  order->_user_id = user->_id;
  order->_project_no = no;
  order->_user_publickey = publickey;
  
  auto ret = _remotedb->add_order(order);
  if(ret != 0)
    return ret;

  user->_orders.push_back(id);
  project->_orders.push_back(id);
  
  _orders[ida] = order;

  project->_booked_crowdfunding += amount;
  
  _order_txid_timeout_heap.emplace(order->_booking_timestamp, order);
  
  _ordercnt++;
  
  return 0;    
}

int FinancialManagement::check_order(std::shared_ptr<Order> order, std::string project_no, std::string publickey)
{
  if(!order)
    return ERROR_NOT_EXIST_ORDER;
    
  auto project_id = order->_project_id;
  auto user_id = order->_user_id;

  auto project = get_project_by_no(project_no);
  if(!project)
    return ERROR_NOT_EXIST_PROJECT_NO;
    
  auto user = get_user_by_publickey(publickey);
  if(!user)
    return ERROR_NOT_EXIST_USER_PUBLICKEY;

  if(order->_user_id != user->_id)
    return ERROR_MISMATCH_ORDERID_USER_PUBLICKEY;

  if(order->_project_id != project->_id)
    return ERROR_MISMATCH_ORDERID_PROJECT_ID;

  return 0;
}
  
int FinancialManagement::cancel_order(std::string project_no, std::string publickey, long orderid)
{
  add_user(publickey);
    
  auto order = get_order(orderid);
    
  auto ret = check_order(order, project_no, publickey);
  if(0 != ret)
    return ret;

  if(order->_status == ORDER_STATUS_BOOKING_TIMEOUT || order->_status == ORDER_STATUS_AUDIT_TIMEOUT)
    return ERROR_ORDER_TIMEOUT;
    
  if(order->_status != ORDER_STATUS_BOOKING_SUCCESS)
    return ERROR_CANNOT_CANCEL_PAYED_ORDER;

  auto project = get_project(order->_project_id);
  if(!project)
    return ERROR_NOT_EXIST_PROJECT_NO;
    
  ret = _remotedb->update_order_status(orderid, ORDER_STATUS_CANCELED);
  if(ret != 0)
    return ret;

  project->_booked_crowdfunding -= order->_booking_amount;
    
  order->_status = ORDER_STATUS_CANCELED;
    
  return 0;
}

int FinancialManagement::update_order_txid(long orderid, std::string project_no, std::string publickey, std::string txid, std::string investment_return_addr, int payment_timestamp)
{
  auto order = get_order(orderid);
    
  auto ret = check_order(order, project_no, publickey);
  if(0 != ret)
    return ret;

  if(order->_status == ORDER_STATUS_BOOKING_TIMEOUT || order->_status == ORDER_STATUS_AUDIT_TIMEOUT)
    return ERROR_ORDER_TIMEOUT;
    
  if(order->_status != ORDER_STATUS_BOOKING_SUCCESS)
    return ERROR_DUPLICATE_SET_TXID;
    
  ret = _remotedb->update_order_txid(orderid, txid, investment_return_addr, payment_timestamp);
  if(ret != 0)
    return ret;

  _remotedb->update_order_status(orderid, ORDER_STATUS_PAYED_AUDITING);
  
  order->_payment_txid = txid;
  order->_status = ORDER_STATUS_PAYED_AUDITING;
  order->_investment_return_addr = investment_return_addr;
  order->_payment_timestamp = payment_timestamp;

  _order_confirm_timeout_heap.emplace(order->_booking_timestamp, order);

  //lmf test
  if(orderid % 2 == 0)
    {
      order_txid_confirm(orderid, txid, "true");
    }
  
  return 0;
}

int FinancialManagement::order_txid_confirm(long orderid, std::string txid, std::string success)
{
  auto order = get_order(orderid);
  if(!order)
    return ERROR_NOT_EXIST_ORDER;

  auto project = get_project(order->_project_id);
  if(!project)
    return ERROR_NOT_EXIST_PROJECT_NO;

  bool timeout = (order->_status == ORDER_STATUS_BOOKING_TIMEOUT || order->_status == ORDER_STATUS_AUDIT_TIMEOUT);
    
  if(order->_status != ORDER_STATUS_PAYED_AUDITING && !timeout)
    return ERROR_CONFIRM_NOT_PAYED_TX;

  if(order->_payment_txid != txid)
    return ERROR_MISMATCH_TXID;

  auto status = order->_status;

  auto now = (int)uint32_t(time(NULL));
  order->_audited_timestamp = now;
  
  if(success == "true")
    {
      if((now - order->_booking_timestamp) > 7200)
	status = ORDER_STATUS_AUDIT_SUCCESS_TOO_LATE;
      else
	status = ORDER_STATUS_AUDIT_SUCCESS;
    }
  else
    status = ORDER_STATUS_AUDIT_FAILURE;

  if(status == ORDER_STATUS_AUDIT_SUCCESS)
    project->_received_crowdfunding += order->_booking_amount;
  else
    project->_booked_crowdfunding -= order->_booking_amount;
    
  auto ret = _remotedb->update_order_status(orderid, status);
  if(ret != 0)
    return ret;

  _remotedb->update_order_confirm_timestamp(orderid, now);
  
  order->_status = status;
  return 0;
}

int FinancialManagement::payment_refund_success(long orderid, std::string txid)
{
  auto order = get_order(orderid);
  if(!order)
    return ERROR_NOT_EXIST_ORDER;
    
  if(order->_status != ORDER_STATUS_AUDIT_SUCCESS)
    return ERROR_REFUND_UNCONFIRMED_TX;

  if(order->_payment_txid != txid)
    return ERROR_MISMATCH_TXID;
    
  auto ret = _remotedb->update_order_status(orderid, ORDER_STATUS_PAYMENT_REFUNDED);
  if(ret != 0)
    return ret;
    
  order->_status = ORDER_STATUS_PAYMENT_REFUNDED;
    
  return 0;
}

int FinancialManagement::collect(std::string publickey, std::string project_no)
{
  add_user(publickey);
    
  auto user = get_user_by_publickey(publickey);
  if(!user)
    return ERROR_NOT_EXIST_USER_PUBLICKEY;

  auto project = get_project_by_no(project_no);
  if(!project)
    return ERROR_NOT_EXIST_PROJECT_NO;

  auto collections = user->_collections;
  auto v = string_split(collections, ";");
  for(auto a : v)
    if(a == std::to_string(project->_id))
      return 0;
    
  if(!collections.empty())
    collections += ";";
  collections += std::to_string(project->_id);
    
  auto ret = _remotedb->update_collections(user->_id, collections);
  if(ret != 0)
    return ret;

  user->_collections = collections;
  return 0;
}

int FinancialManagement::cancel_collection(std::string publickey, std::string project_no)
{
  auto user = get_user_by_publickey(publickey);
  if(!user)
    return ERROR_NOT_EXIST_USER_PUBLICKEY;
    
  auto project = get_project_by_no(project_no);
  if(!project)
    return ERROR_NOT_EXIST_PROJECT_NO;

  std::string collections;
  auto v = string_split(user->_collections, ";");
  for(auto a : v)
    {
      if(a != std::to_string(project->_id))
	{
	  if(!collections.empty())
	    collections += ";";
	  collections += a;
	}
    }
    
  auto ret = _remotedb->update_collections(user->_id, collections);
  if(ret != 0)
    return ret;

  user->_collections = collections;
  return 0;
}
  
int FinancialManagement::get_user_collections(std::string publickey, std::string & collections)
{
  add_user(publickey);
    
  auto user = get_user_by_publickey(publickey);
  if(!user)
    return ERROR_NOT_EXIST_USER_PUBLICKEY;
    
  auto v = string_split(user->_collections, ";");
    
  for(auto project_id : v)
    {
      auto project = get_project(atol(project_id.c_str()));
      if(project)
	{
	  if(!collections.empty())
	    collections += ";";
	  collections += project->_no;
	}
    }
    
  return 0;
}
  
int FinancialManagement::get_orders_by_user_publickey(std::string publickey, std::vector<std::shared_ptr<Order>> & orders)
{
  auto user = get_user_by_publickey(publickey);
  if(!user)
    return 0;
    
  for(auto orderid : user->_orders)
    {
      auto order = get_order(orderid);
      if(order)
	orders.push_back(order);
    }
  return 0;
}

std::shared_ptr<User> FinancialManagement::get_user_by_publickey(std::string publickey)
{
  if(_cache_user_publickey2id.count(publickey))
    return get_user(_cache_user_publickey2id[publickey]);
  return _remotedb->get_user_by_publickey(publickey);
}
  
std::shared_ptr<Project> FinancialManagement::get_project_by_no(std::string no)
{
  if(_cache_project_no2id.count(no))
    return get_project(_cache_project_no2id[no]);
  return _remotedb->get_project_by_no(no);
}
  
std::shared_ptr<Project> FinancialManagement::get_project(long id)
{
  if(_projects[id%maxcnt] && _projects[id%maxcnt]->_id == id)
    return _projects[id%maxcnt];
  return _remotedb->get_project(id);
}

std::shared_ptr<User> FinancialManagement::get_user(long id)
{
  if(_users[id%maxcnt] && _users[id%maxcnt]->_id == id)
    return _users[id%maxcnt];
  return _remotedb->get_user(id);
}

std::shared_ptr<Order> FinancialManagement::get_order(long id)
{
  if(_orders[id%maxcnt] && _orders[id%maxcnt]->_id == id)
    return _orders[id%maxcnt];
  return _remotedb->get_order(id);
}
  
void FinancialManagement::start()
{
  static bool started = false;
  if(started)
    return;
  started = true;
    
  static volatile bool alive = true;
  std::thread thrd(&FinancialManagement::run, this, &alive);
  thrd.detach();
}

void FinancialManagement::load_project(std::shared_ptr<Project> a)
{
  if(!a)
    return;
  _projects[a->_id%maxcnt] = a;
  _cache_project_no2id[a->_no] = a->_id;
}

void FinancialManagement::load_user(std::shared_ptr<User> a)
{
  if(!a)
    return;
  _users[a->_id%maxcnt] = a;
  _cache_user_publickey2id[a->_publickey] = a->_id;
}

void FinancialManagement::load_order(std::shared_ptr<Order> a)
{
  if(!a)
    return;

  _orders[a->_id%maxcnt] = a;
  
  auto user = _users[a->_user_id%maxcnt];
  if(user && user->_id == a->_user_id)
    {
      user->_orders.push_back(a->_id);
      a->_user_publickey = user->_publickey;
    }
  
  auto project = _projects[a->_project_id%maxcnt];
  if(project && project->_id == a->_project_id)
    {
      project->_orders.push_back(a->_id);
      a->_project_no = project->_no;
    }

  if(a->_status == ORDER_STATUS_BOOKING_SUCCESS)
    _order_txid_timeout_heap.emplace(a->_booking_timestamp, a);
  if(a->_status == ORDER_STATUS_PAYED_AUDITING)
    _order_confirm_timeout_heap.emplace(a->_booking_timestamp, a);

  if(project && project->_status == PROJECT_STATUS_CROWDFUNDING)
    {
      switch(a->_status)
	{
	case ORDER_STATUS_BOOKING_SUCCESS:
	case ORDER_STATUS_PAYED_AUDITING:
	  project->_booked_crowdfunding += a->_booking_amount;
	  break;
	case ORDER_STATUS_AUDIT_SUCCESS:
	  project->_received_crowdfunding += a->_booking_amount;
	  break;
	default:
	  project->_booked_crowdfunding -= a->_booking_amount;
	}
    }
}

bool FinancialManagement::load()
{
  TimeElapsed te("load db");
  
  long onetime_to_load = 1000;
    
  auto projectcnt = _remotedb->get_projects_cnt();
  auto ordercnt = _remotedb->get_orders_cnt();
  auto usercnt = _remotedb->get_users_cnt();
    
  {
    auto & acnt = projectcnt;
    std::shared_ptr<Project> last;
    
    long i = 0;
    long cnt = 0;
    while(cnt < acnt)
      {
	auto v = _remotedb->get_projects_by_limit(i*onetime_to_load, onetime_to_load);
	for(auto & a : v)
	  {
	    load_project(a);
	    cnt++;
	    last = a;
	  }
	i++;
      }
    if(last)
      _projectcnt = last->_id+1;
  }

  {
    auto & acnt = usercnt;
    std::shared_ptr<User> last;

    long i = 0;
    long cnt = 0;
    while(cnt < acnt)
      {
	auto v = _remotedb->get_users_by_limit(i*onetime_to_load, onetime_to_load);
	for(auto & a : v)
	  {
	    load_user(a);
	    cnt++;
	    last = a;
	  }
	i++;
      }
    
    if(last)
      _usercnt = last->_id+1;
  }

  {
    auto & acnt = ordercnt;
    std::shared_ptr<Order> last;

    long i = 0;
    long cnt = 0;
    while(cnt < acnt)
      {
	auto v = _remotedb->get_orders_by_limit(i*onetime_to_load, onetime_to_load);
	for(auto & a : v)
	  {
	    load_order(a);
	    cnt++;
	    last = a;
	  }
	i++;
      }
    
    if(last)
      _ordercnt = last->_id+1;
  }

  LOG(INFO, "load success: %ld, %ld, %ld", _projectcnt, _ordercnt, _usercnt);

  return true;
}

void FinancialManagement::start_epoll_server()
{
  std::thread thrd([this]{

      auto qreqmsg = &_qreqmsg;
      
      auto creater = [&](int fd)
	{
	  LOG(INFO, "a client %d is comming....", fd);
	  auto client = std::make_shared<Client>(fd);
	  client->set_qmsg(qreqmsg);
	  return client;
	};
      
      SocketHelper::epoll_loop(60001, 1024, creater);

      LOG(WARNING, "epoll loop exited!!!");
      
    });
  
  thrd.detach();

  std::thread thrdsend([this]{
      
      for(;;)
	{
	  dot("*");
	  
	  auto msg = _qrspmsg.pop();
	  auto rsp = msg->data() + linesplit;
	  auto fd = msg->client()->get_fd();

	  if(fd <= 0)
	    continue;
	  if(msg->client()->send_closed())
	    continue;
	  
	  auto cnt = send(fd, rsp.c_str(), rsp.length(),0);
	  
	  if(cnt != rsp.length())
	    {
	      close(fd);
	      msg->client()->set_send_closed();
	      LOG(WARNING, "send to client failed, will close the socket...");
	      continue;
	    }
	}
    });
  
  thrdsend.detach();  
}

#define TIMER(n) do{ static int last = 0; if(now-last < (n)) return; last = now; } while(0)

void FinancialManagement::print_status(int now)
{
  TIMER(3);
  
  char buf[512];
  sprintf(buf, "qreq:%d, qrsp:%d", _qreqmsg.size(), _qrspmsg.size());
  write_file(buf);
}

void FinancialManagement::handle_order_heap(int now)
{
  TIMER(30);

  dot("-");

  TimeElapsed te("handle_order_heap");
    
  std::multimap<int, std::shared_ptr<Order>> * heap;

  heap = &_order_txid_timeout_heap;
  
  for(;;)
    {
      if(heap->empty())
	break;
	
      auto iter = heap->begin();
      auto order = iter->second;
      auto booktime = order->_booking_timestamp;
	
      bool waiting = (order->_status == ORDER_STATUS_BOOKING_SUCCESS);
      auto timepassed = now - booktime;
	
      if(!waiting)
	{
	  heap->erase(iter);
	  continue;
	}
	
      if(timepassed < 1800)
	break;

      order->_status = ORDER_STATUS_BOOKING_TIMEOUT;
      _remotedb->update_order_status(order->_id, ORDER_STATUS_BOOKING_TIMEOUT);
      
      auto project = get_project(order->_project_id);
      if(project)
	project->_booked_crowdfunding -= order->_booking_amount;
      
      heap->erase(iter);
    }

  heap = &_order_confirm_timeout_heap;
  
  for(;;)
    {
      if(heap->empty())
	break;
	
      auto iter = heap->begin();
      auto order = iter->second;
      auto booktime = order->_booking_timestamp;
	
      bool waiting = (order->_status == ORDER_STATUS_PAYED_AUDITING);
      auto timepassed = now - booktime;
	
      if(!waiting)
	{
	  heap->erase(iter);
	  continue;
	}
	
      if(timepassed < 7200)
	break;

      order->_status = ORDER_STATUS_AUDIT_TIMEOUT;
      _remotedb->update_order_status(order->_id, ORDER_STATUS_AUDIT_TIMEOUT);
	
      auto project = get_project(order->_project_id);
      if(project)
	project->_booked_crowdfunding -= order->_booking_amount;
	
      heap->erase(iter);
    }
  
}

void FinancialManagement::update_project_status(int now)
{
  TIMER(30);

  TimeElapsed te("update_project_status");
  
  for(long i=_projectcnt-1; i>=0; --i)
    {
      auto project = _projects[i%maxcnt];
      if(!project)
	break;
      
      auto oldstatus = project->_status;
      
      if(project->_status == PROJECT_STATUS_NOT_OPENED)
	{
	  if(now > project->_booking_starting_time)
	    project->_status = PROJECT_STATUS_CROWDFUNDING;
	}
      else if(project->_status == PROJECT_STATUS_CROWDFUNDING)
	{
	  if(now > project->_crowdfunding_ending_time)
	    project->_status = PROJECT_STATUS_CROWDFUND_FAILURE;
	  else if(double_great_equal(project->_received_crowdfunding, project->_total_crowdfunding_amount))
	    project->_status = PROJECT_STATUS_CROWDFUND_SUCCESS;
	  else
	    {
	      //_remotedb->update_project_received_crowdfunding(project->_id, project->_received_crowdfunding);
	    }
	}
      
      if(oldstatus != project->_status)
	_remotedb->update_project_status(project->_id, project->_status);
    }
}

void FinancialManagement::watch_new_project(int now)
{
  TIMER(30);

  TimeElapsed te("watch_new_project");
  
  auto cnt = _remotedb->get_projects_cnt();
  
  if(cnt > _projectcnt)
    {
      LOG(INFO, "found %ld new projects", cnt-_projectcnt);
      
      auto projects = _remotedb->get_projects_by_limit(_projectcnt, cnt-_projectcnt);
      
      for(auto project : projects)
	{
	  auto ret = add_project(project);
	  if(ret != 0)
	    LOG(WARNING, "add_project failed: %d, project_id:%ld", ret, project->_id);
	  else
	    LOG(INFO, "found new project... \n%s", project->to_json().c_str());
	}
    }
}

void FinancialManagement::run(volatile bool * alive)
{
  _remotedb = new RemoteDB();
  
  for(;;)
    {
      dot("%");
      if(_remotedb->connect())
	break;
      sleep(3);
    }
  
  load();

  start_epoll_server();
  
  while(*alive)
    {
      auto now = (int)uint32_t(time(NULL));

      dot(".");
      
      handle_order_heap(now);
      watch_new_project(now+3);
      update_project_status(now+6);
      print_status(now);
      
      auto msg = _qreqmsg.timed_pop();
      if(!msg)
	continue;

      auto req = msg->data();
      
      //write_file(req);
      auto rsp = handle_request(req);
      LOG(INFO, "rsp: %s", rsp.c_str());
      //write_file(rsp);

      msg->set_data(rsp);
      _qrspmsg.push(msg);
    }
  
  LOG(INFO, "run exit...");
}

void FinancialManagement::push_request(std::string req)
{
  _qreq.push(req);
}

std::string FinancialManagement::pop_response()
{
  return _qrsp.pop();
}

bool check_paras(const std::map<std::string, std::string> & paras, std::initializer_list<const char*> v)
{
  for(auto p = v.begin(); p != v.end(); p++)
    if(0 == paras.count(*p))
      {
	LOG(WARNING, "check_paras failed: %s", *p);
	return false;
      }
  return true;
}

bool status_match(int status1, int status2)
{
  if(status1 == 0)
    {
      if(status2 == ORDER_STATUS_BOOKING_SUCCESS) return true;
      if(status2 == ORDER_STATUS_PAYED_AUDITING) return true;
    }
  if(status1 == 1)
    {
      if(status2 == ORDER_STATUS_AUDIT_SUCCESS) return true;
    }
  if(status1 == 2)
    {
      if(status2 == ORDER_STATUS_AUDIT_FAILURE) return true;
      if(status2 == ORDER_STATUS_CANCELED) return true;
      if(status2 == ORDER_STATUS_PAYMENT_REFUNDED) return true;
      if(status2 == ORDER_STATUS_AUDIT_SUCCESS_TOO_LATE) return true;
      if(status2 == ORDER_STATUS_AUDIT_TIMEOUT) return true;
      if(status2 == ORDER_STATUS_BOOKING_TIMEOUT) return true;
    }
  return false;
}

std::string FinancialManagement::handle_request(std::string req)
{
  LOG(INFO, "into handle_request:\n%s", req.c_str());
  
  std::string rsp;
  
  std::string command;
  std::map<std::string, std::string> paras;
  int msn;

  std::vector<std::string> v;

  auto ret = extract_request(req, command, paras, msn);

  if(0 != ret)
    {
      LOG(WARNING, "extract_request failed: %d", ret);
      return gen_rsp(command, msn, ERROR_INVALID_JSON, v);
    }
  
  if(command == "get_product_info")
    {
      if(!check_paras(paras, {"product_no"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      
      auto a = get_project_by_no(paras["product_no"]);
      if(a)
	v.push_back(a->to_json());
      return gen_rsp(command, msn, 0, v);
    }
    
  if(command == "get_product_list")
    {
      if(!check_paras(paras, {"start_time", "end_time"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      
      int start = atoi(paras["start_time"].c_str());
      int end = atoi(paras["end_time"].c_str());
      std::string str;
      for(long i=_projectcnt-1; i>=0; i--)
	{
	  auto project = _projects[i];
	  auto t = project->_booking_starting_time;
	  if(t >= start && t <= end)
	    {
	      if(!str.empty())
		str += ";";
	      str += project->_no;
	    }
	}
      v.push_back("{\"products\":\"" + str + "\"}");
      return gen_rsp(command, msn, 0, v);
    }

  if(command == "booking")
    {
      if(!check_paras(paras, {"product_no", "user_publickey", "amount"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      
      std::map<std::string, std::string> kvs;
      kvs["project_no"] = paras["product_no"];
      kvs["user_publickey"] = paras["user_publickey"];
      kvs["booking_amount"] = paras["amount"];
      auto ret = add_order(kvs);
      if(ret == 0)
	v.push_back("{\"order_id\":" + kvs["id"] + "}");
      return gen_rsp(command, msn, ret, v);
    }

  if(command == "cancel_order")
    {
      if(!check_paras(paras, {"product_no", "user_publickey", "order_id"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      
      auto ret = cancel_order(paras["product_no"], paras["user_publickey"], atol(paras["order_id"].c_str()));
      return gen_rsp(command, msn, ret, v);
    }

  if(command == "update_order_txid")
    {
      if(!check_paras(paras, {"product_no", "user_publickey", "order_id", "txid", "investment_return_addr", "payment_timestamp"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      
      auto ret = update_order_txid(
				   atol(paras["order_id"].c_str()),
				   paras["product_no"],
				   paras["user_publickey"],
				   paras["txid"],
				   paras["investment_return_addr"],
				   atoi(paras["payment_timestamp"].c_str())
				   );
      return gen_rsp(command, msn, ret, v);
    }

  if(command == "order_txid_confirm")
    {
      if(!check_paras(paras, {"order_id", "txid", "success"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      
      auto ret = order_txid_confirm(
				    atol(paras["order_id"].c_str()),
				    paras["txid"],
				    paras["success"]
				    );
      return gen_rsp(command, msn, ret, v);
    }

  if(command == "payment_refund_success")
    {
      if(!check_paras(paras, {"order_id", "txid"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      
      auto ret = payment_refund_success(
					atol(paras["order_id"].c_str()),
					paras["txid"]
					);
      return gen_rsp(command, msn, ret, v);
    }
        
  if(command == "collect")
    {
      if(!check_paras(paras, {"user_publickey", "product_no"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      
      auto ret = collect(paras["user_publickey"], paras["product_no"]);
      return gen_rsp(command, msn, ret, v);	
    }

  if(command == "cancel_collection")
    {
      if(!check_paras(paras, {"user_publickey", "product_no"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      
      auto ret = cancel_collection(paras["user_publickey"], paras["product_no"]);
      return gen_rsp(command, msn, ret, v);	
    }
    
  if(command == "get_user_collections")
    {
      if(!check_paras(paras, {"user_publickey"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      
      std::string collections;
      auto ret = get_user_collections(paras["user_publickey"], collections);
      v.push_back("{\"collections\":\"" + collections + "\"}");
      return gen_rsp(command, msn, ret, v);	
    }

  if(command == "get_order")
    {
      if(!check_paras(paras, {"order_id"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      auto order = get_order(atol(paras["order_id"].c_str()));
      if(!order)
	return gen_rsp(command, msn, ERROR_NOT_EXIST_ORDER, v);	
      v.push_back(order->to_json());
      return gen_rsp(command, msn, 0, v);	
    }

  if(command == "get_user")
    {
      if(!check_paras(paras, {"user_publickey"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      auto user = get_user_by_publickey(paras["user_publickey"]);
      if(!user)
	return gen_rsp(command, msn, ERROR_NOT_EXIST_USER_PUBLICKEY, v);	
      v.push_back(user->to_json());
      return gen_rsp(command, msn, 0, v);	
    }

  if(command == "get_orders")
    {
      if(!check_paras(paras, {"product_no", "user_publickey", "status", "offset", "limit"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      
      long offset = atol(paras["offset"].c_str());
      int limit = atoi(paras["limit"].c_str());
      int status = atoi(paras["status"].c_str());
      long idx = 0;

      if(!paras["user_publickey"].empty() && paras["user_publickey"] != "all")
	add_user(paras["user_publickey"]);
      
      std::vector<long> * orders = NULL;
      
      if(paras["product_no"] == "all")
	{
	  auto user = get_user_by_publickey(paras["user_publickey"]);
	  if(!user)
	    return gen_rsp(command, msn, ERROR_NOT_EXIST_USER_PUBLICKEY, v);
	  orders = &user->_orders;
	}
      if(paras["user_publickey"] == "all")
	{
	  auto project = get_project_by_no(paras["product_no"]);
	  if(!project)
	    return gen_rsp(command, msn, ERROR_NOT_EXIST_PROJECT_NO, v);
	  orders = &project->_orders;
	}
      
      if(orders == NULL)
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);

      for(int i=orders->size()-1; i>=0 ; --i)
	{
	  auto order = get_order((*orders)[i]);
	  if(!order)
	    continue;
	  if(status != 999 && !status_match(status, order->_status))
	    continue;
	  if(idx++ < offset)
	    continue;
	  v.push_back(order->to_json());
	  limit--;
	  if(limit <= 0)
	    break;
	}
      
      return gen_rsp(command, msn, 0, v);
    }
  
  return gen_rsp(command, msn, ERROR_INVALID_COMMAND, v);
}


const char* query(const char* req)
{
  //a.push_request(req);
  //return "";
  
  // static FinancialManagement a;
  
  // static bool first = true;
  // if(first)
  //   {
  //     first = false;
  //     Config::instance()->parse("./config");
  //     a.start();
  //   }

  // static std::string rsp;
  
  // a.push_request(req);
  // rsp = a.pop_response();
  
  // return rsp.c_str();
}


// volatile bool client_connected = false;
// volatile int new_server_socket = 0;

// void FinancialManagement::start_server()
// {
//   std::thread thrd(&FinancialManagement::server_proc, this, &_qreq, &_qrsp);
//   thrd.detach();

//   std::thread thrdsend([this]{
      
//       std::string lastfailrsp;
      
//       for(;;)
// 	{
// 	  dot("*");
	  
// 	  std::string rsp;
	  
// 	  if(lastfailrsp.empty())
// 	    rsp = _qrsp.pop() + linesplit;
// 	  else
// 	    rsp = lastfailrsp;
	  
// 	  while(!client_connected)
// 	    sleep(1);
	  
// 	  auto cnt = send(new_server_socket, rsp.c_str(), rsp.length(),0);
	  
// 	  if(cnt != rsp.length())
// 	    {
// 	      client_connected = false;
// 	      close(new_server_socket);
// 	      lastfailrsp = rsp;
// 	      LOG(WARNING, "send to client failed, will close the socket...");
// 	      continue;
// 	    }

// 	  lastfailrsp.clear();
	    
// 	  while(!client_connected)
// 	    sleep(1);
// 	}
//     });
  
//   thrdsend.detach();
// }

// void FinancialManagement::server_proc(utils::Queue<std::string>* qreq, utils::Queue<std::string>* qrsp)
// {
//   struct sockaddr_in server_addr;
//   bzero(&server_addr,sizeof(server_addr)); 
//   server_addr.sin_family = AF_INET;
//   server_addr.sin_addr.s_addr = htons(INADDR_ANY);
//   server_addr.sin_port = htons(60001);
 
//   int server_socket = socket(PF_INET,SOCK_STREAM,0);
//   if( server_socket < 0)
//     {
//       printf("Create Socket Failed!");
//       exit(1);
//     }
    
//   { 
//     int opt =1;
//     setsockopt(server_socket,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
//   }
     
//   if(bind(server_socket,(struct sockaddr*)&server_addr,sizeof(server_addr)))
//     {
//       printf("Server Bind Port : %d Failed!", 60001); 
//       exit(1);
//     }
 
//   if(listen(server_socket, 20) )
//     {
//       printf("Server Listen Failed!"); 
//       exit(1);
//     }

//   static const int buflen = 8192;
//   char buffer[buflen];
//   buffer[buflen-1] = 0x00;
    
//   int idx = 0;

//   while (1) 
//     {
//       sleep(1);

//       idx = 0;
      
//       struct sockaddr_in client_addr;
//       socklen_t length = sizeof(client_addr);

//       LOG(INFO, "server start accept...");
      
//       new_server_socket = accept(server_socket,(struct sockaddr*)&client_addr,&length);
	
//       if(new_server_socket <= 0)
// 	{
// 	  LOG(WARNING, "Server Accept Failed %d!\n", new_server_socket);
// 	  continue;
// 	}

//       client_connected = true;
      
//       LOG(INFO, "a client is coming...");

//       for(;;)
// 	{
// 	  dot("+");
// 	  auto cnt = recv(new_server_socket,&buffer[idx],buflen-1-idx,0);
// 	  if (cnt <= 0)
// 	    {
// 	      client_connected = false;
// 	      close(new_server_socket);
// 	      LOG(WARNING, "Server Recieve Data Failed!\n");
// 	      break;
// 	    }

// 	  idx += cnt;

// 	  char* p = buffer;
// 	  buffer[idx] = 0x00;
	
// 	  while(p < &buffer[idx])
// 	    {
// 	      auto e = strstr(p, linesplit);
// 	      if(e == NULL)
// 		break;
// 	      *e = 0x00;
// 	      qreq->push(p);
// 	      p = e+strlen(linesplit);
// 	    }

// 	  int left = idx-(p-&buffer[0]);
// 	  if(left > 0 && p != &buffer[0])
// 	    memmove(buffer, p, left);
// 	  idx = left;
// 	}
//     }
    
//   close(server_socket);  
// }

