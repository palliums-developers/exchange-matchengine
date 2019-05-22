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
#include "financial_management.h"

std::string json(const char* name, const std::string & value, bool last)
{
  std::string str;
  str = str + "\"" + name + "\":\"" + value + "\"";
  if(!last) str += ",";
}
  
template<class T>
std::string json(const char* name, const T & value, bool last)
{
  std::string str;
  str = str + "\"" + name + "\":" + std::to_string(value);
  if(!last) str += ",";
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

int Project::create(std::map<std::string, std::string> & kvs, std::shared_ptr<Project> & o)
{
  o = std::make_shared<Project>();
    
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
    
  if(o->_no.empty())
    return ERROR_INVALID_PROJECT_NO;
  if(o->_annualized_return_rate <= 0)
    return ERROR_INVALID_ANNUALIZED_RETURN_RATE;
  if(o->_investment_period <=0 || o->_investment_period > 365*10)
    return ERROR_INVALID_INVESTMENT_PERIOD;
  if(o->_min_invest_amount <= 0)
    return ERROR_INVALID_MIN_INVEST_AMOUNT;
  if(o->_max_invest_amount <= 0)
    return ERROR_INVALID_MAX_INVEST_AMOUNT;
  if(o->_total_crowdfunding_amount <= 0)
    return ERROR_INVALID_TOTAL_CROWDFUNDING_AMOUNT;
  if(o->_booking_starting_time <= 0)
    return ERROR_INVALID_BOOKING_STARTING_TIME;
  if(o->_crowdfunding_ending_time <= 0)
    return ERROR_INVALID_CROWDFUNDING_ENDING_TIME;
  if(o->_interest_type < 0 || o->_interest_type >= INTEREST_TYPE_CNT)
    return ERROR_INVALID_INTEREST_TYPE;
  if(o->_borrower_info < 0 || o->_borrower_info >= BORROWER_INFO_CNT)
    return ERROR_INVALID_BORROWER_INFO;
  if(o->_crowdfunding_address.empty())
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


int Order::create(std::map<std::string, std::string> & kvs, std::shared_ptr<Order> & o)
{
  o = std::make_shared<Order>();
    
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
    
  if(o->_project_id < 0)
    return ERROR_INVALID_PROJECT_ID;
  if(o->_user_id < 0)
    return ERROR_INVALID_USER_ID;
  if(o->_booking_amount <= 0)
    return ERROR_INVALID_BOOKING_AMOUNT;
  //if(o->_investment_return_addr.empty())
  //  return ERROR_INVALID_INVESTMENT_RETURN_ADDR;
    
  auto now = uint32_t(time(NULL));
  o->_booking_timestamp = (int)now;
    
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
  str += json("investment_return_addr", _investment_return_addr, false);
  str += json("accumulated_gain", _accumulated_gain, false);
  str += json("status", _status, true);
  str += "}";
  return str;
}


int User::create(std::map<std::string, std::string> & kvs, std::shared_ptr<User> & o)
{
  o = std::make_shared<User>();
    
  if(kvs.count("id"))
    o->_id = atol(kvs["id"].c_str());

  if(kvs.count("publickey"))
    o->_publickey = kvs["publickey"];
  if(kvs.count("name"))
    o->_name = kvs["name"];
  if(kvs.count("collections"))
    o->_collections = kvs["collections"];
    
  if(o->_publickey.empty())
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


int FinancialManagement::add_project(std::map<std::string, std::string> & kvs)
{
  long id = _projectcnt;
  long ida = _projectcnt % maxcnt;
      
  //auto kvs = json2map(jsonstr);
  kvs["id"] = std::to_string(id);
  std::shared_ptr<Project> project;
    
  auto ret = Project::create(kvs, project);
  if(ret != 0)
    return ret;
    
  auto no = kvs["no"];
    
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
    
  std::shared_ptr<User> user;
  auto ret = User::create(kvs, user);
  if(ret != 0)
    return ret;
    
  ret = _remotedb->add_user(user);
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
  long id = _ordercnt;
  long ida = _ordercnt % maxcnt;
    
  kvs["id"] = std::to_string(id);
  std::shared_ptr<Order> order;
  auto ret = Order::create(kvs, order);
  if(ret != 0)
    return ret;
    
  auto no = kvs["project_no"];
  auto publickey = kvs["user_publickey"];

  long projectid = -1;
  std::shared_ptr<Project> project;
  if(_cache_project_no2id.count(no))
    projectid = _cache_project_no2id[no];
  else
    {
      project = _remotedb->get_project_by_no(no);
      if(project)
	projectid = project->_id;
    }
  if(projectid < 0)
    return ERROR_ADD_ORDER_WITH_WRONG_PROJECT_NO;
  if(!project)
    project = _projects[projectid%maxcnt];
  if(!project || project->_id != projectid)
    return ERROR_ADD_ORDER_WITH_WRONG_PROJECT_NO;

  if(project->_status == PROJECT_STATUS_CROWDFUND_SUCCESS)
    return ERROR_ADD_ORDER_CROWDFUNDING_FULL;
  if(project->_received_crowdfunding >= project->_total_crowdfunding_amount)
    return ERROR_ADD_ORDER_CROWDFUNDING_FULL;
    
  auto now = (int)uint32_t(time(NULL));
  if(now < project->_booking_starting_time)
    return ERROR_ADD_ORDER_CROWDFUNDING_NOT_START;
    
  if(now > (project->_crowdfunding_ending_time - 7200))
    return ERROR_ADD_ORDER_CROWDFUNDING_FINISHED;

  double amount = atof(kvs["booking_amount"].c_str());
    
  if(amount < project->_min_invest_amount)
    return ERROR_ADD_ORDER_TOO_LOW_AMOUNT;
  if(amount > project->_max_invest_amount)
    return ERROR_ADD_ORDER_TOO_HIGH_AMOUNT;

  if(amount > (project->_total_crowdfunding_amount - project->_booked_crowdfunding))
    return ERROR_ADD_ORDER_CROWDFUNDING_BOOK_FULL;
    
  long userid = -1;
  if(_cache_user_publickey2id.count(publickey))
    userid = _cache_user_publickey2id[publickey];
  else
    {
      auto user = _remotedb->get_user_by_publickey(publickey);
      if(user)
	userid = user->_id;
    }
  if(userid < 0)
    {
      add_user(publickey);
      userid = _cache_user_publickey2id[publickey];
    }
    
  kvs["project_id"] = std::to_string(projectid);
  kvs["user_id"] = std::to_string(userid);

  ret = _remotedb->add_order(order);
  if(ret != 0)
    return ret;

  _users[userid%maxcnt]->_orders.push_back(id);
  _projects[projectid%maxcnt]->_orders.push_back(id);
    
  _orders[ida] = order;

  project->_booked_crowdfunding += amount;
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

int FinancialManagement::update_order_txid(long orderid, std::string project_no, std::string publickey, std::string txid, std::string investment_return_addr)
{
  auto order = get_order(orderid);
    
  auto ret = check_order(order, project_no, publickey);
  if(0 != ret)
    return ret;

  if(order->_status == ORDER_STATUS_BOOKING_TIMEOUT || order->_status == ORDER_STATUS_AUDIT_TIMEOUT)
    return ERROR_ORDER_TIMEOUT;
    
  if(order->_status != ORDER_STATUS_BOOKING_SUCCESS)
    return ERROR_DUPLICATE_SET_TXID;
    
  ret = _remotedb->update_order_txid(orderid, txid, investment_return_addr);
  if(ret != 0)
    return ret;

  order->_payment_txid = txid;
  order->_status = ORDER_STATUS_PAYED_AUDITING;
  order->_investment_return_addr = investment_return_addr;
    
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

  if(success == "true")
    {
      auto now = (int)uint32_t(time(NULL));
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
    
  volatile bool alive = true;
  std::thread thrd(&FinancialManagement::run, this, &alive);
  thrd.detach();
}

bool FinancialManagement::load()
{
  long onetime_to_load = 1000;
    
  auto projectcnt = _remotedb->get_projects_cnt();
  auto ordercnt = _remotedb->get_orders_cnt();
  auto usercnt = _remotedb->get_users_cnt();
    
  {
    auto & acnt = projectcnt;
    std::shared_ptr<Project> last;
      
    auto cnt = acnt/onetime_to_load;
    auto left = acnt%onetime_to_load;
    for(long i=0; i<cnt; ++i)
      {
	auto v = _remotedb->get_projects_by_limit(i*onetime_to_load, onetime_to_load);
	for(auto & a : v)
	  {
	    _projects[a->_id%maxcnt] = a;
	    _cache_project_no2id[a->_no] = a->_id;
	    last = a;
	  }
      }

    auto v = _remotedb->get_projects_by_limit(cnt*onetime_to_load, left);
    for(auto & a : v)
      {
	_projects[a->_id%maxcnt] = a;
	_cache_project_no2id[a->_no] = a->_id;
	last = a;
      }
      
    if(last)
      _projectcnt = last->_id+1;
  }

  {
    auto & acnt = usercnt;
    std::shared_ptr<User> last;
      
    auto cnt = acnt/onetime_to_load;
    auto left = acnt%onetime_to_load;
    for(long i=0; i<cnt; ++i)
      {
	auto v = _remotedb->get_users_by_limit(i*onetime_to_load, onetime_to_load);
	for(auto & a : v)
	  {
	    _users[a->_id%maxcnt] = a;
	    _cache_user_publickey2id[a->_publickey] = a->_id;
	    last = a;
	  }
      }

    auto v = _remotedb->get_users_by_limit(cnt*onetime_to_load, left);
    for(auto & a : v)
      {
	_users[a->_id%maxcnt] = a;
	_cache_user_publickey2id[a->_publickey] = a->_id;
	last = a;
      }
      
    if(last)
      _usercnt = last->_id+1;
  }

  {
    auto & acnt = ordercnt;
    std::shared_ptr<Order> last;
      
    auto cnt = acnt/onetime_to_load;
    auto left = acnt%onetime_to_load;
    for(long i=0; i<cnt; ++i)
      {
	auto v = _remotedb->get_orders_by_limit(i*onetime_to_load, onetime_to_load);
	for(auto & a : v)
	  {
	    _orders[a->_id%maxcnt] = a;
	    auto user = _users[a->_user_id%maxcnt];
	    if(user && user->_id == a->_user_id)
	      user->_orders.push_back(a->_id);
	    auto project = _projects[a->_project_id%maxcnt];
	    if(project && project->_id == a->_project_id)
	      project->_orders.push_back(a->_id);
	    last = a;
	  }
      }

    auto v = _remotedb->get_orders_by_limit(cnt*onetime_to_load, left);
    for(auto & a : v)
      {
	_orders[a->_id%maxcnt] = a;
	auto user = _users[a->_user_id%maxcnt];
	if(user && user->_id == a->_user_id)
	  user->_orders.push_back(a->_id);
	auto project = _projects[a->_project_id%maxcnt];
	if(project && project->_id == a->_project_id)
	  project->_orders.push_back(a->_id);
	last = a;
      }
      
    if(last)
      _ordercnt = last->_id+1;
  }
    
}

void FinancialManagement::handle_order_heap()
{
  auto now = (int)uint32_t(time(NULL));

  for(;;)
    {
      if(_order_heap.empty())
	break;
	
      auto iter = _order_heap.begin();
      auto order = iter->second;
      auto booktime = order->_booking_timestamp;
	
      bool waiting = (order->_status == ORDER_STATUS_BOOKING_SUCCESS || order->_status == ORDER_STATUS_PAYED_AUDITING);
      auto timepassed = now - booktime;
	
      if(!waiting)
	{
	  _order_heap.erase(iter);
	  continue;
	}
	
      if(timepassed < 7200)
	break;
	
      if(order->_status == ORDER_STATUS_BOOKING_SUCCESS)
	order->_status = ORDER_STATUS_BOOKING_TIMEOUT;
      if(order->_status == ORDER_STATUS_PAYED_AUDITING)
	order->_status = ORDER_STATUS_AUDIT_TIMEOUT;
	
      auto project = get_project(order->_project_id);
      if(project)
	project->_booked_crowdfunding -= order->_booking_amount;
	
      _order_heap.erase(iter);
    }
}
  
void FinancialManagement::run(volatile bool * alive)
{
  _remotedb = new RemoteDB();
  
  for(;;)
    {
      if(_remotedb->connect())
	break;
      sleep(3);
    }
  
  load();
    
  while(*alive)
    {
      handle_order_heap();
      auto req = _qreq.pop();
      auto rsp = handle_request(req);
      _qrsp.push(rsp);
    }
}

void FinancialManagement::push_request(std::string req)
{
  _qreq.push(req);
}

std::string FinancialManagement::pop_response()
{
  return _qrsp.pop();
}

std::string FinancialManagement::handle_request(std::string req)
{
  std::string rsp;
  
  std::string command;
  std::map<std::string, std::string> paras;
  int msn;

  std::vector<std::string> v;
  
  if(0 != extract_request(req, command, paras, msn))
    return gen_rsp(command, msn, ERROR_INVALID_JSON, v);
    
  if(command == "get_product_info")
    {
      auto a = get_project_by_no(paras["product_no"]);
      if(a)
	v.push_back(a->to_json());
      return gen_rsp(command, msn, 0, v);
    }
    
  if(command == "get_product_list")
    {
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
	  v.push_back("{\"products\":\"" + str + "\"}");
	}
      return gen_rsp(command, msn, 0, v);
    }

  if(command == "booking")
    {
      std::map<std::string, std::string> kvs;
      kvs["project_no"] = paras["product_no"];
      kvs["user_publickey"] = paras["user_publickey"];
      kvs["booking_amount"] = paras["amount"];
      auto ret = add_order(kvs);
      return gen_rsp(command, msn, ret, v);
    }

  if(command == "cancel_order")
    {
      auto ret = cancel_order(paras["product_no"], paras["user_publickey"], atol(paras["order_id"].c_str()));
      return gen_rsp(command, msn, ret, v);
    }

  if(command == "update_order_txid")
    {
      auto ret = update_order_txid(
				   atol(paras["order_id"].c_str()),
				   paras["product_no"],
				   paras["user_publickey"],
				   paras["txid"],
				   paras["investment_return_addr"]
				   );
      return gen_rsp(command, msn, ret, v);
    }

  if(command == "order_txid_confirm")
    {
      auto ret = order_txid_confirm(
				    atol(paras["order_id"].c_str()),
				    paras["txid"],
				    paras["success"]
				    );
      return gen_rsp(command, msn, ret, v);
    }

  if(command == "payment_refund_success")
    {
      auto ret = payment_refund_success(
					atol(paras["order_id"].c_str()),
					paras["txid"]
					);
      return gen_rsp(command, msn, ret, v);
    }
        
  if(command == "collect")
    {
      auto ret = collect(paras["user_publickey"], paras["product_no"]);
      return gen_rsp(command, msn, ret, v);	
    }

  if(command == "cancel_collection")
    {
      auto ret = cancel_collection(paras["user_publickey"], paras["product_no"]);
      return gen_rsp(command, msn, ret, v);	
    }
    
  if(command == "get_user_collections")
    {
      std::string collections;
      auto ret = get_user_collections(paras["user_publickey"], collections);
      v.push_back("{\"collections\":\"" + collections + "\"}");
      return gen_rsp(command, msn, ret, v);	
    }

  return gen_rsp(command, msn, ERROR_INVALID_COMMAND, v);
}


const char* query(const char* req)
{
  static FinancialManagement a;
  a.start();
  a.push_request(req);
  auto rsp = a.pop_response();
  return rsp.c_str();
}
