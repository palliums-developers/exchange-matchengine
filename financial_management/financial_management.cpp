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

thread_local RemoteDB* _remotedb = NULL;

const char * linesplit = "@#$";

bool double_equal(double a, double b)
{
  return fabs(a-b) < 0.00001;
}

bool double_not_equal(double a, double b)
{
  return !double_equal(a, b);
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
  static auto fh = fopen("a1.txt", "a");
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
  char str[4*8192];
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

  if(kvs.count("projecter_publickey"))
    o->_projecter_publickey = kvs["projecter_publickey"];
  if(kvs.count("financial_controller_publickey"))
    o->_financial_controller_publickey = kvs["financial_controller_publickey"];
  if(kvs.count("crowdfunding_payment_txid"))
    o->_crowdfunding_payment_txid = kvs["crowdfunding_payment_txid"];
  
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
  {
    std::unique_lock<std::mutex> lk(_mtx);
    v["received_crowdfunding"] = std::to_string(_received_crowdfunding);
  }
  v["interest_type"] = std::to_string(_interest_type);
  v["borrower_info"] = std::to_string(_borrower_info);
  v["crowdfunding_address"] = _crowdfunding_address;
  v["product_description"] = _product_description;

  v["projecter_publickey"] = _projecter_publickey;
  v["financial_controller_publickey"] = _financial_controller_publickey;
  v["crowdfunding_payment_txid"] = _crowdfunding_payment_txid;
  
  return v;
}

  
std::string Project::to_json()
{
  if(!_invalid && !_json.empty())
    return _json;

  LOG(INFO, "project to_json called");
  
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
  {
    std::unique_lock<std::mutex> lk(_mtx);
    str += json("received_crowdfunding", _received_crowdfunding, false);
  }
  str += json("interest_type", _interest_type, false);
  str += json("borrower_info", _borrower_info, false);

  str += json("crowdfunding_address", _crowdfunding_address, false);

  str += json("projecter_publickey", _projecter_publickey, false);
  str += json("financial_controller_publickey", _financial_controller_publickey, false);
  str += json("crowdfunding_payment_txid", _crowdfunding_payment_txid, false);

  str += json("product_description", _product_description, true);
  str += "}";

  _invalid = false;
  _json = str;
  
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

  if(kvs.count("payment_timestamp"))
    o->_payment_timestamp = atoi(kvs["payment_timestamp"].c_str());
  
  if(kvs.count("oracle_publickey"))
    o->_oracle_publickey = kvs["oracle_publickey"];
  if(kvs.count("contract_address"))
    o->_contract_address = kvs["contract_address"];
  if(kvs.count("contract_script"))
    o->_contract_script = kvs["contract_script"];

  if(kvs.count("vout"))
    o->_vout = atoi(kvs["vout"].c_str());
  if(kvs.count("mc"))
    o->_mc = atoi(kvs["mc"].c_str());
  if(kvs.count("ms"))
    o->_ms = atoi(kvs["ms"].c_str());
  if(kvs.count("version"))
    o->_version = atoi(kvs["version"].c_str());
  if(kvs.count("locktime"))
    o->_locktime = atoi(kvs["locktime"].c_str());
  if(kvs.count("script_oracle_address"))
    o->_script_oracle_address = kvs["script_oracle_address"];
  
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

  v["oracle_publickey"] = _oracle_publickey;
  v["contract_address"] = _contract_address;
  v["contract_script"] = _contract_script;

  v["vout"] = std::to_string(_vout);
  v["mc"] = std::to_string(_mc);
  v["ms"] = std::to_string(_ms);
  v["version"] = std::to_string(_version);
  v["locktime"] = std::to_string(_locktime);
  v["script_oracle_address"] = _script_oracle_address;
  
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

  str += json("oracle_publickey", _oracle_publickey, false);
  str += json("contract_address", _contract_address, false);
  str += json("contract_script", _contract_script, false);

  str += json("vout", _vout, false);
  str += json("mc", _mc, false);
  str += json("ms", _ms, false);
  str += json("version", _version, false);
  str += json("locktime", _locktime, false);
  str += json("script_oracle_address", _script_oracle_address, false);

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

FinancialManagement* FinancialManagement::get()
{
  static FinancialManagement a;
  return &a;
}

std::shared_ptr<Message> gen_self_update_order_status_req(long orderid, int orderstatus)
{
  const char* str = "{\"command\": \"self_update_order_status\", \"seq\": 0, \"paras\": { \"order_id\":%ld, \"order_status\":%d } }";
  char buf[256];
  snprintf(buf, sizeof(buf), str, orderid, orderstatus);
  return std::make_shared<Message>(std::shared_ptr<Client>(), buf);
}

std::shared_ptr<Message> gen_self_update_project_received_crowdfunding(long projectid, double received_crowdfunding)
{
  const char* str = "{\"command\": \"self_update_project_received_crowdfunding\", \"seq\": 0, \"paras\": { \"project_id\":%ld, \"received_crowdfunding\":%f } }";
  char buf[256];
  snprintf(buf, sizeof(buf), str, projectid, received_crowdfunding);
  return std::make_shared<Message>(std::shared_ptr<Client>(), buf);
}

int FinancialManagement::add_project(std::shared_ptr<Project> project)
{
  if(project->_id != _projectcnt)
    return ERROR_ADD_EXISTING_PROJECT;
  
  long id = _projectcnt;
  long ida = _projectcnt % maxcnt;

  project->_id = id;
    
  auto no = project->_no;

  {
    std::unique_lock<std::mutex> lk(_mtx);
    if(_cache_project_no2id.count(no))
      return ERROR_ADD_EXISTING_PROJECT;

    _cache_project_no2id[no] = id;

    if(_projects[ida])
      _cache_project_no2id.erase(_projects[ida]->_no);
  }
  
  _projects[ida] = project;

  _projectcnt++;
  return 0;
}
  
int FinancialManagement::add_user(std::string publickey)
{
  {
    std::unique_lock<std::mutex> lk(_mtx);
    if(_cache_user_publickey2id.count(publickey))
      return 0;
  }
  
  long id = _usercnt;
  long ida = _usercnt % maxcnt;
    
  std::map<std::string, std::string> kvs;
    
  kvs["id"] = std::to_string(id);
  kvs["publickey"] = publickey;
    
  auto user = User::create(kvs);
    
  auto ret = _remotedb->add_user(user);
  if(ret != 0)
    return ret;

  user->_order_scanned = true;

  {
    std::unique_lock<std::mutex> lk(_mtx);
    _cache_user_publickey2id[publickey] = id;
    if(_users[ida])
      _cache_user_publickey2id.erase(_users[ida]->_publickey);
  }
  
  _users[ida] = user;
    
  _usercnt++;
  return 0;    
}

int check_valid_for_new_order(std::shared_ptr<Order> order, std::shared_ptr<Project> project)
{
  auto now = order->_booking_timestamp;
  auto amount = order->_booking_amount;
  
  if(project->_status == PROJECT_STATUS_CROWDFUND_SUCCESS)
    return ERROR_ADD_ORDER_CROWDFUNDING_FULL;

  {
    std::unique_lock<std::mutex> lk(project->_mtx);
    if(double_great_equal(project->_received_crowdfunding, project->_total_crowdfunding_amount))
      return ERROR_ADD_ORDER_CROWDFUNDING_FULL;
  }
  
  if(now < project->_booking_starting_time)
    return ERROR_ADD_ORDER_CROWDFUNDING_NOT_START;
  
  if(now > (project->_crowdfunding_ending_time - 7200))
    return ERROR_ADD_ORDER_CROWDFUNDING_FINISHED;

  if(double_less(amount, project->_min_invest_amount))
    return ERROR_ADD_ORDER_TOO_LOW_AMOUNT;
  
  if(double_great(amount, project->_max_invest_amount))
    return ERROR_ADD_ORDER_TOO_HIGH_AMOUNT;

  {
    std::unique_lock<std::mutex> lk(project->_mtx);
    if(double_great(amount, (project->_total_crowdfunding_amount - project->_booked_crowdfunding)))
      return ERROR_ADD_ORDER_CROWDFUNDING_BOOK_FULL;
  }
  
  return 0;
}


int FinancialManagement::add_order(std::map<std::string, std::string> & kvs)
{
  auto now = (int)uint32_t(time(NULL));

  long id = _ordercnt++;
  long ida = id % maxcnt;

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

  {
    std::unique_lock<std::mutex> lk(project->_mtx);
    if(double_great_equal(project->_received_crowdfunding, project->_total_crowdfunding_amount))
      return ERROR_ADD_ORDER_CROWDFUNDING_FULL;
  }
  
  if(now < project->_booking_starting_time)
    return ERROR_ADD_ORDER_CROWDFUNDING_NOT_START;
  
  if(now > (project->_crowdfunding_ending_time - 7200))
    return ERROR_ADD_ORDER_CROWDFUNDING_FINISHED;

  if(double_less(amount, project->_min_invest_amount))
    return ERROR_ADD_ORDER_TOO_LOW_AMOUNT;
  
  if(double_great(amount, project->_max_invest_amount))
    return ERROR_ADD_ORDER_TOO_HIGH_AMOUNT;

  {
    std::unique_lock<std::mutex> lk(project->_mtx);
    if(double_great(amount, (project->_total_crowdfunding_amount - project->_booked_crowdfunding)))
      return ERROR_ADD_ORDER_CROWDFUNDING_BOOK_FULL;
  }
  
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

  if(double_great(amount, (project->_total_crowdfunding_amount - project->_booked_crowdfunding)))
    return ERROR_ADD_ORDER_CROWDFUNDING_BOOK_FULL;
    
  project->update_booked_crowdfunding(amount);
  
  auto ret = _remotedb->add_order(order);
  
  if(ret != 0)
    {
      project->update_booked_crowdfunding(-amount);
      return ret;
    }

  user->_orders.push_back(id);
  {
    std::unique_lock<std::mutex> lk(project->_mtx);
    project->_orders.push_back(id);
  }
  
  _orders[ida] = order;

  {
    std::unique_lock<std::mutex> lk(_mtx);
    _order_txid_timeout_heap.emplace(order->_booking_timestamp, order);
  }
  
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

  project->update_booked_crowdfunding(-order->_booking_amount);
  
  order->_status = ORDER_STATUS_CANCELED;
    
  return 0;
}

int FinancialManagement::update_order_txid(long orderid, std::string project_no, std::string publickey, std::string txid, std::string investment_return_addr, int payment_timestamp, std::string oracle_publickey, std::string contract_address, std::string contract_script, int vout, int mc, int ms, int version, int locktime, std::string script_oracle_address)
{
  auto order = get_order(orderid);
    
  auto ret = check_order(order, project_no, publickey);
  if(0 != ret)
    return ret;

  if(order->_status == ORDER_STATUS_BOOKING_TIMEOUT || order->_status == ORDER_STATUS_AUDIT_TIMEOUT)
    return ERROR_ORDER_TIMEOUT;

  auto now = (int)uint32_t(time(NULL));
  if(now > (order->_booking_timestamp+1800))
    return ERROR_ORDER_TIMEOUT;
  
  if(order->_status != ORDER_STATUS_BOOKING_SUCCESS)
    return ERROR_DUPLICATE_SET_TXID;

  ret = _remotedb->update_order_txid(orderid, txid, investment_return_addr, payment_timestamp, oracle_publickey, contract_address, contract_script, vout, mc, ms, version, locktime, script_oracle_address);
  if(ret != 0)
    return ret;

  _remotedb->update_order_status(orderid, ORDER_STATUS_PAYED_AUDITING);
  
  order->_payment_txid = txid;
  order->_status = ORDER_STATUS_PAYED_AUDITING;
  order->_investment_return_addr = investment_return_addr;
  order->_payment_timestamp = payment_timestamp;
  order->_oracle_publickey = oracle_publickey;
  order->_contract_address = contract_address;
  order->_contract_script = contract_script;
  order->_vout = vout;
  order->_mc = mc;
  order->_ms = ms;
  order->_version = version;
  order->_locktime = locktime;
  order->_script_oracle_address = script_oracle_address;
  
  {
    std::unique_lock<std::mutex> lk(_mtx);
    _order_confirm_timeout_heap.emplace(order->_booking_timestamp, order);
  }
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

  {
    if(status == ORDER_STATUS_AUDIT_SUCCESS)
      project->update_received_crowdfunding(order->_booking_amount);
    else
      project->update_booked_crowdfunding(-order->_booking_amount);
  }
  
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
  auto userid = -1;
    
  {
    std::unique_lock<std::mutex> lk(_mtx);
    if(_cache_user_publickey2id.count(publickey))
      userid = _cache_user_publickey2id[publickey];
  }
    
  if(userid >= 0)
    return get_user(userid);
  return _remotedb->get_user_by_publickey(publickey);
}
  
std::shared_ptr<Project> FinancialManagement::get_project_by_no(std::string no)
{
  long projectid = -1;

  {
    std::unique_lock<std::mutex> lk(_mtx);
    if(_cache_project_no2id.count(no))
      projectid = _cache_project_no2id[no];
  }
  
  if(projectid >= 0)
    return get_project(projectid);
  
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

  dot("`");
  
  auto order = _remotedb->get_order(id);
  if(!order)
    return order;
  
  complete_order(order);
  _orders[id%maxcnt] = order;
  
  return order;
}

std::shared_ptr<Project> FinancialManagement::cache_get_project(long id)
{
  if(_projects[id%maxcnt] && _projects[id%maxcnt]->_id == id)
    return _projects[id%maxcnt];
  return std::shared_ptr<Project>();
}

std::shared_ptr<User> FinancialManagement::cache_get_user(long id)
{
  if(_users[id%maxcnt] && _users[id%maxcnt]->_id == id)
    return _users[id%maxcnt];
  return std::shared_ptr<User>();
}

std::shared_ptr<Order> FinancialManagement::cache_get_order(long id)
{
  if(_orders[id%maxcnt] && _orders[id%maxcnt]->_id == id)
    return _orders[id%maxcnt];
  return std::shared_ptr<Order>();
}

std::shared_ptr<Project> FinancialManagement::cache_get_project_by_no(std::string no)
{
  long projectid = -1;

  {
    std::unique_lock<std::mutex> lk(_mtx);
    if(_cache_project_no2id.count(no))
      projectid = _cache_project_no2id[no];
  }
  
  if(projectid >= 0)
    return cache_get_project(projectid);

  return std::shared_ptr<Project>();
}

std::shared_ptr<User> FinancialManagement::cache_get_user_by_publickey(std::string publickey)
{
  auto userid = -1;
    
  {
    std::unique_lock<std::mutex> lk(_mtx);
    if(_cache_user_publickey2id.count(publickey))
      userid = _cache_user_publickey2id[publickey];
  }
    
  if(userid >= 0)
    return cache_get_user(userid);

  return std::shared_ptr<User>();
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

struct ThreadPool
{
  static const int thread_count = THREAD_CNT;
  ThreadPool()
  {
    for(int i=0; i<thread_count; ++i)
      _threads[i] = new std::thread(&ThreadPool::run, this);
  }
  
  ~ThreadPool()
  {
    _running = false;
    
    for(int i=0; i<thread_count; ++i)
      {
	_threads[i]->join();
	delete _threads[i];
      }

    LOG(INFO, "thread pool exited!");
  }
  
  void run()
  {
    RemoteDB remotedb;
    _remotedb = &remotedb;
    
    for(;;)
      {
	if(_remotedb->connect())
	  break;
	sleep(3);
      }
    
    for(;;)
      {
	auto task = _qtasks.timed_pop();
	if(!task)
	  {
	    if(!_running)
	      break;
	    continue;
	  }
	
	task(_remotedb);
	
	_running_count--;
      }
  }

  void push(std::function<void(RemoteDB*)> task)
  {
    _running_count++;
    _qtasks.push(task);
  }

  void wait()
  {
    while(_running_count > 0)
      sleep(1);
    
    sleep(1);

    while(_running_count > 0)
      sleep(1);
  }
  
  utils::Queue<std::function<void(RemoteDB*)>> _qtasks;
  std::thread* _threads[thread_count];
  bool _running = true;
  std::atomic_int _running_count = 0;
};


void FinancialManagement::multithread_load_orders(std::vector<long> orders, std::shared_ptr<Project> project)
{
  utils::Queue<long> q;
  for(auto a : orders)
    q.push(a);
  
  auto handler = [&,this] {

    RemoteDB remotedb;
    _remotedb = &remotedb;

    for(;;)
      {
	if(_remotedb->connect())
	  break;
	sleep(3);
      }
    
    for(;;)
      {
	auto orderid = q.timed_pop();
    
	if(orderid == 0)
	  return;

	dot(",");
    
	auto order = get_order(orderid);
	if(!order)
	  continue;
    
	auto user = get_user(order->_user_id);
	if(!user)
	  continue;
    
	//load_order(order, project, user);
      }
  };
  
  for(int i=0; i<THREAD_CNT; ++i)
    {
      std::thread thrd(handler);
      thrd.detach();
    }
}

void FinancialManagement::load_project(std::shared_ptr<Project> project)
{
  dot("^");
  
  if(!project)
    return;
  _projects[project->_id%maxcnt] = project;

  {
    std::unique_lock<std::mutex> lk(_mtx);
    _cache_project_no2id[project->_no] = project->_id;
  }

  if(project->_status == PROJECT_STATUS_CROWDFUND_SUCCESS)
    {
      project->update_received_crowdfunding(project->_total_crowdfunding_amount);
      project->update_booked_crowdfunding(project->_total_crowdfunding_amount);
    }
  else if(project->_status == PROJECT_STATUS_CROWDFUNDING)
    {
      
    }
}

void FinancialManagement::load_user(std::shared_ptr<User> a)
{
  dot("#");

  if(!a)
    return;
  _users[a->_id%maxcnt] = a;
  
  {
    std::unique_lock<std::mutex> lk(_mtx);
    _cache_user_publickey2id[a->_publickey] = a->_id;
  }
}

void FinancialManagement::load_order(std::shared_ptr<Order> order)
{
  if(!order)
    return;

  auto project = cache_get_project(order->_project_id);
  auto user = cache_get_user(order->_user_id);

  if(!project || !user)
    {
      LOG(WARNING, "load order failed: %s", order->to_json());
      return;
    }
  
  _orders[order->_id%maxcnt] = order;

  order->_user_publickey = user->_publickey;
  order->_project_no = project->_no;

  {
    std::unique_lock<std::mutex> lk(_mtx);
    project->_orders.push_back(order->_id);
    user->_orders.push_back(order->_id);
  }
  
  {
    std::unique_lock<std::mutex> lk(_mtx);
    if(order->_status == ORDER_STATUS_BOOKING_SUCCESS)
      _order_txid_timeout_heap.emplace(order->_booking_timestamp, order);
    if(order->_status == ORDER_STATUS_PAYED_AUDITING)
      _order_confirm_timeout_heap.emplace(order->_booking_timestamp, order);
  }

  switch(order->_status)
    {
    case ORDER_STATUS_BOOKING_SUCCESS:
    case ORDER_STATUS_PAYED_AUDITING:
      {
	project->update_booked_crowdfunding(order->_booking_amount);
      }
      break;
    case ORDER_STATUS_AUDIT_SUCCESS:
      {
	project->update_booked_crowdfunding(order->_booking_amount);
	project->update_received_crowdfunding(order->_booking_amount);
      }
      break;
    default:
      {
      }
    }

  //dot(",");
}

bool FinancialManagement::load_multithread()
{
  LOG(INFO, "got into load_multithread");
  
  TimeElapsed te("load db");
  
  long onetime_to_load = 10000;
    
  auto lastproject = _remotedb->get_last_project();
  auto lastuser = _remotedb->get_last_user();
  auto lastorder = _remotedb->get_last_order();

  if(lastproject)
    _projectcnt = lastproject->_id+1;
  if(lastuser)
    _usercnt = lastuser->_id+1;
  if(lastorder)
    _ordercnt = lastorder->_id+1;

  LOG(INFO, "load multithread:  %ld %ld %ld", _projectcnt.load(), _usercnt.load(), _ordercnt.load());
  
  {
    ThreadPool tp;

    {
      auto cnt = _usercnt / onetime_to_load;
      if((_usercnt % onetime_to_load) != 0)
	cnt++;
      for(long i=0; i<cnt; ++i)
	{
	  auto func = [&](RemoteDB* remotedb, long idx){
	    dot("&");
	    auto v = remotedb->get_users_by_limit(idx*onetime_to_load, onetime_to_load);
	    for(auto a : v)
	      load_user(a);
	  };	  
	  tp.push(std::bind(func, std::placeholders::_1, i));
	}
    }

    tp.wait();
    
    {
      auto cnt = _projectcnt / onetime_to_load;
      if((_projectcnt % onetime_to_load) != 0)
	cnt++;
      for(long i=0; i<cnt; ++i)
	{
	  auto func = [&](RemoteDB* remotedb, long idx){
	    dot("(");
	    auto v = remotedb->get_projects_by_limit(idx*onetime_to_load, onetime_to_load);
	    for(auto a : v)
	      load_project(a);
	  };	  
	  tp.push(std::bind(func, std::placeholders::_1, i));
	}
    }

    tp.wait();
    
    {
      auto cnt = _ordercnt / onetime_to_load;
      if((_ordercnt % onetime_to_load) != 0)
	cnt++;
      for(long i=0; i<cnt; ++i)
	{
	  auto func = [&](RemoteDB* remotedb, long idx){
	    dot("[");
	    auto v = remotedb->get_orders_by_limit(idx*onetime_to_load, onetime_to_load);
	    for(auto a : v)
	      load_order(a);
	  };	  
	  tp.push(std::bind(func, std::placeholders::_1, i));
	}
    }

  }
  
  LOG(INFO, "load success: %ld, %ld, %ld", long(_projectcnt), long(_ordercnt), long(_usercnt));

  return true;
}

bool FinancialManagement::load()
{
  LOG(INFO, "got into load");
  
  TimeElapsed te("load db");
  
  long onetime_to_load = 10000;
    
  auto projectcnt = _remotedb->get_projects_cnt();
  auto ordercnt = _remotedb->get_orders_cnt();
  auto usercnt = _remotedb->get_users_cnt();

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
  
  //lmf_test
  //_ordercnt += 100000;
  
  LOG(INFO, "load success: %ld, %ld, %ld", long(_projectcnt), long(_ordercnt), long(_usercnt));

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
	  //dot("*");
	  
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

void FinancialManagement::handle_order_list(int now)
{
  // static int idx1 = _usercnt-1;
  // if(idx1 >= 0)
  //   {
  //     auto user = _users[idx1%maxcnt];
  //     if(user && !user->_order_scanned && user->_orders.empty())
  // 	get_user_orders(user);
  //     idx1--;
  //   }
  // else
  //   idx1 = _usercnt-1;
  
  // static int idx2 = _projectcnt-1;
  // if(idx2 >= 0)
  //   {
  //     auto project = _projects[idx2%maxcnt];
  //     if(project && !project->_order_scanned && project->_orders.empty())
  // 	get_project_orders(project);
  //     idx2--;
  //   }
  // else
  //   idx2 = _projectcnt-1;

  // static int idx3 = _ordercnt-1;
  // static int cnt = 0;
  // if(idx3 >= 0 && cnt++ < 100000)
  //   {
  //     auto order = _orders[idx3%maxcnt];
  //     if(!order)
  // 	get_order(idx3);
  //     idx3--;
  //   }
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
      dot(":");
      std::multimap<int, std::shared_ptr<Order>>::iterator iter;
      std::shared_ptr<Order> order;
      
      {
	std::unique_lock<std::mutex> lk(_mtx);
	if(heap->empty())
	  break;
	iter = heap->begin();
	order = iter->second;
      }
      
      auto booktime = order->_booking_timestamp;
	
      bool waiting = (order->_status == ORDER_STATUS_BOOKING_SUCCESS);
      auto timepassed = now - booktime;
	
      if(!waiting)
	{
	  std::unique_lock<std::mutex> lk(_mtx);
	  heap->erase(iter);
	  continue;
	}
	
      if(timepassed < 1800)
	break;

      dot("<");
      order->_status = ORDER_STATUS_BOOKING_TIMEOUT;
      
      _qreqmsg.push(gen_self_update_order_status_req(order->_id, order->_status));
      //_remotedb->update_order_status(order->_id, ORDER_STATUS_BOOKING_TIMEOUT);
      
      auto project = get_project(order->_project_id);
      if(project)
	project->update_booked_crowdfunding(-order->_booking_amount);
      
      {
	std::unique_lock<std::mutex> lk(_mtx);
	heap->erase(iter);
      }
    }

  heap = &_order_confirm_timeout_heap;
  
  for(;;)
    {
      dot(";");
      
      std::multimap<int, std::shared_ptr<Order>>::iterator iter;
      std::shared_ptr<Order> order;

      {
	std::unique_lock<std::mutex> lk(_mtx);
	if(heap->empty())
	  break;
	iter = heap->begin();
	order = iter->second;
      }
      
      auto booktime = order->_booking_timestamp;
	
      bool waiting = (order->_status == ORDER_STATUS_PAYED_AUDITING);
      auto timepassed = now - booktime;
	
      if(!waiting)
	{
	  std::unique_lock<std::mutex> lk(_mtx);
	  heap->erase(iter);
	  continue;
	}
	
      if(timepassed < 7200)
	break;

      dot(">");

      order->_status = ORDER_STATUS_AUDIT_TIMEOUT;

      _qreqmsg.push(gen_self_update_order_status_req(order->_id, order->_status));
      //_remotedb->update_order_status(order->_id, ORDER_STATUS_AUDIT_TIMEOUT);
	
      auto project = get_project(order->_project_id);
      if(project)
	project->update_booked_crowdfunding(-order->_booking_amount);
      
      {
	std::unique_lock<std::mutex> lk(_mtx);
	heap->erase(iter);
      }
    }
  
}

void FinancialManagement::update_project_status(int now)
{
  TIMER(30);

  dot("?");
  
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
	    project->update_status(PROJECT_STATUS_CROWDFUNDING);
	}
      else if(project->_status == PROJECT_STATUS_CROWDFUNDING)
	{
	  if(now > project->_crowdfunding_ending_time)
	    project->update_status(PROJECT_STATUS_CROWDFUND_FAILURE);
	  else if(double_great_equal(project->_received_crowdfunding, project->_total_crowdfunding_amount))
	    project->update_status(PROJECT_STATUS_CROWDFUND_SUCCESS);
	  else
	    {
	      if(double_not_equal(project->_received_crowdfunding, project->_last_received_crowdfunding))
		{
		  project->_last_received_crowdfunding = project->_received_crowdfunding;
		  _qreqmsg.push(gen_self_update_project_received_crowdfunding(project->_id, project->_received_crowdfunding));
		}
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

  dot("+");
  
  TimeElapsed te("watch_new_project");
  
  auto last = _remotedb->get_last_project();
  
  if(!last)
    return;
  
  if(last->_id >= _projectcnt)
    {
      LOG(INFO, "found %ld new projects", last->_id-_projectcnt+1);
      
      auto projects = _remotedb->get_projects_by_limit(_projectcnt, last->_id-_projectcnt+1);
      
      for(auto project : projects)
	{
	  project->_order_scanned = true;
	  
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
      if(_remotedb->connect())
	break;
      sleep(3);
    }

  //load();
  load_multithread();

  start_epoll_server();

  auto msghandler = [this]{

    _remotedb = new RemoteDB();

    for(;;)
      {
	if(_remotedb->connect())
	  break;
	sleep(3);
      }
    
    for(;;)
      {
	auto msg = _qreqmsg.timed_pop();
	if(!msg)
	  continue;

	auto req = msg->data();
      
	//write_file(req);
      
	auto rsp = handle_request(req);
	if(rsp.empty())
	  continue;

	//lmf_test
	LOG(INFO, "rsp: %s", rsp.c_str());
	//dot(">");
      
	//write_file(rsp);

	msg->set_data(rsp);
	_qrspmsg.push(msg);

      }
    
  };
  
  for(int i=0; i<THREAD_CNT; ++i)
    {
      std::thread thrd(msghandler);
      thrd.detach();
    }
  
  while(*alive)
    {
      auto now = (int)uint32_t(time(NULL));
      
      dot(".");
      
      handle_order_heap(now);
      watch_new_project(now+3);
      update_project_status(now+6);
      print_status(now);
      handle_order_list(now);
      
      sleep(1);
    }
  
  LOG(INFO, "run exit...");
}

// lmf
// {"seq": 1, "command": "get_orders", "paras": {"user_publickey": "mousWBSN7Rsqi8qpmZp7C6VmRkBGPD5bFF", "product_no": "all", "status": 1, "offset": 0, "limit": 10}}

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

std::vector<int> status_match2(int status)
{
  std::vector<int> v;
  if(status == 0)
    {
      v.push_back(ORDER_STATUS_BOOKING_SUCCESS);
      v.push_back(ORDER_STATUS_PAYED_AUDITING);
    }
  if(status == 1)
    {
      v.push_back(ORDER_STATUS_AUDIT_SUCCESS);
    }
  if(status == 2)
    {
      v.push_back(ORDER_STATUS_AUDIT_FAILURE);
      v.push_back(ORDER_STATUS_CANCELED);
      v.push_back(ORDER_STATUS_PAYMENT_REFUNDED);
      v.push_back(ORDER_STATUS_AUDIT_SUCCESS_TOO_LATE);
      v.push_back(ORDER_STATUS_AUDIT_TIMEOUT);
      v.push_back(ORDER_STATUS_BOOKING_TIMEOUT);
    }
  return v;
}

std::vector<long> FinancialManagement::get_user_orders(std::shared_ptr<User> user)
{
  std::vector<long> v;
  
  if(!user)
    return v;

  LOG(INFO, "call remotedb get_user_orders %ld", user->_id);
  
  v = _remotedb->get_user_orders(user->_id);

  user->_orders = v;
  user->_order_scanned = true;
  
  return v;
}

void FinancialManagement::cache_order(std::shared_ptr<Order> order)
{
  complete_order(order);
  _orders[order->_id%maxcnt] = order;
}

std::vector<std::shared_ptr<Order>> 
FinancialManagement::get_user_orders_limit(std::shared_ptr<User> user, std::vector<int> statuses, int offset, int count, int direction)
{
  auto v = _remotedb->get_user_orders_limit(user, statuses, offset, count, direction);
  for(auto order : v)
    cache_order(order);
  return v;
}

std::vector<std::shared_ptr<Order>> 
FinancialManagement::get_project_orders_limit(std::shared_ptr<Project> project, std::vector<int> statuses, int offset, int count, int direction)
{
  auto v = _remotedb->get_project_orders_limit(project, statuses, offset, count, direction);
  for(auto order : v)
    cache_order(order);
  return v;
}

std::vector<long> FinancialManagement::get_project_orders(std::shared_ptr<Project> project)
{
  std::vector<long> v;
  
  if(!project)
    return v;

  LOG(INFO, "call remotedb get_project_orders %ld", project->_id);
  
  v = _remotedb->get_project_orders(project->_id);

  {
    std::unique_lock<std::mutex> lk(project->_mtx);
    project->_orders = v;
    project->_order_scanned = true;
  }
  
  return v;
}

void FinancialManagement::complete_order(std::shared_ptr<Order> order)
{
  if(order->_user_publickey.empty())
    {
      auto user = get_user(order->_user_id);
      if(user)
	order->_user_publickey = user->_publickey;
    }

  if(order->_project_no.empty())
    {
      auto project = get_project(order->_project_id);
      if(project)
	order->_project_no = project->_no;
    }
}

std::string FinancialManagement::handle_request(std::string req)
{
  //lmf_test
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

  if(command == "self_update_order_status")
    {
      _remotedb->update_order_status(atol(paras["order_id"].c_str()), atoi(paras["order_status"].c_str()));
      return "";
    }

  if(command == "self_update_project_received_crowdfunding")
    {
      _remotedb->update_project_received_crowdfunding(atol(paras["project_id"].c_str()), atof(paras["received_crowdfunding"].c_str()));
      return "";
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
	  if(!project)
	    continue;
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
      if(!check_paras(paras, {"product_no", "user_publickey", "order_id", "txid", "investment_return_addr", "payment_timestamp", "oracle_publickey", "contract_address", "contract_script", "vout", "mc", "ms", "version", "locktime", "script_oracle_address"}))
      	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);

      
      auto ret = update_order_txid(
      				   atol(paras["order_id"].c_str()),
      				   paras["product_no"],
      				   paras["user_publickey"],
      				   paras["txid"],
      				   paras["investment_return_addr"],
      				   atoi(paras["payment_timestamp"].c_str()),
      				   paras["oracle_publickey"],
      				   paras["contract_address"],
      				   paras["contract_script"],
				   atoi(paras["vout"].c_str()),
				   atoi(paras["mc"].c_str()),
				   atoi(paras["ms"].c_str()),
				   atoi(paras["version"].c_str()),
				   atoi(paras["locktime"].c_str()),
				   paras["script_oracle_address"]
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

      std::vector<long> orders;
      
      if(paras["product_no"] == "all")
	{
	  auto user = get_user_by_publickey(paras["user_publickey"]);
	  if(!user)
	    return gen_rsp(command, msn, ERROR_NOT_EXIST_USER_PUBLICKEY, v);
	  orders = user->_orders;
	  if(orders.empty() && !user->_order_scanned)
	    orders = get_user_orders(user);
	}
      
      if(paras["user_publickey"] == "all")
	{
	  auto project = get_project_by_no(paras["product_no"]);
	  if(!project)
	    return gen_rsp(command, msn, ERROR_NOT_EXIST_PROJECT_NO, v);
	  {
	    std::unique_lock<std::mutex> lk(project->_mtx);
	    orders = project->_orders;
	  }
	  if(orders.empty() && !project->_order_scanned)
	    orders = get_project_orders(project);
	}
      
      for(int i=orders.size()-1; i>=0 ; --i)
	{
	  auto order = get_order(orders[i]);
	  if(!order)
	    continue;
	  complete_order(order);
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

  if(command == "update_crowdfunding_payment_txid")
    {
      if(!check_paras(paras, {"product_no", "crowdfunding_payment_txid"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      
      auto a = get_project_by_no(paras["product_no"]);
      if(!a)
	return gen_rsp(command, msn, ERROR_NOT_EXIST_PROJECT_NO, v);
      if(a->_status != PROJECT_STATUS_CROWDFUND_SUCCESS)
	return gen_rsp(command, msn, ERROR_PROJECT_NOT_CROWDFUND_SUCCESS, v);

      auto ret = _remotedb->update_crowdfunding_payment_txid(a->_id, paras["crowdfunding_payment_txid"]);
      
      return gen_rsp(command, msn, ret, v);
    }
  
  return gen_rsp(command, msn, ERROR_INVALID_COMMAND, v);
}

const char* query(const char* req)
{
}


