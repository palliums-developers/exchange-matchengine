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
#include "payment.h"

thread_local RemoteDB* _remotedb = NULL;

const char * linesplit = "@#$";

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

FILE* log2filehandle = NULL;

void log_2_file(const std::string & str)
{
  static auto fh = fopen("a.log", "a");
  log2filehandle = fh;
  if(fh)
    {
      fwrite(str.c_str(), 1, str.length(), fh);
      fwrite("\n\n", 1, 2, fh);
    }
}

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

std::shared_ptr<Order> Order::create(std::map<std::string, std::string> & kvs)
{
  auto o = std::make_shared<Order>();

  if(kvs.count("id")) 
    o->_id = atol(kvs["id"].c_str());
  if(kvs.count("type")) 
    o->_type = atoi(kvs["type"].c_str());
  if(kvs.count("from")) 
    o->_from = atol(kvs["from"].c_str());
  if(kvs.count("to")) 
    o->_to = atol(kvs["to"].c_str());
  if(kvs.count("amount")) 
    o->_amount = atof(kvs["amount"].c_str());
  if(kvs.count("timestamp")) 
    o->_timestamp = atoi(kvs["timestamp"].c_str());
  if(kvs.count("withdraw_addr")) 
    o->_withdraw_addr = kvs["withdraw_addr"];
  if(kvs.count("recharge_utxo")) 
    o->_recharge_utxo = kvs["recharge_utxo"];
  if(kvs.count("withdraw_utxo")) 
    o->_withdraw_utxo = kvs["withdraw_utxo"];
  if(kvs.count("withdraw_fee")) 
    o->_withdraw_fee = atof(kvs["withdraw_fee"].c_str());
  if(kvs.count("utxo_confirmed")) 
    o->_utxo_confirmed = atoi(kvs["utxo_confirmed"].c_str());

  if(kvs.count("legal_currency_name")) 
    o->_legal_currency_name = kvs["legal_currency_name"];
  if(kvs.count("legal_currency_value")) 
    o->_legal_currency_value = atof(kvs["legal_currency_value"].c_str());
  
  return o;
}

int Order::check_valid()
{
  
  return 0;
}

std::map<std::string, std::string> Order::to_map()
{
  std::map<std::string, std::string> v;

  v["id"] = std::to_string(_id);
  v["type"] = std::to_string(_type);
  v["from"] = std::to_string(_from);
  v["to"] = std::to_string(_to);
  v["amount"] = std::to_string(_amount);
  v["timestamp"] = std::to_string(_timestamp);
  v["withdraw_addr"] = (_withdraw_addr);
  v["recharge_utxo"] = (_recharge_utxo);
  v["withdraw_utxo"] = (_withdraw_utxo);
  v["withdraw_fee"] = std::to_string(_withdraw_fee);
  v["utxo_confirmed"] = std::to_string(_utxo_confirmed);

  v["legal_currency_name"] = _legal_currency_name;
  v["legal_currency_value"] = std::to_string(_legal_currency_value);
  
  return v;
}

std::string Order::to_json()
{
  std::string str = "{";

  str += json("id", _id, false);
  str += json("type", _type, false);
  str += json("from", _from, false);
  str += json("to", _to, false);
  str += json("amount", _amount, false);
  str += json("timestamp", _timestamp, false);
  str += json("withdraw_addr", _withdraw_addr, false);
  str += json("recharge_utxo", _recharge_utxo, false);
  str += json("withdraw_utxo", _withdraw_utxo, false);
  str += json("withdraw_fee", _withdraw_fee, false);
  str += json("utxo_confirmed", _utxo_confirmed, false);

  str += json("legal_currency_name", _legal_currency_name, false);
  str += json("legal_currency_value", _legal_currency_value, true);
  
  str += "}";
  return str;
}


std::mutex* get_user_mtx(long id)
{
  static std::mutex mtxs[1024];
  return &mtxs[id%1024];
}

std::shared_ptr<User> User::create(std::map<std::string, std::string> & kvs)
{
  auto o = std::make_shared<User>();

  if(kvs.count("id"))
    o->_id = atol(kvs["id"].c_str());
  if(kvs.count("user"))
    o->_user = kvs["user"];
  if(kvs.count("login_pwd_hash"))
    o->_login_pwd_hash = kvs["login_pwd_hash"];
  if(kvs.count("tx_pwd_hash"))
    o->_tx_pwd_hash = kvs["tx_pwd_hash"];
  if(kvs.count("login_pwd_salt"))
    o->_login_pwd_salt = kvs["login_pwd_salt"];
  if(kvs.count("tx_pwd_salt"))
    o->_tx_pwd_salt = kvs["tx_pwd_salt"];
  if(kvs.count("balance"))
    o->_balance = atof(kvs["balance"].c_str());
  if(kvs.count("phone"))
    o->_phone = kvs["phone"];
  if(kvs.count("mail"))
    o->_mail = kvs["mail"];
  if(kvs.count("recharge_addr"))
    o->_recharge_addr = kvs["recharge_addr"];
  if(kvs.count("reward_point"))
    o->_reward_point = atoi(kvs["reward_point"].c_str());
  if(kvs.count("timestamp"))
    o->_timestamp = atoi(kvs["timestamp"].c_str());

  if(kvs.count("device_token"))
    o->_device_token = kvs["device_token"];
  if(kvs.count("withdraw_addr"))
    o->_withdraw_addr = kvs["withdraw_addr"];

  //o->_mtx = get_user_mtx(o->_id);
  
  return o;
}

int User::check_valid()
{
  return 0;
}

std::map<std::string, std::string> User::to_map()
{
  std::map<std::string, std::string> v;

  v["id"] = std::to_string(_id);
  v["user"] = (_user);
  v["login_pwd_hash"] = (_login_pwd_hash);
  v["tx_pwd_hash"] = (_tx_pwd_hash);
  v["login_pwd_salt"] = (_login_pwd_salt);
  v["tx_pwd_salt"] = (_tx_pwd_salt);
  v["balance"] = std::to_string(_balance);
  v["phone"] = (_phone);
  v["mail"] = (_mail);
  v["recharge_addr"] = (_recharge_addr);
  v["reward_point"] = std::to_string(_reward_point);
  v["timestamp"] = std::to_string(_timestamp);
  v["device_token"] = _device_token;
  v["withdraw_addr"] = _withdraw_addr;
  
  return v;
}

std::string User::to_json()
{
  std::string str = "{";
  str += json("id", _id, false);
  str += json("user", _user, false);
  str += json("login_pwd_hash", _login_pwd_hash, false);
  str += json("tx_pwd_hash", _tx_pwd_hash, false);
  str += json("login_pwd_salt", _login_pwd_salt, false);
  str += json("tx_pwd_salt", _tx_pwd_salt, false);
  str += json("balance", _balance, false);
  str += json("phone", _phone, false);
  str += json("mail", _mail, false);
  str += json("recharge_addr", _recharge_addr, false);
  str += json("reward_point", _reward_point, false);
  str += json("timestamp", _timestamp, false);
  str += json("device_token", _device_token, false);
  str += json("withdraw_addr", _withdraw_addr, true);
  
  str += "}";
  return str;
}

Payment* Payment::get()
{
  static Payment a;
  return &a;
}

// std::shared_ptr<Message> gen_self_update_order_status_req(long orderid, int orderstatus)
// {
//   const char* str = "{\"command\": \"self_update_order_status\", \"seq\": 0, \"paras\": { \"order_id\":%ld, \"order_status\":%d } }";
//   char buf[256];
//   snprintf(buf, sizeof(buf), str, orderid, orderstatus);
//   return std::make_shared<Message>(std::shared_ptr<Client>(), buf);
// }

  
int Payment::add_user(std::map<std::string, std::string> & kvs)
{
  long id = _usercnt++;
  long ida = id % maxcnt;

  kvs["id"] = std::to_string(id);
  kvs["timestamp"] = std::to_string((int)uint32_t(time(NULL)));
  
  auto user = User::create(kvs);
  
  {
    std::unique_lock<std::mutex> lk(_mtx);
    auto & v = _cache_user_phone_mail_2_id;
    if(!user->_user.empty() && v.count(user->_user))
      return ERROR_EXIST_USER;
    if(!user->_phone.empty() && v.count(user->_phone))
      return ERROR_EXIST_USER;
    if(!user->_mail.empty() && v.count(user->_mail))
      return ERROR_EXIST_USER;
  }
  
  auto ret = _remotedb->add_user(user);
  if(ret != 0)
    return ret;

  {
    std::unique_lock<std::mutex> lk(_mtx);
    auto & v = _cache_user_phone_mail_2_id;
    if(!user->_user.empty()) v[user->_user] = id;
    if(!user->_phone.empty()) v[user->_phone] = id;
    if(!user->_mail.empty()) v[user->_mail] = id;

    if(_users[ida]) {
      auto o = _users[ida];
      if(v.count(o->_user)) v.erase(o->_user);
      if(v.count(o->_phone)) v.erase(o->_phone);
      if(v.count(o->_mail)) v.erase(o->_mail);
    }
  }
  
  _users[ida] = user;
    
  return 0;    
}

int Payment::update_user(std::shared_ptr<User> user, std::map<std::string, std::string> & kvs)
{
  auto user_ori = *user;
  auto o = user;

  // if(kvs.count("id"))
  //   o->_id = atol(kvs["id"].c_str());
  if(kvs.count("user"))
    o->_user = kvs["user"];
  if(kvs.count("login_pwd_hash"))
    o->_login_pwd_hash = kvs["login_pwd_hash"];
  if(kvs.count("tx_pwd_hash"))
    o->_tx_pwd_hash = kvs["tx_pwd_hash"];
  if(kvs.count("login_pwd_salt"))
    o->_login_pwd_salt = kvs["login_pwd_salt"];
  if(kvs.count("tx_pwd_salt"))
    o->_tx_pwd_salt = kvs["tx_pwd_salt"];
  // if(kvs.count("balance"))
  //   o->_balance = atof(kvs["balance"].c_str());
  if(kvs.count("phone"))
    o->_phone = kvs["phone"];
  if(kvs.count("mail"))
    o->_mail = kvs["mail"];
  if(kvs.count("recharge_addr"))
    o->_recharge_addr = kvs["recharge_addr"];
  if(kvs.count("reward_point"))
    o->_reward_point = atoi(kvs["reward_point"].c_str());
  // if(kvs.count("timestamp"))
  //   o->_timestamp = atoi(kvs["timestamp"].c_str());

  if(kvs.count("device_token"))
    o->_device_token = kvs["device_token"];
  if(kvs.count("withdraw_addr"))
    o->_withdraw_addr = kvs["withdraw_addr"];

  auto ret = _remotedb->update_user(user, kvs);
  if(ret != 0) {
    *user = user_ori;
    return ret;
  }
  
  return 0;
}

int Payment::update_order(std::shared_ptr<Order> order, std::map<std::string, std::string> & kvs)
{
  auto order_ori = *order;
  auto o = order;
  
  // if(kvs.count("id")) 
  //   o->_id = atol(kvs["id"].c_str());
  // if(kvs.count("type")) 
  //   o->_type = atoi(kvs["type"].c_str());
  // if(kvs.count("from")) 
  //   o->_from = atol(kvs["from"].c_str());
  // if(kvs.count("to")) 
  //   o->_to = atol(kvs["to"].c_str());
  // if(kvs.count("amount")) 
  //   o->_amount = atof(kvs["amount"].c_str());
  // if(kvs.count("timestamp")) 
  //   o->_timestamp = atoi(kvs["timestamp"].c_str());
  // if(kvs.count("withdraw_addr")) 
  //   o->_withdraw_addr = kvs["withdraw_addr"];
  // if(kvs.count("recharge_utxo")) 
  //   o->_recharge_utxo = kvs["recharge_utxo"];
  // if(kvs.count("withdraw_utxo")) 
  //   o->_withdraw_utxo = kvs["withdraw_utxo"];
  // if(kvs.count("withdraw_fee")) 
  //   o->_withdraw_fee = atof(kvs["withdraw_fee"].c_str());
  // if(kvs.count("legal_currency_name")) 
  //   o->_legal_currency_name = kvs["legal_currency_name"];
  // if(kvs.count("legal_currency_value")) 
  //   o->_legal_currency_value = atof(kvs["legal_currency_value"].c_str());

  if(kvs.count("utxo_confirmed") && kvs["utxo_confirmed"] == "1") {
    if(o->_utxo_confirmed)
      return ERROR_DUPLICATE_UTXO_CONFIRM;
    o->_utxo_confirmed = 1;
  }
  
  auto ret = _remotedb->update_order(order, kvs);
  if(ret != 0) {
    *order = order_ori;
    return ret;
  }
  
  return 0;
}

static const double min_withdraw_fee = 0.00050000;

struct BalanceSaver
{
  BalanceSaver(double* balance, std::mutex& mtx) : _balance(balance), _mtx(mtx) { }
  ~BalanceSaver() {
    if(!_commited) {
      std::unique_lock<std::mutex> lk(_mtx);
      *_balance = *_balance - _amount;
    }
  }
  void change(double amount)
  {
    _amount = amount;
    *_balance = *_balance + _amount;
  }
  void commit() { _commited = true; }
  double* _balance;
  double _amount = 0;
  bool _commited = false;
  std::mutex& _mtx;
};

int Payment::add_order(std::map<std::string, std::string> & kvs)
{
  auto now = (int)uint32_t(time(NULL));

  long id = _ordercnt++;
  long ida = id % maxcnt;

  kvs["id"] = std::to_string(id);
  kvs["timestamp"] = std::to_string((int)uint32_t(time(NULL)));

  auto order = Order::create(kvs);
  if(!order)
    return ERROR_NOT_EXIST_ORDER;
  order->_id = id;
  
  auto user = get_user(order->_from);
  if(!user)
    return ERROR_NOT_EXIST_USER;

  std::shared_ptr<User> touser;
  if(order->_type == ORDER_TYPE_TRANSACTION) {
    touser = get_user(order->_to);
    if(!touser)
      return ERROR_NOT_EXIST_USER;
  }
  
  auto amount = atof(kvs["amount"].c_str());
  if(amount <= 0)
    return ERROR_INVALID_PARAS;
  
  if(order->_type == ORDER_TYPE_TRANSACTION) {
    if(order->_from == order->_to)
      return ERROR_INVALID_PARAS;
  }
  
  if(order->_type == ORDER_TYPE_WITHDRAW &&
     double_less(order->_withdraw_fee, min_withdraw_fee))
    return ERROR_INSUFFICIENT_FEE; 
  
  BalanceSaver bs(&user->_balance, _mtx);
  
  {
    std::unique_lock<std::mutex> lk(_mtx);
    
    if(order->_type == ORDER_TYPE_TRANSACTION) {
      if(double_less(user->_balance, amount))
	return ERROR_INSUFFICIENT_AMOUNT;
      bs.change(-amount);
    }
    if(order->_type == ORDER_TYPE_WITHDRAW)  {
      if(double_less(user->_balance, (amount + order->_withdraw_fee)))
	return ERROR_INSUFFICIENT_AMOUNT; 
      bs.change(-(amount + order->_withdraw_fee));
    }
  }

  auto ret = _remotedb->add_order(order, user, touser);
  
  if(ret != 0)
    return ret;

  {
    std::unique_lock<std::mutex> lk(_mtx);
    
    if(order->_type == ORDER_TYPE_RECHARGE)
      user->_balance += amount;
    if(order->_type == ORDER_TYPE_TRANSACTION) 
      touser->_balance += amount;
    
    user->_orders.push_back(id);
    if(user->_orders.size() > 4096)
      user->_orders.pop_front();
  
    if(order->_type == ORDER_TYPE_RECHARGE)
      user->_recharges.push_back(id);
  
    if(order->_type == ORDER_TYPE_TRANSACTION) {
      touser->_orders.push_back(id);
      if(touser->_orders.size() > 4096)
	touser->_orders.pop_front();
    }
    
    if(order->_type == ORDER_TYPE_WITHDRAW)
      user->_withdraws.push_back(id);
  }
  
  _orders[ida] = order;

  bs.commit();
  
  return 0;    
}

std::shared_ptr<User> Payment::get_user_by_user_phone_mail(std::string name, std::string value)
{
  auto userid = -1;
    
  {
    std::unique_lock<std::mutex> lk(_mtx);
    if(_cache_user_phone_mail_2_id.count(value))
      userid = _cache_user_phone_mail_2_id[value];
  }
    
  if(userid >= 0)
    return get_user(userid);
  return _remotedb->get_user_by_user_phone_mail(name, value);
}
  
std::shared_ptr<User> Payment::get_user(long id)
{
  if(_users[id%maxcnt] && _users[id%maxcnt]->_id == id)
    return _users[id%maxcnt];
  return _remotedb->get_user(id);
}

std::shared_ptr<Order> Payment::get_order(long id)
{
  if(_orders[id%maxcnt] && _orders[id%maxcnt]->_id == id)
    return _orders[id%maxcnt];

  auto order = _remotedb->get_order(id);
  if(!order)
    return order;
  
  complete_order(order);
  _orders[id%maxcnt] = order;
  
  return order;
}

std::shared_ptr<User> Payment::cache_get_user(long id)
{
  if(_users[id%maxcnt] && _users[id%maxcnt]->_id == id)
    return _users[id%maxcnt];
  return std::shared_ptr<User>();
}

std::shared_ptr<Order> Payment::cache_get_order(long id)
{
  if(_orders[id%maxcnt] && _orders[id%maxcnt]->_id == id)
    return _orders[id%maxcnt];
  return std::shared_ptr<Order>();
}


std::shared_ptr<User> Payment::cache_get_user_by_user_phone_mail(std::string name, std::string value)
{
  auto userid = -1;
    
  {
    std::unique_lock<std::mutex> lk(_mtx);
    if(_cache_user_phone_mail_2_id.count(value))
      userid = _cache_user_phone_mail_2_id[value];
  }
    
  if(userid >= 0)
    return cache_get_user(userid);

  return std::shared_ptr<User>();
}

void Payment::start()
{
  static bool started = false;
  if(started)
    return;
  started = true;
    
  static volatile bool alive = true;
  std::thread thrd(&Payment::run, this, &alive);
  thrd.detach();
}


void Payment::load_user(std::shared_ptr<User> user)
{
  if(!user)
    return;
  _users[user->_id%maxcnt] = user;

  {
    std::unique_lock<std::mutex> lk(_mtx);
    auto & v = _cache_user_phone_mail_2_id;
    if(!user->_user.empty())
      v[user->_user] = user->_id;
    if(!user->_phone.empty())
      v[user->_phone] = user->_id;
    if(!user->_mail.empty())
      v[user->_mail] = user->_id;
  }
}

void Payment::load_order(std::shared_ptr<Order> order)
{
  if(!order)
    return;

  auto fromuser = cache_get_user(order->_from);
  auto touser = cache_get_user(order->_to);

  if(!fromuser || !touser)
    {
      LOG(WARNING, "load order failed: %s", order->to_json());
      return;
    }
  
  _orders[order->_id%maxcnt] = order;

  auto id = order->_id;

  {
    std::unique_lock<std::mutex> lk(_mtx);
  
    if(order->_type == ORDER_TYPE_RECHARGE) {
      fromuser->_orders.push_back(id);
      fromuser->_recharges.push_back(id);
    }
  
    if(order->_type == ORDER_TYPE_TRANSACTION) {
      fromuser->_orders.push_back(id);
      touser->_orders.push_back(id);
    }
  
    if(order->_type == ORDER_TYPE_WITHDRAW) {
      fromuser->_orders.push_back(id);
      fromuser->_withdraws.push_back(id);
    }
  }
}

bool Payment::load_multithread()
{
  LOG(INFO, "got into load_multithread");
  
  TimeElapsed te("load db");
  
  long onetime_to_load = 4096;
    
  auto lastuser = _remotedb->get_last_user();
  auto lastorder = _remotedb->get_last_order();

  if(lastuser)
    _usercnt = lastuser->_id+1;
  if(lastorder)
    _ordercnt = lastorder->_id+1;

  LOG(INFO, "load multithread:  %ld %ld", _usercnt.load(), _ordercnt.load());
  
  {
    auto remotedb_creater = [&]() -> void* {
      auto remotedb = new RemoteDB();
      for(;;)
    	{
    	  if(remotedb->connect())
    	    break;
    	  sleep(3);
    	}
      return (void*)remotedb;
    };

    auto remotedb_destroyer = [&](void* p) {
      delete (RemoteDB*)p;
    };

    ThreadPool tp(THREAD_CNT, remotedb_creater, remotedb_destroyer);

    {
      auto cnt = _usercnt / onetime_to_load;
      if((_usercnt % onetime_to_load) != 0)
	cnt++;
      for(long i=0; i<cnt; ++i)
	{
	  auto func = [&](void* state, long idx){
	    auto remotedb = (RemoteDB*)state;
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
      long start = 0;
      long count = _ordercnt;
      if(_ordercnt > maxcnt) {
	start = _ordercnt - maxcnt;
	count = maxcnt;
      }
      
      auto cnt = count / onetime_to_load;
      if((count % onetime_to_load) != 0)
	cnt++;
      
      for(long i=0; i<cnt; ++i)
	{
	  auto func = [&](void* state, long idx){
	    auto remotedb = (RemoteDB*)state;
	    dot("[");
	    auto v = remotedb->get_orders_by_limit(start+idx*onetime_to_load, onetime_to_load);
	    for(auto a : v)
	      load_order(a);
	  };	  
	  tp.push(std::bind(func, std::placeholders::_1, i));
	}
    }

  }
  
  LOG(INFO, "load success: %ld, %ld", long(_ordercnt), long(_usercnt));

  return true;
}

void Payment::start_epoll_server()
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
	  //auto rsp = msg->data() + linesplit;
	  auto rsp = msg->data();
	  auto fd = msg->client()->get_fd();

	  if(fd <= 0)
	    continue;
	  if(msg->client()->send_closed())
	    continue;

	  unsigned short len = rsp.length()+3;
	  auto cnt = send(fd, &len, 2, 0);
	  if(cnt != 2)
	    {
	      close(fd);
	      msg->client()->set_send_closed();
	      LOG(WARNING, "send header to client failed, will close the socket...");
	      continue;
	    }
	  
	  cnt = send(fd, rsp.c_str(), rsp.length()+1,0);
	  
	  if(cnt != rsp.length()+1)
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

void Payment::print_status(int now)
{
  TIMER(3);
  
  char buf[512];
  snprintf(buf, sizeof(buf), "qreq:%d, qrsp:%d", _qreqmsg.size(), _qrspmsg.size());
  write_file(buf);
}

void Payment::run(volatile bool * alive)
{
  _remotedb = new RemoteDB();
  
  for(;;)
    {
      if(_remotedb->connect())
	break;
      sleep(3);
    }

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
	  {
	    fflush(log2filehandle);
	    _remotedb->ping();
	    continue;
	  }

	auto req = msg->data();
      
	log_2_file(req);
      
	auto rsp = handle_request(req);
	if(rsp.empty())
	  continue;

	log_2_file(rsp);

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

      if(0 == now%60)
	dot(".");
      if(0 == now%3600)
	dot("@");
      
      print_status(now);
      sleep(1);
    }
  
  LOG(INFO, "run exit...");
}

void Payment::push_request(std::string req)
{
  _qreq.push(req);
}

std::string Payment::pop_response()
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

std::string check_or_paras(const std::map<std::string, std::string> & paras, std::initializer_list<const char*> v)
{
  for(auto p = v.begin(); p != v.end(); p++)
    if(paras.count(*p))
      return *p;
  return "";
}

void Payment::cache_order(std::shared_ptr<Order> order)
{
  complete_order(order);
  _orders[order->_id%maxcnt] = order;
}

void Payment::complete_order(std::shared_ptr<Order> order)
{
  
}

std::string Payment::handle_request(std::string req)
{
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

  // if(command == "self_update_order_status")
  //   {
  //     _remotedb->update_order_status(atol(paras["order_id"].c_str()), atoi(paras["order_status"].c_str()));
  //     return "";
  //   }

  // if(command == "self_update_project_received_crowdfunding")
  //   {
  //     _remotedb->update_project_received_crowdfunding(atol(paras["project_id"].c_str()), atof(paras["received_crowdfunding"].c_str()));
  //     return "";
  //   }

  
  if(command == "add_user")
    {
      //if(!check_paras(paras, {"login_pwd_hash", "recharge_addr", }))
      //if(!check_paras(paras, {"login_pwd_hash", "tx_pwd_hash", "login_pwd_salt", "tx_pwd_salt", "balance", "recharge_addr", "reward_point", "timestamp"}))
      //return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      auto idstr = check_or_paras(paras, {"user", "phone", "mail"});
      if(idstr.empty())
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      
      auto & kvs = paras;

      auto ret = add_user(kvs);
      if(ret == 0)
      	v.push_back("{\"user_id\":" + kvs["id"] + "}");
      return gen_rsp(command, msn, ret, v);
    }

  if(command == "update_user")
    {
      auto idstr = check_or_paras(paras, {"id", "user", "phone", "mail"});
      if(idstr.empty())
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      if(idstr == "null")
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);

      std::shared_ptr<User> user;
      if(paras.count("id"))
	user = get_user(atol(paras["id"].c_str()));
      else
	user = get_user_by_user_phone_mail(idstr, paras[idstr]);
      if(!user)
	return gen_rsp(command, msn, ERROR_NOT_EXIST_USER, v);

      if(paras.count("withdraw_addr") && paras["withdraw_addr"].length() > 512)
	return gen_rsp(command, msn, ERROR_TOO_LONG_WITHDRAW_ADDR, v);
      
      auto ret = update_user(user, paras);
      
      return gen_rsp(command, msn, ret, v);
    }

  if(command == "get_user")
    {
      auto idstr = check_or_paras(paras, {"id", "user", "phone", "mail"});
      if(idstr.empty())
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      if(idstr == "null")
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);

      std::shared_ptr<User> user;
      if(paras.count("id"))
	user = get_user(atol(paras["id"].c_str()));
      else
	user = get_user_by_user_phone_mail(idstr, paras[idstr]);
      if(!user)
	return gen_rsp(command, msn, ERROR_NOT_EXIST_USER, v);
      
      v.push_back(user->to_json());
      return gen_rsp(command, msn, 0, v);	
    }
  
  if(command == "add_order")
    {
      if(!check_paras(paras, {"type", "from", "to", "amount"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);

      auto type = atoi(paras["type"].c_str());
      if(type > 2)
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      
      if(type == 0) {
	if(!check_paras(paras, {"recharge_utxo"}))
	  return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      }
      if(type == 2) {
	if(!check_paras(paras, {"withdraw_addr", "withdraw_fee"}))
	  return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      }

      auto & kvs = paras;

      auto ret = add_order(kvs);
      if(ret == 0)
      	v.push_back("{\"order_id\":" + kvs["id"] + "}");
      return gen_rsp(command, msn, ret, v);
    }

  if(command == "get_order")
    {
      if(!check_paras(paras, {"id"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      auto order = get_order(atol(paras["id"].c_str()));
      if(!order)
	return gen_rsp(command, msn, ERROR_NOT_EXIST_ORDER, v);	
      v.push_back(order->to_json());
      return gen_rsp(command, msn, 0, v);	
    }

  if(command == "update_order")
    {
      if(!check_paras(paras, {"id"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      auto order = get_order(atol(paras["id"].c_str()));
      if(!order)
	return gen_rsp(command, msn, ERROR_NOT_EXIST_ORDER, v);
      auto ret = update_order(order, paras);
      return gen_rsp(command, msn, ret, v);	
    }
  
  if(command == "get_orders")
    {
      if(!check_paras(paras, {"type", "offset", "limit"}))
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      auto idstr = check_or_paras(paras, {"user_id", "user", "phone", "mail"});
      if(idstr.empty())
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      if(idstr == "null")
	return gen_rsp(command, msn, ERROR_INVALID_PARAS, v);
      
      long offset = atol(paras["offset"].c_str());
      int limit = atoi(paras["limit"].c_str());
      int type = atoi(paras["type"].c_str());
      long idx = 0;

      std::shared_ptr<User> user;
      if(paras.count("user_id"))
	user = get_user(atol(paras["user_id"].c_str()));
      else
	user = get_user_by_user_phone_mail(idstr, paras[idstr]);
      if(!user)
	return gen_rsp(command, msn, ERROR_NOT_EXIST_USER, v);

      std::list<long> orders;
      {
	std::unique_lock<std::mutex> lk(_mtx);
	if(type == ORDER_TYPE_RECHARGE)
	  orders = user->_recharges;
	else if(type == ORDER_TYPE_WITHDRAW)
	  orders = user->_withdraws;
	else
	  orders = user->_orders;
      }
      
      long last_orderid = 0;

      for(auto it=orders.rbegin(); it != orders.rend(); it++)
	{
	  auto order = get_order(*it);
	  if(!order)
	    continue;
	  last_orderid = order->_id;
	  complete_order(order);
	  if(type != 999 && type != order->_type)
	    continue;
	  if(idx++ < offset)
	    continue;
	  v.push_back(order->to_json());
	  limit--;
	  if(limit <= 0)
	    break;
	}
      
      if(limit > 0 && orders.size() > 1024)
	{
	  auto start_timestamp = user->_timestamp;
	  
	  offset -= idx;
	  if(offset < 0)
	    offset = 0;
	      
	  std::vector<int> types;
	  if(type != 999) 
	    types.push_back(type);
	  
	  auto orders = _remotedb->get_user_orders_limit(user, types, offset, limit, 0, start_timestamp, last_orderid);
	  
	  for(auto order : orders) 
	    v.push_back(order->to_json());
	}
      
      return gen_rsp(command, msn, 0, v);
    }
  
  return gen_rsp(command, msn, ERROR_INVALID_COMMAND, v);
}



