#ifndef __FINANCIAL_MANAGEMENT_H__
#define __FINANCIAL_MANAGEMENT_H__

#define THREAD_CNT 80

enum
  {
    ERROR_ADD_EXISTING_ORDER = 3002,
    ERROR_ADD_EXISTING_USER = 3003,
    ERROR_INSUFFICIENT_AMOUNT = 3012,
    ERROR_INSUFFICIENT_FEE = 3013,
    
    ERROR_INVALID_USER_ID = 3202,

    ERROR_MYSQL_REAL_QUERY_FAILED = 3401,
    ERROR_MYSQL_STORE_RESULT_FAILED = 3402,

    ERROR_NOT_EXIST_ORDER = 3501,
    ERROR_NOT_EXIST_USER = 3503,
    ERROR_EXIST_USER = 3514,
    ERROR_DUPLICATE_UTXO_CONFIRM = 3515,
    ERROR_UPDATE_ORDER_WITH_WRONG_KV = 3516,
    
    ERROR_INVALID_JSON = 3601,
    ERROR_INVALID_COMMAND = 3602,
    ERROR_INVALID_PARAS = 3603,

    ERROR_INVALID_REMOTEDB_COMMAND = 3604,
    ERROR_BUSY = 3605,
  };

enum
  {
    ORDER_TYPE_RECHARGE = 0,
    ORDER_TYPE_TRANSACTION = 1,
    ORDER_TYPE_WITHDRAW = 2
  };

class RemoteDB;

struct Order
{
  static std::shared_ptr<Order> create(std::map<std::string, std::string> & kvs);
  
  std::map<std::string, std::string> to_map();

  std::string to_json();
  
  int check_valid();
  
  long _id;
  int _type;
  long _from;
  long _to;
  double _amount;
  int _timestamp;
  std::string _withdraw_addr;
  std::string _recharge_utxo;
  std::string _withdraw_utxo;
  double _withdraw_fee;
  int _utxo_confirmed;

  std::string _legal_currency_name;
  double _legal_currency_value;
};


struct User
{
  static std::shared_ptr<User> create(std::map<std::string, std::string> & kvs);
  
  std::map<std::string, std::string> to_map();

  std::string to_json();

  int check_valid();

  long _id;
  std::string _user;
  std::string _login_pwd_hash;
  std::string _tx_pwd_hash;
  std::string _login_pwd_salt;
  std::string _tx_pwd_salt;
  double _balance;
  std::string _phone;
  std::string _mail;
  std::string _recharge_addr;
  int _reward_point;
  int _timestamp;
  std::string _device_token;
  std::string _withdraw_addr;
  
  std::list<long> _orders;
  std::list<long> _recharges;
  std::list<long> _withdraws;
};

struct Payment
{
  static Payment* get();
  Payment()
  {
    _users.resize(maxcnt);
    _orders.resize(maxcnt);
  }
  
  int add_user(std::map<std::string, std::string> & kvs);
  int update_user(std::shared_ptr<User> user, std::map<std::string, std::string> & kvs);

  int add_order(std::map<std::string, std::string> & kvs);
  int update_order(std::shared_ptr<Order> order, std::map<std::string, std::string> & kvs);
  
  std::shared_ptr<User> get_user_by_user_phone_mail(std::string name, std::string value);

  std::shared_ptr<User> get_user(long id);
  std::shared_ptr<Order> get_order(long id);

  std::shared_ptr<User> cache_get_user(long id);
  std::shared_ptr<Order> cache_get_order(long id);

  std::shared_ptr<User> cache_get_user_by_user_phone_mail(std::string name, std::string value);

  void complete_order(std::shared_ptr<Order> order);

  void cache_order(std::shared_ptr<Order> order);

  void start();
  bool load();
  bool load_multithread();

  void load_user(std::shared_ptr<User> a);
  void load_order(std::shared_ptr<Order> a);

  void start_server();
  void server_proc(utils::Queue<std::string>* qreq, utils::Queue<std::string>* qrsp);
  void start_epoll_server();

  void print_status(int);

  void run(volatile bool * alive);
  
  void push_request(std::string req);
  std::string pop_response();

  std::string handle_request(std::string req);

  std::atomic_long _ordercnt = 0;
  std::atomic_long _usercnt = 0;

  std::vector<std::shared_ptr<Order>>   _orders;
  std::vector<std::shared_ptr<User>>    _users;

  static const long maxcnt = 1*1024*1024;
  
  std::unordered_map<std::string, long> _cache_user_phone_mail_2_id;

  utils::Queue<std::string> _qreq;
  utils::Queue<std::string> _qrsp;

  utils::Queue<std::shared_ptr<Message>> _qreqmsg;
  utils::Queue<std::shared_ptr<Message>> _qrspmsg;
  
  std::mutex _mtx;
};


#endif


