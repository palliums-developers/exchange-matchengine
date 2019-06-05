#ifndef __FINANCIAL_MANAGEMENT_H__
#define __FINANCIAL_MANAGEMENT_H__

#define THREAD_CNT 80

enum
  {
    ERROR_ADD_EXISTING_PROJECT = 3001,
    ERROR_ADD_EXISTING_ORDER = 3002,
    ERROR_ADD_EXISTING_USER = 3003,
    ERROR_ADD_ORDER_WITH_WRONG_PROJECT_NO = 3004,
    ERROR_ADD_ORDER_WITH_WRONG_USER_PUBLICKEY = 3005,
    ERROR_ADD_ORDER_CROWDFUNDING_FULL = 3006,
    ERROR_ADD_ORDER_CROWDFUNDING_NOT_START = 3007,
    ERROR_ADD_ORDER_CROWDFUNDING_FINISHED = 3008,
    ERROR_ADD_ORDER_TOO_LOW_AMOUNT = 3009,    
    ERROR_ADD_ORDER_TOO_HIGH_AMOUNT = 3010,
    ERROR_ADD_ORDER_CROWDFUNDING_BOOK_FULL = 3011,
    
    ERROR_INVALID_PROJECT_NO = 3101,
    ERROR_INVALID_ANNUALIZED_RETURN_RATE = 3102,
    ERROR_INVALID_INVESTMENT_PERIOD = 3103,
    ERROR_INVALID_MIN_INVEST_AMOUNT = 3104,
    ERROR_INVALID_MAX_INVEST_AMOUNT = 3105,
    ERROR_INVALID_TOTAL_CROWDFUNDING_AMOUNT = 3106,
    ERROR_INVALID_BOOKING_STARTING_TIME = 3107,
    ERROR_INVALID_CROWDFUNDING_ENDING_TIME = 3108,
    ERROR_INVALID_INTEREST_TYPE = 3109,
    ERROR_INVALID_BORROWER_INFO = 3110,
    ERROR_INVALID_CROWDFUNDING_ADDRESS = 3111,

    ERROR_INVALID_PROJECT_ID = 3201,
    ERROR_INVALID_USER_ID = 3202,
    ERROR_INVALID_BOOKING_AMOUNT = 3203,
    ERROR_INVALID_INVESTMENT_RETURN_ADDR = 3204,

    ERROR_INVALID_PUBLICKEY = 3301,

    ERROR_MYSQL_REAL_QUERY_FAILED = 3401,
    ERROR_MYSQL_STORE_RESULT_FAILED = 3402,

    ERROR_NOT_EXIST_ORDER = 3501,
    ERROR_NOT_EXIST_PROJECT_NO = 3502,
    ERROR_NOT_EXIST_USER_PUBLICKEY = 3503,
    ERROR_MISMATCH_ORDERID_USER_PUBLICKEY = 3504,
    ERROR_MISMATCH_ORDERID_PROJECT_ID = 3505,
    ERROR_MISMATCH_TXID = 3506,
    ERROR_CANNOT_CANCEL_PAYED_ORDER = 3507,
    ERROR_DUPLICATE_SET_TXID = 3508,
    ERROR_CONFIRM_NOT_PAYED_TX = 3509,
    ERROR_REFUND_UNCONFIRMED_TX = 3510,
    ERROR_ORDER_TIMEOUT = 3511,
    ERROR_PROJECT_NOT_CROWDFUND_SUCCESS = 3512,
    
    ERROR_INVALID_JSON = 3601,
    ERROR_INVALID_COMMAND = 3602,
    ERROR_INVALID_PARAS = 3603,

    ERROR_INVALID_REMOTEDB_COMMAND = 3604,
    ERROR_BUSY = 3605,
  };

enum 
  {
    PROJECT_STATUS_NOT_OPENED = 0,
    PROJECT_STATUS_CROWDFUNDING,
    PROJECT_STATUS_CROWDFUND_SUCCESS,
    PROJECT_STATUS_CROWDFUND_FAILURE,
    PROJECT_STATUS_CNT,
  };

enum
  {
    INTEREST_TYPE_AFTER_CROWDFUND = 0,
    INTEREST_TYPE_CNT,
  };

enum
  {
    BORROWER_INFO_FROM_CUSTOMER = 0,
    BORROWER_INFO_CNT,
  };

enum
  {
    ORDER_STATUS_BOOKING_SUCCESS = 0,
    ORDER_STATUS_PAYED_AUDITING = 1,
    ORDER_STATUS_AUDIT_SUCCESS = 2,
    ORDER_STATUS_AUDIT_FAILURE = 3,
    ORDER_STATUS_CANCELED = 4,
    ORDER_STATUS_PAYMENT_REFUNDED = 5,
    ORDER_STATUS_AUDIT_SUCCESS_TOO_LATE = 6, 
    ORDER_STATUS_AUDIT_TIMEOUT = 7,
    ORDER_STATUS_BOOKING_TIMEOUT = 8,
 };

class RemoteDB;

struct Project
{
  static std::shared_ptr<Project> create(std::map<std::string, std::string> & kvs);

  std::map<std::string, std::string> to_map();
  
  std::string to_json();

  int check_valid();
  
  long         _id;
  std::string  _no;
  std::string  _name;
  double       _annualized_return_rate = 0;
  int          _investment_period = 0;
  double       _min_invest_amount = 0;
  double       _max_invest_amount = 0;
  double       _total_crowdfunding_amount = 0;
  int          _booking_starting_time = 0;
  int          _crowdfunding_ending_time = 0;
  int          _status = 0;
  double       _received_crowdfunding = 0;
  double       _booked_crowdfunding = 0;
  int          _interest_type = 0;
  int          _borrower_info = 0;
  std::string  _crowdfunding_address;
  std::string  _product_description;

  std::string  _projecter_publickey;
  std::string  _financial_controller_publickey;
  std::string  _crowdfunding_payment_txid;

  std::vector<long> _orders;

  std::mutex _mtx;
};

struct Order
{
  static std::shared_ptr<Order> create(std::map<std::string, std::string> & kvs);
  
  std::map<std::string, std::string> to_map();

  std::string to_json();
  
  int check_valid();
  
  long        _id;
  long        _project_id = -1;
  long        _user_id = -1;
  int         _booking_timestamp;
  double      _booking_amount = 0;
  std::string _payment_txid;
  int         _payment_timestamp = 0;
  int         _audited_timestamp = 0;
  std::string _investment_return_addr;
  double      _accumulated_gain = 0;
  int         _status = 0;

  std::string _project_no;
  std::string _user_publickey;

  std::string _oracle_publickey;
  std::string _contract_address;
  std::string _contract_script;

  int          _vout;
  int          _mc;
  int          _ms;
  int          _version;
  int          _locktime;
  std::string  _script_oracle_address;
};

struct User
{
  static std::shared_ptr<User> create(std::map<std::string, std::string> & kvs);
  
  std::map<std::string, std::string> to_map();

  std::string to_json();

  int check_valid();
  
  long        _id;
  std::string _publickey;
  std::string _name;
  std::string _collections;

  std::vector<long> _orders;
};

struct FinancialManagement
{
  static FinancialManagement* get();
  FinancialManagement()
  {
    _projects.resize(maxcnt);
    _users.resize(maxcnt);
    _orders.resize(maxcnt);
  }
  
  int add_project(std::shared_ptr<Project> project);
  int add_user(std::string publickey);
  int add_order(std::map<std::string, std::string> & kvs);
  
  int check_order(std::shared_ptr<Order> order, std::string project_no, std::string publickey);
  int cancel_order(std::string project_no, std::string publickey, long orderid);
  int update_order_txid(long orderid, std::string project_no, std::string publickey, std::string txid, std::string investment_return_addr, int payment_timestamp, std::string oracle_publickey, std::string contract_address, std::string contract_script, int vout, int mc, int ms, int version, int locktime, std::string script_oracle_address);
  int order_txid_confirm(long orderid, std::string txid, std::string success);
  int payment_refund_success(long orderid, std::string txid);
  
  int collect(std::string publickey, std::string project_no);
  int cancel_collection(std::string publickey, std::string project_no);
  int get_user_collections(std::string publickey, std::string & collections);
  
  int get_orders_by_user_publickey(std::string publickey, std::vector<std::shared_ptr<Order>> & orders);
  std::shared_ptr<User> get_user_by_publickey(std::string publickey);
  std::shared_ptr<Project> get_project_by_no(std::string no);

  std::shared_ptr<Project> get_project(long id);
  std::shared_ptr<User> get_user(long id);
  std::shared_ptr<Order> get_order(long id);

  std::shared_ptr<Project> cache_get_project(long id);
  std::shared_ptr<User> cache_get_user(long id);
  std::shared_ptr<Order> cache_get_order(long id);

  std::shared_ptr<Project> cache_get_project_by_no(std::string no);
  std::shared_ptr<User> cache_get_user_by_publickey(std::string publickey);
  
  void start();
  bool load();

  void load_project(std::shared_ptr<Project> a);
  void load_user(std::shared_ptr<User> a);
  void load_order(std::shared_ptr<Order> a);

  void start_server();
  void server_proc(utils::Queue<std::string>* qreq, utils::Queue<std::string>* qrsp);
  void start_epoll_server();
  
  void handle_order_heap(int);
  void watch_new_project(int);
  void update_project_status(int);
  void print_status(int);
  
  void run(volatile bool * alive);
  
  void push_request(std::string req);
  std::string pop_response();

  std::string handle_request(std::string req);

  std::atomic_long _projectcnt = 0;
  std::atomic_long _ordercnt = 0;
  std::atomic_long _usercnt = 0;

  std::vector<std::shared_ptr<Project>> _projects;
  std::vector<std::shared_ptr<Order>>   _orders;
  std::vector<std::shared_ptr<User>>    _users;

  static const long maxcnt = 1*1024*1024;
  
  std::unordered_map<std::string, long> _cache_project_no2id;
  std::unordered_map<std::string, long> _cache_user_publickey2id;

  utils::Queue<std::string> _qreq;
  utils::Queue<std::string> _qrsp;

  utils::Queue<std::shared_ptr<Message>> _qreqmsg;
  utils::Queue<std::shared_ptr<Message>> _qrspmsg;
  
  std::multimap<int, std::shared_ptr<Order>> _order_txid_timeout_heap;
  std::multimap<int, std::shared_ptr<Order>> _order_confirm_timeout_heap;

  std::mutex _mtx;
};

const char* query(const char* req);

#endif


