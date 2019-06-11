#ifndef __REMOTEDB_HPP__
#define __REMOTEDB_HPP__

class Project;
class User;
class Order;
class Task;

struct RemoteDB
{
  RemoteDB()
  {
    _mysql= mysql_init(NULL);
  }

  ~RemoteDB()
  {
    mysql_close(_mysql);
  }
  
  bool connect();

  std::shared_ptr<Project> get_project(long id);
  std::shared_ptr<User>    get_user(long id);
  std::shared_ptr<Order>   get_order(long id);

  std::shared_ptr<Project> get_project_by_no(std::string no);
  std::shared_ptr<User>    get_user_by_publickey(std::string publickey);

  int add_user(std::shared_ptr<User> o);
  int add_order(std::shared_ptr<Order> o);

  std::shared_ptr<Project> get_project_impl(MYSQL* mysql, long id);
  std::shared_ptr<User>    get_user_impl(MYSQL* mysql,long id);
  std::shared_ptr<Order>   get_order_impl(MYSQL* mysql,long id);
  std::shared_ptr<Project> get_project_by_no_impl(MYSQL* mysql,std::string no);
  std::shared_ptr<User>    get_user_by_publickey_impl(MYSQL* mysql,std::string publickey);
  int add_user_impl(MYSQL* mysql, std::shared_ptr<User> o);
  int add_order_impl(MYSQL* mysql, std::shared_ptr<Order> o);
  
  std::vector<std::shared_ptr<Project>> get_projects(long id);

  long get_projects_cnt();
  long get_users_cnt();
  long get_orders_cnt();

  std::vector<std::shared_ptr<Project>> get_projects_by_limit(long start, long cnt);
  std::vector<std::shared_ptr<User>>    get_users_by_limit(long start, long cnt);
  std::vector<std::shared_ptr<Order>>   get_orders_by_limit(long start, long cnt);

  int update_order_status(long orderid, int status);
  int update_order_txid(long orderid, std::string txid, std::string investment_return_addr, int payment_timestamp, std::string oracle_publickey, std::string contract_address, std::string contract_script, int vout, int mc, int ms, int version, int locktime, std::string script_oracle_address);
  int update_order_confirm_timestamp(long orderid, int now);
  int update_project_status(long projectid, int status);
  int update_project_received_crowdfunding(long projectid, double amount);
  int update_crowdfunding_payment_txid(long projectid, std::string crowdfunding_payment_txid);
  
  int update_collections(long userid, std::string collections);

  int update_order_status_impl(MYSQL* mysql, long orderid, int status);
  int update_order_txid_impl(MYSQL* mysql, long orderid, std::string txid, std::string investment_return_addr, int payment_timestamp, std::string oracle_publickey, std::string contract_address, std::string contract_script, int vout, int mc, int ms, int version, int locktime, std::string script_oracle_address);
  int update_order_confirm_timestamp_impl(MYSQL* mysql, long orderid, int now);
  int update_project_status_impl(MYSQL* mysql, long projectid, int status);
  int update_project_received_crowdfunding_impl(MYSQL* mysql, long projectid, double amount);
  int update_crowdfunding_payment_txid_impl(MYSQL* mysql, long projectid, std::string crowdfunding_payment_txid);
  int update_collections_impl(MYSQL* mysql, long userid, std::string collections);
  
  std::shared_ptr<Order> get_last_order();
  std::shared_ptr<Project> get_last_project();
  std::shared_ptr<User> get_last_user();

  std::vector<long> get_project_orders(long projectid);
  std::vector<long> get_user_orders(long userid);
  std::vector<long> get_user_history_orders(long userid, long end_orderid);

  std::vector<std::shared_ptr<Order>> 
  get_project_orders_limit(std::shared_ptr<Project> project, std::vector<int> statuses, int offset, int count, int direction);
  std::vector<std::shared_ptr<Order>> 
  get_user_orders_limit(std::shared_ptr<User> user, std::vector<int> statuses, int offset, int count, int direction);

private:

  void print_mysql_error()
  {
    print_mysql_error_impl(_mysql);
  }

  void print_mysql_error_impl(MYSQL* mysql)
  {
    printf("Error1(%d) [%s] \"%s\"", mysql_errno(mysql), mysql_sqlstate(mysql), mysql_error(mysql));
  }

  bool do_connect();
  
  int do_query(const char* query, std::vector<std::vector<std::string>>* vv);

  int do_select(const char* query, std::vector<std::string> keys, std::vector<std::map<std::string, std::string>>* vv);

  int do_query_impl(MYSQL* mysql, const char* query, std::vector<std::vector<std::string>>* vv);
  int do_select_impl(MYSQL* mysql, const char* query, std::vector<std::string> keys, std::vector<std::map<std::string, std::string>>* vv);
  
  std::vector<std::map<std::string, std::string>>
    get_record_by_limit(const char* table, const std::vector<std::string> & keys, long start, long cnt);
  
  long get_rows_count(const char* table);
  
  std::map<std::string, std::string>
    get_last_record(const char* table, const std::vector<std::string> & keys);

  static std::vector<std::string> _projectKeys;
  static std::vector<std::string> _userKeys;
  static std::vector<std::string> _orderKeys;

  bool _connected = false;
  MYSQL *_mysql;
};

#endif
