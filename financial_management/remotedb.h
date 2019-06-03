#ifndef __REMOTEDB_HPP__
#define __REMOTEDB_HPP__

class Project;
class User;
class Order;

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

  std::vector<std::shared_ptr<Project>> get_projects(long id);

  long get_projects_cnt();
  long get_users_cnt();
  long get_orders_cnt();

  std::vector<std::shared_ptr<Project>> get_projects_by_limit(long start, long cnt);
  std::vector<std::shared_ptr<User>>    get_users_by_limit(long start, long cnt);
  std::vector<std::shared_ptr<Order>>   get_orders_by_limit(long start, long cnt);

  int update_order_status(long orderid, int status);
  int update_order_txid(long orderid, std::string txid, std::string investment_return_addr, int payment_timestamp);
  int update_order_confirm_timestamp(long orderid, int now);
  int update_project_status(long projectid, int status);
  int update_project_received_crowdfunding(long projectid, double amount);
  
  int update_collections(long userid, std::string collections);
  
private:

  void print_mysql_error()
  {
    printf("Error(%d) [%s] \"%s\"", mysql_errno(_mysql), mysql_sqlstate(_mysql), mysql_error(_mysql));
  }

  bool do_connect();
  
  int do_query(const char* query, std::vector<std::vector<std::string>>* vv);

  int do_select(const char* query, std::vector<std::string> keys, std::vector<std::map<std::string, std::string>>* vv);

  std::vector<std::map<std::string, std::string>>
    get_record_by_limit(const char* table, const std::vector<std::string> & keys, long start, long cnt);
  
  long get_rows_count(const char* table);
  
  std::map<std::string, std::string>
    get_first_record(const char* table, const std::vector<std::string> & keys);

  std::vector<std::string> _projectKeys;
  std::vector<std::string> _userKeys;
  std::vector<std::string> _orderKeys;
  
  bool _connected = false;
  MYSQL *_mysql;
};

#endif
