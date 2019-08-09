#ifndef __REMOTEDB_HPP__
#define __REMOTEDB_HPP__

class User;
class Order;
class Task;

struct RemoteDBBase
{
  RemoteDBBase()
  {
    _mysql= mysql_init(NULL);
  }

  virtual ~RemoteDBBase()
  {
    mysql_close(_mysql);
  }
  
  std::string escape_string(std::string str);
  void zip_map(const std::map<std::string, std::string> & v, std::string & keys, std::string & vals);
  std::string join_map(const std::map<std::string, std::string> & v);

  void free_result();

  void print_mysql_error()
  {
    printf("Error(%d) [%s] \"%s\"", mysql_errno(_mysql), mysql_sqlstate(_mysql), mysql_error(_mysql));
  }

  void ping()
  {
    mysql_ping(_mysql);
  }
  
  bool do_connect();
  
  int do_query(const char* query, std::vector<std::vector<std::string>>* vv, bool);

  int do_select(const char* query, std::vector<std::string> keys, std::vector<std::map<std::string, std::string>>* vv, bool);

  std::vector<std::map<std::string, std::string>>
    get_record_by_limit(const char* table, const std::vector<std::string> & keys, long start, long cnt);
  
  long get_rows_count(const char* table);
  
  std::map<std::string, std::string>
    get_last_record(const char* table, const std::vector<std::string> & keys);

 protected:
  
   bool _connected = false;
   MYSQL *_mysql = NULL;
};

struct RemoteDB : public RemoteDBBase
{
  RemoteDB() {}
  virtual ~RemoteDB() {}

  bool connect();

  typedef std::shared_ptr<User> user_p;
  typedef std::shared_ptr<Order> order_p;
  
  typedef std::vector<user_p> user_v;
  typedef std::vector<order_p> order_v;
  
  user_p get_user(long id);
  order_p get_order(long id);

  int add_user(user_p o);
  int add_order(order_p o, user_p from_user, user_p to_user);
  
  int update_user(std::shared_ptr<User> user, std::map<std::string, std::string>& kvs);
  int update_order(std::shared_ptr<Order> order, std::map<std::string, std::string>& kvs);

  user_p get_user_by_user_phone_mail(std::string name, std::string value);
  
  long get_users_cnt();
  long get_orders_cnt();

  user_v get_users_by_limit(long start, long cnt);
  order_v get_orders_by_limit(long start, long cnt);

  int update_order_status(long orderid, int status);
  
  order_p get_last_order();
  user_p get_last_user();

  std::vector<long> get_user_orders(long userid);
  std::vector<long> get_user_history_orders(long userid, long end_orderid);

  order_v get_user_orders_limit(user_p user, std::vector<int> statuses, int offset, int count, int direction, int start_timestamp, long last_orderid);

  int add_feedback(std::map<std::string, std::string> & kvs);

private:
  
  user_p get_user_impl(long id, bool forupate, bool);
  order_p get_order_impl(long id, bool);
  user_p get_user_by_user_phone_mail_impl(std::string name, std::string value, bool);
  int add_order_impl(order_p order, bool);
  int add_user_impl(std::shared_ptr<User> o, bool);
  
  static std::vector<std::string> _userKeys;
  static std::vector<std::string> _orderKeys;

};

#endif
