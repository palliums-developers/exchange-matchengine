#ifndef __REMOTEDB_HPP__
#define __REMOTEDB_HPP__

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
  
  std::vector<std::map<std::string, std::string>>
    get_orders(long idx);
  
  std::vector<std::map<std::string, std::string>>
    get_transactions(long idx);

  int append_transaction(std::string tx)
  {
    _qtxins.push(tx);
  }
  
  bool append_order(const std::map<std::string, std::string> & v);
  bool append_transaction(const std::map<std::string, std::string> & v, long idx);

  void set_start_idxs(long orderidx, long txidx)
  {
    _startOrderIdx = orderidx;
    _startTxIdx = txidx;
  }

  void set_order_tx_queues(utils::Queue<std::pair<long, std::string>> * qorders, utils::Queue<std::pair<long, std::string>> * qtxs, utils::Queue<std::string>* qtxfails)
  {
    _qorders = qorders;
    _qtxs = qtxs;
    _qtxFails = qtxfails;
  }
  
  void run(volatile bool* alive);
  
private:

  void print_mysql_error()
  {
    printf("Error(%d) [%s] \"%s\"", mysql_errno(_mysql), mysql_sqlstate(_mysql), mysql_error(_mysql));
  }

  bool do_connect();
  
  bool do_query(const char* query, std::vector<std::vector<std::string>>* vv);

  bool do_select(const char* query, std::vector<std::string> keys, std::vector<std::map<std::string, std::string>>* vv);
  
  std::vector<std::string> _orderKeys;
  std::vector<std::string> _txKeys;
  bool _connected = false;
  MYSQL *_mysql;

  long _startOrderIdx = 0;
  long _startTxIdx = 0;
  
  utils::Queue<std::pair<long, std::string>> * _qorders;
  utils::Queue<std::pair<long, std::string>> * _qtxs;

  utils::Queue<std::string> _qtxins;
  utils::Queue<std::string>* _qtxFails;
};

#endif
