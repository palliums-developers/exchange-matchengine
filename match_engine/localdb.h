#ifndef __LOCALDB_HPP__
#define __LOCALDB_HPP__

struct LocalDB
{
  LocalDB(const char* path);
  
  void run(volatile bool* alive);
  int load();
  
  int append_order(long idx, std::string order)
  {
    _qorders.push(std::pair<long, std::string>(idx, order));
  }

  int append_transaction(long idx, std::string tx)
  {
    _qtxs.push(std::pair<long, std::string>(idx, tx));
  }
  
  void set_handlers(std::function<int(std::string)>  orderhdr,
		    std::function<bool(std::string)> isOrderAlive,
		    std::function<bool(std::string)> isOrderTimeout4remove,
		    std::function<bool(std::string)> isTxTimeout4remove)
  {
    _orderHandler = orderhdr;
    _isOrderAlive = isOrderAlive;
    _isOrderTimeout4remove = isOrderTimeout4remove;
    _isTxTimeout4remove = isTxTimeout4remove;
  }

  rocksdb::DB* raw_db()
  {
    return _db;
  }

  std::string string()
  {
    char buf[80];
    sprintf(buf, "{orderCheckpoint:%ld, orderRemove:%ld, txRemove:%ld}", _orderCheckpoint, _orderRemove, _txRemove);
    return std::string(buf);
  }
  
private:
  
  std::function<int(std::string)> _orderHandler;
  std::function<bool(std::string)> _isOrderAlive;
  std::function<bool(std::string)> _isOrderTimeout4remove;
  std::function<bool(std::string)> _isTxTimeout4remove;
  
  utils::Queue<std::pair<long, std::string>> _qorders;
  utils::Queue<std::pair<long, std::string>> _qtxs;
  
  long _orderCheckpoint = 0;
  long _orderRemove = 0;
  long _txRemove = 0;
  
  rocksdb::DB* _db = nullptr;
};

#endif
