#ifndef __EXCHANGE_MATCH_ENGINE__
#define __EXCHANGE_MATCH_ENGINE__

struct LocalDB;
struct RemoteDB;
struct BtcPriceUpdater;

namespace exchange {
  
  enum {
    TOKEN_BTC = 0,
    TOKEN_USDT,
    TOKEN_MAX,
  };

  enum {
    ORDER_TYPE_DELEGATE_BY_MARKET_PRICE = 0,
    ORDER_TYPE_DELEGATE_BY_LIMIT_PRICE,
    ORDER_TYPE_MAX,
  };

  enum {
    TX_STATUS_INIT = 0,
    TX_STATUS_DONE,
    TX_STATUS_MAX,
  };

  struct Configure
  {
    //static constexpr double initMarketRate = 0.00000005;
    static constexpr double initMarketRate = 0.5;
    static constexpr uint64_t minExchangeNum = 1;
    static constexpr uint64_t maxExchangeNum = 10000;
  };

  struct Order
  {
    static std::atomic<int> gcnt;
    
    Order(){}
    Order(long idx, int from, int to , int type, double rate, int num, int minNum, std::string user)
      : _idx(idx), _from(from), _to(to), _type(type), _rate(rate), _num(num), _minNum(minNum), _user(user)
    {
      _timeStamp = time(NULL);
      _deadline = _timeStamp + 3600;
      gcnt++;
    }
    ~Order(){
      /* if(!_matched && !_timeout) { */
      /* 	LOG(ERROR, "order is deleted wrongly: %s", string().c_str()); */
      /* 	dump(); */
      /* } */
      gcnt--;
    }

    bool is_order_alive()
    {
      return _matched == false && _timeout == false && _txidx == -1;
    }
    
    int check();
    
    std::string string()
    {
      char text[1024];
      snprintf(text, sizeof(text), "{idx:%ld,tokenfrom:%d,tokento:%d,type:%d,rate:%.8f,num:%lu,minNum:%lu,user:%s,timeStamp:%u,deadline:%u,matched:%d,timeout:%d,txidx:%ld}"
	       , _idx, _from, _to, _type, _rate, _num, _minNum, _user.c_str(), _timeStamp, _deadline, _matched, _timeout, _txidx);
      return std::string(text);
    }

    static std::shared_ptr<Order> create_from_string(std::string str)
    {
      //auto v = json_get_object(str);
      auto v = json2map(str);
      auto order = std::make_shared<Order>(
					   std::atol(v["idx"].c_str()),
					   std::atoi(v["tokenfrom"].c_str()),
					   std::atoi(v["tokento"].c_str()),
					   std::atoi(v["type"].c_str()),
					   std::atof(v["rate"].c_str()),
					   std::atol(v["num"].c_str()),
					   std::atol(v["minNum"].c_str()),
					   v["user"]);
      
      order->_timeStamp = std::atoi(v["timeStamp"].c_str());
      order->_deadline = std::atoi(v["deadline"].c_str());
      order->_matched = std::atoi(v["matched"].c_str());
      order->_timeout = std::atoi(v["timeout"].c_str());
      order->_txidx = std::atol(v["txidx"].c_str());
      
      return order;
    }
    
    void dump()
    {
      LOG(DEBUG, "%s", string().c_str());
    }
    
    long _idx;
    int _from;
    int _to;
    int _type;
    double _rate;
    uint64_t _num = 0;
    uint64_t _minNum = 0;
    std::string _user;
    uint32_t _timeStamp;
    uint32_t _deadline;
    bool _matched = false;
    bool _timeout = false;
    long _txidx = -1;

    //int _unspentOrderIdx = -1;
  };

  struct comparebynum
  {
    bool operator()(const std::shared_ptr<Order>& lhs, const std::shared_ptr<Order>& rhs) const
    {
      return lhs->_num < rhs->_num;
    }
  };

  struct comparebyrate1
  {
    bool operator()(const std::shared_ptr<Order>& lhs, const std::shared_ptr<Order>& rhs) const
    {
      return lhs->_rate < rhs->_rate;
    }
  };

  struct comparebyrate2
  {
    bool operator()(const std::shared_ptr<Order>& lhs, const std::shared_ptr<Order>& rhs) const
    {
      return !(lhs->_rate < rhs->_rate);
    }
  };

  struct comparebydeadline
  {
    bool operator()(const std::shared_ptr<Order>& lhs, const std::shared_ptr<Order>& rhs) const
    {
      return lhs->_deadline < rhs->_deadline;
    }
  };

  typedef std::shared_ptr<Order> OrderPtr;
  typedef std::multiset<OrderPtr, comparebynum>  OrderNumSet;
  typedef std::multiset<OrderPtr, comparebyrate1> OrderRateSet1;
  typedef std::multiset<OrderPtr, comparebyrate2> OrderRateSet2;
  
  struct Transaction
  {
    Transaction(long idx, OrderPtr d1, OrderPtr d2, double rate, uint64_t num)
      : _idx(idx), _rate(rate), _num(num)
    {
      _timeStamp = time(NULL);
      _order1 = d1->_idx;
      _order2 = d2->_idx;
    }

  Transaction(long idx, long d1, long d2, double rate, uint64_t num)
      : _idx(idx), _order1(d1), _order2(d2), _rate(rate), _num(num)
    {
      _timeStamp = time(NULL);
    }
    
    static std::shared_ptr<Transaction> create_from_string(std::string str)
    {
      //auto v = json_get_object(str);
      auto v = json2map(str);
      auto tx = std::make_shared<Transaction>(
					   std::atol(v["idx"].c_str()),
					   std::atol(v["order1"].c_str()),
					   std::atol(v["order2"].c_str()),
					   std::atof(v["rate"].c_str()),
					   std::atol(v["num"].c_str())
					      );
      
      tx->_timeStamp = std::atoi(v["timeStamp"].c_str());
      tx->_status = std::atoi(v["status"].c_str());
      
      return tx;
    }
    
    void dump()
    {
      LOG(DEBUG, "%s", string().c_str());
    }

    std::string string()
    {
      char text[1024];
      snprintf(text, sizeof(text), "{idx:%ld,order1:%ld,order2:%ld,rate:%.8f,num:%lu,timeStamp:%u,status:%d}"
	       , _idx, _order1, _order2, _rate, _num, _timeStamp, _status);
      return std::string(text);
    }
    
    long _idx;
    long _order1;
    long _order2;
    double _rate;
    uint64_t _num;
    uint32_t _timeStamp;
    int _status = 0;
  };

  typedef std::shared_ptr<Transaction> TransactionPtr;

  template<class OrderRateSet>
  struct OrderBook
  {
    typedef OrderRateSet OrderRateSet_t;
    
    OrderBook(bool isSeller)
      : _isSeller(isSeller) {}
  
    int insert(OrderPtr order);
    void remove_cached_invalid_order();
    void dump();

    size_t count()
    {
      return _marketPriceOrderQueue.size() + _limitPriceOrderSetByRate.size() + _retry4minNumQueue.size();
    }

    void clear_cache()
    {
      _marketPriceOrderSetByNum.clear();
      _limitPriceOrderMapByNum.clear();
    }

    bool _isSeller;
    
    std::list<OrderPtr> _marketPriceOrderQueue;
    OrderNumSet _marketPriceOrderSetByNum;

    OrderRateSet _limitPriceOrderSetByRate;
    std::map<uint64_t, std::shared_ptr<OrderRateSet>> _limitPriceOrderMapByNum;

    std::list<OrderPtr> _retry4minNumQueue;
  };

  struct MatchEngine
  {
    void run(volatile bool* alive);

    void set_queues(utils::Queue<OrderPtr>* in, utils::Queue<TransactionPtr>* out)
    {
      _qin = in;
      _qout = out;
    }

    //private:

    bool create_localdb();

    bool create_remotedb();

    void handle_missed_order_transactions();
    
    double market_rate()
    {
      return _marketRate;
    }

    bool is_seller(int from, int to)
    {
      return to == TOKEN_BTC;
    }
    
    int insert_order(OrderPtr order);
  
    void update_market_rate(double rate);

    bool is_order_rate_match(OrderPtr a, OrderPtr b);

    bool is_order_mininal_num_match(OrderPtr a, OrderPtr b)
    {
      auto num = std::min(a->_num, b->_num);
      return a->_minNum <= num && b->_minNum <= num;
    }
    
    bool is_order_match(OrderPtr a, OrderPtr b)
    {
      return is_order_rate_match(a, b) && is_order_mininal_num_match(a, b);
    }
      
    bool check_order_minimal_num(OrderPtr a, OrderPtr b);

    bool check_order_timeout(OrderPtr a)
    {
      return a->_timeout;
    }

    bool check_order_matched(OrderPtr a)
    {
      return a->_matched;
    }
    
    void match_handler(OrderPtr a, OrderPtr b);

    int handle_orders(OrderPtr a, OrderPtr b);

    template<class BOOK1, class BOOK2>
      int do_match(BOOK1 & book1, BOOK2 & book2);
    
    template<class BOOK1, class BOOK2>
      int do_one_match(BOOK1 & book1, BOOK2 & book2, OrderPtr order);

    utils::Queue<OrderPtr>* _qin{nullptr};
    utils::Queue<TransactionPtr>* _qout{nullptr};
    
    std::vector<double> _lastRates;
    double _marketRate = Configure::initMarketRate;
  
    OrderBook<OrderRateSet1> _sellerOrderBook{true};
    OrderBook<OrderRateSet2> _buyerOrderBook{false};

    std::multiset<OrderPtr, comparebydeadline> _timeoutQueue;

    LocalDB* _localdb = nullptr;
    RemoteDB* _remotedb = nullptr;
    BtcPriceUpdater* _btcPriceUpdater = nullptr;
    
    utils::Queue<std::pair<long, std::string>> _qorders;
    utils::Queue<std::pair<long, std::string>> _qtxs;
    utils::Queue<std::string> _qtxFails;

    long _orderStartIdx = 0;
    long _txStartIdx = 0;

    std::unordered_map<long, OrderPtr> _orders;
    std::unordered_map<long, TransactionPtr> _txs;
  };

  OrderNumSet::iterator
    num_set_find_nearest(uint64_t num, OrderNumSet & set);

  template<class OrderRateSet>
    typename std::map<uint64_t, std::shared_ptr<OrderRateSet>>::iterator
    num_map_find_nearest(uint64_t num, std::map<uint64_t, std::shared_ptr<OrderRateSet>> & map);
  
  template<class OrderRateSet>
    std::pair<typename OrderRateSet::iterator, typename OrderRateSet::iterator>
    find_valid_order_by_rate(double rate, std::shared_ptr<OrderRateSet> & set);
  
} // namespace exchange


#endif
