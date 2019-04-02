#include <iostream>
#include <string>
#include <vector>
#include <list>
#include <set>
#include <map>
#include <queue>
#include <unordered_map>
#include <functional>
#include <algorithm>
#include <mutex>
#include <thread>
#include <atomic>
#include <condition_variable>

#include <unistd.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#include <rocksdb/db.h>
#include <mariadb/mysql.h>

#include "../utils/utilities.h"

#include "localdb.h"
#include "remotedb.h"
#include "btcmarketprice.h"

#include "engine.h"

namespace exchange {

  std::atomic<int> Order::gcnt(0);

  int Order::check()
  {
    if(_from == _to)
      return 1;
    if(_from < 0 || _from >= TOKEN_MAX)
      return 2;
    if(_to < 0 || _to >= TOKEN_MAX)
      return 3;
    if(_rate < 0)
      return 4;
    if(_num < Configure::minExchangeNum || _num > Configure::maxExchangeNum)
      return 5;
    if(_minNum > _num)
      return 6;
    if(_user.empty())
      return 7;
    return 0;
  }
  
  template<class OrderRateSet>
  int OrderBook<OrderRateSet>::insert(OrderPtr order)
  {
    if(order->_type == ORDER_TYPE_DELEGATE_BY_MARKET_PRICE)
      {
	_marketPriceOrderQueue.push_back(order);
	_marketPriceOrderSetByNum.insert(order);
	return 0;
      }

    if(order->_type == ORDER_TYPE_DELEGATE_BY_LIMIT_PRICE)
      {
	_limitPriceOrderSetByRate.insert(order);
	if(0 == _limitPriceOrderMapByNum.count(order->_num))
	  _limitPriceOrderMapByNum[order->_num] = std::make_shared<OrderRateSet>();
	_limitPriceOrderMapByNum[order->_num]->insert(order);
	return 0;
      }
      
    return -1;
  }

  template<class OrderRateSet>
  void OrderBook<OrderRateSet>::remove_cached_invalid_order()
  {
    std::function<bool(OrderPtr)> func =
      [](OrderPtr order){ return order->_matched || order->_timeout; };

    remove_if_ex(_marketPriceOrderQueue, func);
    remove_if_ex(_marketPriceOrderSetByNum, func);
    remove_if_ex(_limitPriceOrderSetByRate, func);
    for(auto it = _limitPriceOrderMapByNum.begin(); it != _limitPriceOrderMapByNum.end(); ++it)
      remove_if_ex(*(it->second), func);
    remove_if_ex(_retry4minNumQueue, func);
  }

  template<class OrderRateSet>
  void OrderBook<OrderRateSet>::dump()
  {
    printf("\nisSeller: %d\n", _isSeller);
    printf("marketPriceOrderQueue: %lu\n", _marketPriceOrderQueue.size());
    printf("marketPriceOrderSetByNum: %lu\n", _marketPriceOrderSetByNum.size());
    printf("limitPriceOrderSetByRate: %lu\n", _limitPriceOrderSetByRate.size());
    
    size_t total = 0;
    for(auto & a : _limitPriceOrderMapByNum)
      {
	if(a.second->size() != 0)
	  {
	    //printf("    limitPriceOrderMapByNum %lu: %lu\n", a.first, a.second->size());
	    total += a.second->size();
	  }
      }
    printf("limitPriceOrderMapByNum %lu\n", total);
    
    printf("retry4minNumQueue: %lu\n", _retry4minNumQueue.size());
  }

  bool MatchEngine::create_localdb()
  {
    _localdb = new LocalDB("localdb");

    auto orderHandler = [&](std::string a)
      {
	auto now = uint32_t(time(NULL));
	auto order = Order::create_from_string(a);
	if(order->is_order_alive() && order->_deadline > now)
	  {
	    _orders[order->_idx] = order;
	    insert_order(order);
	  }
	return 0;
      };
    
    auto isOrderAlive = [&](std::string a)
      {
	auto order = Order::create_from_string(a);
	return order->is_order_alive();
      };

    auto isOrderTimeout4remove = [&](std::string a)
      {
	auto now = uint32_t(time(NULL));
	auto order = Order::create_from_string(a);
	return (now - order->_timeStamp) >= 10*24*3600;
      };

    auto isTxTimeout4remove = [&](std::string a)
      {
	auto now = uint32_t(time(NULL));
	auto tx = Transaction::create_from_string(a);
	return (now - tx->_timeStamp) >= 10*24*3600;
      };
    
    _localdb->set_handlers(orderHandler, isOrderAlive, isOrderTimeout4remove, isTxTimeout4remove);
    
    _localdb->get_checkpoints(&_orderStartIdx, &_txStartIdx);
    
    return true;
  }

  bool MatchEngine::create_remotedb()
  {
     _remotedb = new RemoteDB();

     _remotedb->set_order_tx_queues(&_qorders, &_qtxs, &_qtxFails);
     
     _remotedb->set_start_idxs(_orderStartIdx, _txStartIdx);

     if(!_remotedb->connect())
       return false;
     
     return true;
  }

  void MatchEngine::handle_missed_order_transactions()
  {
    while(!_qorders.empty())
      {
	auto orderp = _qorders.pop();
	LOG(INFO, "order is loaded...: %s", orderp.second.c_str());

	auto order = _localdb->get_order(orderp.first);
	
	if(order.empty())
	  _localdb->set_order(orderp.first, orderp.second);
      }
    
    while(!_qtxs.empty())
      {
	auto txp = _qtxs.pop();
	LOG(INFO, "transaction is loaded...: %s", txp.second.c_str());
	
	auto tx = Transaction::create_from_string(txp.second);
	_localdb->set_transaction(txp.first, txp.second);

	{
	  auto idx = tx->_order1;
	  auto orderstr = _localdb->get_order(idx);
	  if(!orderstr.empty())
	    {
	      auto order = Order::create_from_string(orderstr);
	      order->_matched = true;
	      order->_txidx = txp.first;
	      _localdb->set_order(idx, order->string());
	    }
	}
	    
	{
	  auto idx = tx->_order2;
	  auto orderstr = _localdb->get_order(idx);
	  if(!orderstr.empty())
	    {
	      auto order = Order::create_from_string(orderstr);
	      order->_matched = true;
	      order->_txidx = txp.first;
	      _localdb->set_order(idx, order->string());
	    }
	}
      }
  }
  
  void MatchEngine::run(volatile bool* alive)
  {
    _btcPriceUpdater = new BtcPriceUpdater();
    
    if(!_btcPriceUpdater->load())
      return;

    _marketRate = ((double)1.0)/_btcPriceUpdater->get_btc_price_by_usd();
    
    if(!create_localdb())
      return;

    if(!create_remotedb())
      return;
    
    _remotedb->load();

    handle_missed_order_transactions();
    
    _localdb->load();

    std::thread localdb_thrd([&]{ _localdb->run(alive); });
    
    std::thread remotedb_thrd([&]{ _remotedb->run(alive); });

    std::thread btcPriceUpdater_thrd([&]{ _btcPriceUpdater->run(alive); });

    LOG(INFO, "MatchEngine::run start!");
    
    while(*alive)
      {
	auto now = uint32_t(time(NULL));

	for(;;)
	  {
	    if(_timeoutQueue.empty())
	      break;
	
	    auto pos = _timeoutQueue.begin();
	    auto order = *pos;
	
	    if(order->_matched) {
	      _timeoutQueue.erase(pos);
	      continue;
	    }
	
	    if(order->_deadline > now)
	      break;
	
	    order->_timeout = true;
	    _timeoutQueue.erase(pos);
	    _orders.erase(order->_idx);
	    _localdb->append_order(order->_idx, order->string());
	    
	    if(!order->_matched)
	      LOG(INFO, "order timeout! : %s", order->string().c_str());
	  }
	
	while(!_qorders.empty())
	  {
	    auto orderp = _qorders.pop();
	    LOG(INFO, "order is coming...: %s", orderp.second.c_str());
	    
	    auto order = Order::create_from_string(orderp.second);

	    if(order->is_order_alive() && order->_deadline > now && _orders.count(order->_idx) == 0)
	      {
		insert_order(order);
		_orders[order->_idx] = order;
		_localdb->append_order(orderp.first, orderp.second);
	      }
	  }
	
	while(!_qtxs.empty())
	  {
	    auto txp = _qtxs.pop();
	    auto tx = Transaction::create_from_string(txp.second);
	    _localdb->append_transaction(txp.first, txp.second);

	    {
	      auto idx = tx->_order1;
	      if(_orders.count(idx))
		{
		  _orders[idx]->_matched = true;
		  _orders[idx]->_txidx = tx->_idx;
		  _localdb->append_order(idx, _orders[idx]->string());
		  _orders.erase(idx);
		}
	    }
	    
	    {
	      auto idx = tx->_order2;
	      if(_orders.count(idx))
		{
		  _orders[idx]->_matched = true;
		  _orders[idx]->_txidx = tx->_idx;
		  _localdb->append_order(idx, _orders[idx]->string());
		  _orders.erase(idx);
		}
	    }
	  }

	while(!_qtxFails.empty())
	  {
	    auto txstr = _qtxFails.peek();

	    if(txstr.empty())
	      break;
	    
	    auto tx = Transaction::create_from_string(txstr);

	    if((tx->_timeStamp - now) < 8)
	      break;

	    _qtxFails.pop();
	    
	    auto idx = tx->_order1;
	    if(_orders.count(idx) > 0 && _orders[idx]->_txidx != -1)
	      {
		_orders[idx]->_matched = false;
		insert_order(_orders[idx]);
	      }
	    
	    idx = tx->_order2;
	    if(_orders.count(idx) > 0 && _orders[idx]->_txidx != -1)
	      {
		_orders[idx]->_matched = false;
		insert_order(_orders[idx]);
	      }
	  }
	
	if(_buyerOrderBook.count() == 0 || _sellerOrderBook.count() == 0)
	  {
	    if(_buyerOrderBook.count() == 0)
	      _buyerOrderBook.clear_cache();
	    if(_sellerOrderBook.count() == 0)
	      _sellerOrderBook.clear_cache();
	    usleep(100*1000);
	    continue;
	  }
	  
	static int cnt = 0;
	int ret;
	if(cnt++%2)
	  ret = do_match(_sellerOrderBook, _buyerOrderBook);
	else
	  ret = do_match(_buyerOrderBook, _sellerOrderBook);

	if(ret != 0) 
	  LOG(DEBUG, "do_match return with %d", ret);
	
	static int failcnt = 0;
	failcnt = (ret == 0) ? 0 : (failcnt+1);
	if(failcnt >= 10)
	  {
	    failcnt = 0;
	    usleep(100*1000);
	    dot("*");
	  }
	
	static uint32_t last = 0;
	if(now % 8 == 0 && now != last)
	  {
	    last = now;
	    
	    dot(".");

	    auto rate = _btcPriceUpdater->get_btc_price_by_usd();
	    if(rate > 0)
	      _marketRate = ((double)1.0)/rate;
	    
	    _sellerOrderBook.remove_cached_invalid_order();
	    _buyerOrderBook.remove_cached_invalid_order();
	    
	    std::function<bool(OrderPtr)> func = [](OrderPtr order){ return order->_matched; };
	    remove_if_ex(_timeoutQueue, func);

	    if(Order::gcnt.load() > 0)
	      LOG(INFO, "order gcnt: %d, buyers:%lu, sellers:%lu", Order::gcnt.load(), _buyerOrderBook.count(), _sellerOrderBook.count());
	  }
      }

    localdb_thrd.join();
    remotedb_thrd.join();
    btcPriceUpdater_thrd.join();
    
    LOG(INFO, "MatchEngine::run exit!");
  }

  int MatchEngine::insert_order(OrderPtr order)
  {
    int ret = order->check();
    
    if(ret != 0)
      {
	LOG(WARNING, "invalid order, error:%d : %s", ret, order->string().c_str());
	return ret;
      }
    
    _timeoutQueue.insert(order);
    
    if(is_seller(order->_from, order->_to))
      return _sellerOrderBook.insert(order);
    else
      return _buyerOrderBook.insert(order);
    
    return ret;
  }
  
  void MatchEngine::update_market_rate(double rate)
  {
    return;
    
    static uint64_t totalcnt = 0;
    int cnt = 10;
    
    if(_lastRates.size() < cnt) 
      _lastRates.push_back(rate);
    else 
      _lastRates[totalcnt%cnt] = rate;
    
    cnt = _lastRates.size();
    double sum = 0;
    
    for(auto a : _lastRates)
      sum += a;
    
    totalcnt++;
    
    _marketRate = sum/cnt;
  }
  
  bool MatchEngine::is_order_rate_match(OrderPtr a, OrderPtr b)
  {
    auto sellerOrder = a->_to == TOKEN_BTC ? a : b;
    auto buyerOrder  = a->_to != TOKEN_BTC ? a : b;
      
    if(sellerOrder->_type == ORDER_TYPE_DELEGATE_BY_MARKET_PRICE)
      sellerOrder->_rate = _marketRate;
    if(buyerOrder->_type == ORDER_TYPE_DELEGATE_BY_MARKET_PRICE)
      buyerOrder->_rate = _marketRate;
      
    return buyerOrder->_rate >= sellerOrder->_rate;
  }
  
  bool MatchEngine::check_order_minimal_num(OrderPtr a, OrderPtr b)
  {
    auto sellerOrder = a->_to == TOKEN_BTC ? a : b;
    auto buyerOrder  = a->_to != TOKEN_BTC ? a : b;
      
    auto ret = true;
    auto num = std::min(sellerOrder->_num, buyerOrder->_num);

    if(sellerOrder->_minNum > num || buyerOrder->_minNum > num)
      {
	_sellerOrderBook._retry4minNumQueue.push_back(sellerOrder);
	_buyerOrderBook._retry4minNumQueue.push_back(buyerOrder);
	ret = false;
      }
      
    return ret;
  }

  void MatchEngine::match_handler(OrderPtr a, OrderPtr b)
  {
    auto sellerOrder = a->_to == TOKEN_BTC ? a : b;
    auto buyerOrder  = a->_to != TOKEN_BTC ? a : b;

    if(sellerOrder->_type == ORDER_TYPE_DELEGATE_BY_MARKET_PRICE)
      sellerOrder->_rate = _marketRate;
    if(buyerOrder->_type == ORDER_TYPE_DELEGATE_BY_MARKET_PRICE)
      buyerOrder->_rate = _marketRate;

    if(check_order_matched(a) || check_order_matched(b))
      {
	LOG(WARNING, "match_handler check_order_matched failed");
	a->dump();
	b->dump();
	return;
      }
      
    if(!is_order_rate_match(a, b))
      {
	LOG(WARNING, "match_handler is_order_rate_match failed");
	a->dump();
	b->dump();
	return;
      }
      
    if(check_order_timeout(a) || check_order_timeout(b))
      {
	LOG(WARNING, "match_handler check_order_timeout failed");
	a->dump();
	b->dump();
	return;
      }

    if(a->_matched || b->_matched)
      {
	LOG(WARNING, "match_handler check matched failed");
	a->dump();
	b->dump();
	return;
      }
      
    if(!check_order_minimal_num(a, b))
      {
	LOG(WARNING, "match_handler check_order_minimal_num failed");
	a->dump();
	b->dump();
	return;
      }
      
    double rate = _marketRate;

    if(_marketRate < sellerOrder->_rate)
      rate = sellerOrder->_rate;
    else if(_marketRate > buyerOrder->_rate)
      rate = buyerOrder->_rate;
      
    update_market_rate(rate);

    sellerOrder->_matched = true;
    buyerOrder->_matched = true;

    static int txidx = 0;
    auto ptx = std::make_shared<Transaction>(txidx++, sellerOrder, buyerOrder, rate, std::min(sellerOrder->_num, buyerOrder->_num));

    LOG(INFO, "new transacton: %s", ptx->string().c_str());

    _remotedb->append_transaction(ptx->string());
  }
  
  int MatchEngine::handle_orders(OrderPtr a, OrderPtr b)
  {
    LOG(DEBUG, "handle_orders start...");
      
    if(check_order_timeout(a)) 
      return 11;

    if(check_order_timeout(b)) 
      return 12;

    if(check_order_matched(a)) 
      return 13;

    if(check_order_matched(b)) 
      return 14;

    if(!is_order_rate_match(a, b))
      {
	LOG(ERROR, "handle_orders is_order_rate_match failed");
	return 15;
      }
      
    if(!check_order_minimal_num(a, b)) 
      return 16;
      
    match_handler(a, b);
	
    static int i = 0;
    LOG(INFO, "handle_orders success... %d", i++);
      
    return 0;
  };

  template<class BOOK1, class BOOK2>
  int MatchEngine::do_match(BOOK1 & book1, BOOK2 & book2)
  {
    int ret = -1;
    OrderPtr order;
      
    while(!book1._marketPriceOrderQueue.empty())
      {
	order = book1._marketPriceOrderQueue.front();
	book1._marketPriceOrderQueue.pop_front();
	if(!order->is_order_alive()) 
	  continue;
	ret = do_one_match(book1, book2, order);
	if(ret == 0) return 0;
	book1._retry4minNumQueue.push_back(order);
	break;
      }

    while(!book1._limitPriceOrderSetByRate.empty())
      {
	auto pos = book1._limitPriceOrderSetByRate.begin();
	order = *pos;
	book1._limitPriceOrderSetByRate.erase(pos);
	if(!order->is_order_alive()) 
	  continue;
	ret = do_one_match(book1, book2, order);
	if(0 == ret) return 0;
	book1._retry4minNumQueue.push_back(order);
	break;
      }

    while(!book1._retry4minNumQueue.empty())
      {
	order = book1._retry4minNumQueue.front();
	book1._retry4minNumQueue.pop_front();
	if(!order->is_order_alive()) 
	  continue;
	ret = do_one_match(book1, book2, order);
	if(0 == ret) return 0;
	book1._retry4minNumQueue.push_back(order);
	break;
      }

    return ret;
  }

  template<class BOOK1, class BOOK2>
  int MatchEngine::do_one_match(BOOK1 & book1, BOOK2 & book2, OrderPtr order)
  {
    LOG(DEBUG, "do_match @ 2");
      
    {
      auto & v = book2._marketPriceOrderSetByNum;
      auto pair = v.equal_range(order);
      for(auto pos=pair.first; pos!=pair.second; )
	{
	  auto order2 = *pos;
	  if(!order2->is_order_alive())
	    {
	      pos = v.erase(pos);
	      continue;
	    }
	  if(is_order_match(order, order2))
	    {
	      v.erase(pos);
	      return handle_orders(order, order2);
	    }
	  pos++;
	}
    }

    LOG(DEBUG, "do_match @ 3");
      
    {
      auto & v = book2._limitPriceOrderMapByNum;
      auto pos = v.find(order->_num);
      if(pos != v.end())
	{
	  auto & v1 = pos->second;
	  auto pair = find_valid_order_by_rate(order->_rate, v1);
	  for(auto pos1=pair.first; pos1!=pair.second;  )
	    {
	      auto order2 = *pos1;
	      if(!order2->is_order_alive())
		{
		  pos1 = v1->erase(pos1);
		  continue;
		}
	      if(is_order_match(order, order2))
		{
		  v1->erase(pos1);
		  return handle_orders(order, order2);
		}
	      pos1++;
	    }
	}
    }

    LOG(DEBUG, "do_match @ 4");
      
    {
      auto & v = book2._marketPriceOrderSetByNum;
      while(!v.empty())
	{
	  auto pos = num_set_find_nearest(order->_num, v);
	  if(pos == v.end())
	    break;
	  auto order2 = *pos;
	  if(!order2->is_order_alive())
	    {
	      v.erase(pos);
	      continue;
	    }
	  if(!is_order_match(order, order2))
	    break;
	  v.erase(pos);
	  return handle_orders(order, order2);
	}
    }

    LOG(DEBUG, "do_match @ 5");

    {
      auto & v = book2._limitPriceOrderMapByNum;
      auto pos = num_map_find_nearest(order->_num, v);
      if(pos != v.end())
	{
	  auto & v1 = pos->second;
	  auto pair = find_valid_order_by_rate(order->_rate, v1);
	  for(auto pos1=pair.first; pos1!=pair.second;  )
	    {
	      auto order2 = *pos1;
	      if(!order2->is_order_alive())
		{
		  pos1 = v1->erase(pos1);
		  continue;
		}
	      if(is_order_match(order, order2))
		{
		  v1->erase(pos1);
		  return handle_orders(order, order2);
		}
	      pos1++;
	    }
	}
    }

    LOG(DEBUG, "do_match @ 6");
      
    {
      auto & v = book2._limitPriceOrderSetByRate;
      while(!v.empty())
	{
	  auto pos = v.begin();
	  auto order2 = *pos;
	  if(!order2->is_order_alive())
	    {
	      v.erase(pos);
	      continue;
	    }
	  if(!is_order_match(order, order2))
	    break;
	  v.erase(pos);
	  return handle_orders(order, order2);
	}
    }

    LOG(DEBUG, "do_match @ 7");

    auto & v = book2._retry4minNumQueue;
    for(auto pos=v.begin(); pos!=v.end(); )
      {
	auto order2 = *pos;
	if(!order2->is_order_alive())
	  {
	    pos = v.erase(pos);
	    continue;
	  }
	if(is_order_match(order, order2))
	  {
	    v.erase(pos);
	    return handle_orders(order, order2);
	  }
	pos++;
      }
      
    LOG(DEBUG, "do_match @ 8");

    return 4;
  }

  OrderNumSet::iterator num_set_find_nearest(uint64_t num, OrderNumSet & set)
  {
    if(set.empty())
      return set.end();
    if(set.size() == 1)
      return set.begin();

    static std::shared_ptr<Order> key(new Order);
    
    key->_num = num;
    auto pos = set.lower_bound(key);
    if(pos == set.begin())
      return pos;
    if(pos == set.end()) 
      return --pos;
    if((*pos)->_num == num)
      return pos;
    
    auto posleft = --pos;
    auto numleft = (*posleft)->_num;
    auto numright = (*pos)->_num;
    auto invl = (numleft > num) ? (numleft - num) : (num - numleft);
    auto invr = (numright > num) ? (numright - num) : (num - numright);
    
    return (invl <= invr) ? posleft : pos;
  }

  template<class OrderRateSet>
  typename std::map<uint64_t, std::shared_ptr<OrderRateSet>>::iterator
  num_map_find_nearest(uint64_t num, std::map<uint64_t, std::shared_ptr<OrderRateSet>> & map)
  {
    if(map.empty())
      return map.end();
    if(map.size() == 1)
      return map.begin();
    
    auto pos = map.lower_bound(num);
    if(pos == map.begin()) 
      return pos;
    if(pos == map.end()) 
      return --pos;
    if(pos->first == num)
      return pos;
    
    auto posleft = --pos;
    auto invl = (posleft->first > num) ? (posleft->first - num) : (num - posleft->first) ;
    auto invr = (pos->first > num) ? (pos->first - num) : (num - pos->first) ;
    
    return (invl <= invr) ? posleft : pos;    
  }

  template<class OrderRateSet>
  std::pair<typename OrderRateSet::iterator, typename OrderRateSet::iterator>
  find_valid_order_by_rate(double rate, std::shared_ptr<OrderRateSet> & set)
  {
    auto ascending = std::is_same<typename OrderRateSet::key_compare, comparebyrate1>::value;
    auto isSeller = ascending;

    std::pair<typename OrderRateSet::iterator, typename OrderRateSet::iterator> pair;
    pair.first = set->begin();
    pair.second = set->begin();

    static auto key = std::make_shared<Order>();
    key->_rate = rate;
    pair.second = set->upper_bound(key);
    
    return pair;
  }
  
} // namespace exchange
