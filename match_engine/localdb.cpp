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

#include <unistd.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#include <rocksdb/db.h>

#include "../utils/utilities.h"

#include "localdb.h"

LocalDB::LocalDB(const char* path)
{
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, path, &_db);
  if (!status.ok())
    std::cerr << "open db failed: " << status.ToString() << std::endl;
  else
    {
      _orderCheckpoint = get_long("o-checkpoint");
      _txCheckpoint = get_long("t-checkpoint");
      _orderRemove = get_long("o-remove");
      _txRemove = get_long("t-remove");
      _unspentOrderCheckpoint = get_long("u-checkpoint");
    }
}

int LocalDB::load()
{
  long i = std::min(_orderCheckpoint, _unspentOrderCheckpoint);

  LOG(INFO, "localdb start load from idx %ld", i);
  
  for(;;)
    {
      auto order = get_order(i++);
      if(order.empty())
	break;

      LOG(INFO, "localdb load a order %s", order.c_str());
      
      if(_isOrderAlive(order))
	{
	  _orderHandler(order);
	}
    }

  // i = 0;
  
  // for(;;)
  //   {
  //     auto order = get_unspent_order(i++);
  //     if(order.empty())
  // 	break;
  //     _orderHandler(order);
  //   }
  
  return 0;
}

long LocalDB::get_long(const std::string & key)
{
  std::string value;
  _db->Get(rocksdb::ReadOptions(), key, &value);
  return std::atol(value.c_str());
}

std::string LocalDB::get_unspent_order(long uidx)
{
  auto idx = get_unspent_order_idx(uidx);
  
  if(idx < 0)
    return "";

  return get_order(idx);
}

long LocalDB::get_unspent_order_idx(long idx)
{
  char key[80];
  sprintf(key, "u%08ld", idx);
  std::string orderidx;
  auto s = _db->Get(rocksdb::ReadOptions(), std::string(key), &orderidx);
  if(s.ok())
    return std::atol(orderidx.c_str());
  return -1;
}

std::string LocalDB::order_key(long idx)
{
  char key[80];
  sprintf(key, "o%08ld", idx);
  return std::string(key);
}

std::string LocalDB::transaction_key(long idx)
{
  char key[80];
  sprintf(key, "t%08ld", idx);
  return std::string(key);
}

std::string LocalDB::get_order(long idx)
{
  char key[80];
  sprintf(key, "o%08ld", idx);
  std::string order;
  auto s = _db->Get(rocksdb::ReadOptions(), std::string(key), &order);
  return order;
}

std::string LocalDB::get_transaction(long idx)
{
  char key[80];
  sprintf(key, "t%08ld", idx);
  std::string tx;
  auto s = _db->Get(rocksdb::ReadOptions(), std::string(key), &tx);
  return tx;
}

void LocalDB::set_order(long idx, std::string order)
{
  char key[80];
  sprintf(key, "o%08ld", idx);
  _db->Put(rocksdb::WriteOptions(), std::string(key), order);
}

void LocalDB::set_transaction(long idx, std::string tx)
{
  char key[80];
  sprintf(key, "t%08ld", idx);
  _db->Put(rocksdb::WriteOptions(), std::string(key), tx);
}

void LocalDB::run(volatile bool* alive)
{
  rocksdb::Status s;
  std::string value;
  
  auto now = uint32_t(time(NULL));
  auto last = now;
  
  while(*alive)
    {
      sleep(1);
      
      now = uint32_t(time(NULL));
      
      while(!_qorders.empty())
	{
	  auto pair = _qorders.pop();
	  set_order(pair.first, pair.second);
	}

      while(!_qtxs.empty())
	{
	  auto pair = _qtxs.pop();
	  set_transaction(pair.first, pair.second);
	}
      
      if(now != last && (now-last) >= 30)
	{
	  last = now;

	  printf("db checkpoint\n");

	  long i = _orderCheckpoint;
	  
	  for(;;)
	    {
	      auto order = get_order(i++);

	      if(!order.empty())
		continue;
	      
	      i--;
	      
	      if(i > _orderCheckpoint)
		{
		  _db->Put(rocksdb::WriteOptions(), std::string("o-checkpoint"), std::to_string(i));
		  _orderCheckpoint = i;
		}
	      break;
	    }

	  i = _unspentOrderCheckpoint;

	  for(;;)
	    {
	      auto order = get_order(i++);

	      if(!order.empty() && !_isOrderAlive(order))
		continue;
	      
	      i--;
	      
	      if(i > _unspentOrderCheckpoint)
		{
		  _db->Put(rocksdb::WriteOptions(), std::string("u-checkpoint"), std::to_string(i));
		  _unspentOrderCheckpoint = i;
		}
	      break;
	    }
	  
	  
	  i = _txCheckpoint;
	  
	  for(;;)
	    {
	      auto tx = get_transaction(i++);

	      if(!tx.empty())
		continue;
		
	      i--;
	      
	      if(i > _txCheckpoint)
		{
		  _db->Put(rocksdb::WriteOptions(), std::string("t-checkpoint"), std::to_string(i));
		  _txCheckpoint = i;
		}
	      break;
	    }
	  
	  i = _orderRemove;

	  for(;;)
	    {
	      auto order = get_order(i++);
	      
	      if(!order.empty() && _isOrderTimeout4remove(order))
		{
		  _db->Delete(rocksdb::WriteOptions(), order_key(i-1));
		  continue;
		}

	      i--;
	      
	      if(i > _orderRemove)
		{
		  _db->Put(rocksdb::WriteOptions(), std::string("o-remove"), std::to_string(i));
		  _orderRemove = i;
		}
	      break;
	    }

	  i = _txRemove;

	  for(;;)
	    {
	      auto tx = get_transaction(i++);

	      if (!tx.empty() && _isTxTimeout4remove(tx))
		{
		  _db->Delete(rocksdb::WriteOptions(), transaction_key(i-1));
		  continue;
		}
	      
	      i--;
	      
	      if(i > _txRemove)
		{
		  _db->Put(rocksdb::WriteOptions(), std::string("t-remove"), std::to_string(i));
		  _txRemove = i;
		}
	      break;
	    }	  
	}
    }
}

int main_test()
{
  {
    volatile bool alive = true;
    LocalDB db("testdb");

    db.set_handlers(
		    [](std::string a){ std::cout << "order handler: " << a << "\n"; return 0; },
		    [](std::string a){ std::cout << "order alive  : " << a << "\n"; return true; },
		    [](std::string a){ std::cout << "order remove : " << a << "\n"; return false; },
		    [](std::string a){ std::cout << "tx remove    : " << a << "\n"; return false; }
		    );
    
    auto ret = db.load();
    std::cout << "load: " << ret << "\n";
    
    std::thread thrd([&]{ db.run(&alive); });

    db.append_order(0, "a000");
    db.append_order(1, "a001");
    db.append_order(2, "a002");
    db.append_order(3, "a003");
    db.append_order(4, "a004");
    
    db.append_transaction(0, "b000");
    db.append_transaction(1, "b001");
    db.append_transaction(2, "b002");
    db.append_transaction(3, "b003");
    db.append_transaction(4, "b004");

    auto pdb = [&] {
      rocksdb::Iterator* it = db.raw_db()->NewIterator(rocksdb::ReadOptions());
      for (it->SeekToFirst(); it->Valid(); it->Next()) 
	std::cout << it->key().ToString() << ": " << it->value().ToString() << std::endl;
      assert(it->status().ok()); 
      delete it;
    };

    pdb();
    
    for(;;)
      {
	pdb();
	std::cout << db.string() << "\n";
	sleep(5);
      }
    
    thrd.join();
    return 0;
  }
}
