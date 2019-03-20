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

#include "../utils/utilities.h"

#include "rocksdb/db.h"

#include "localdb.h"

LocalDB::LocalDB(const char* path)
{
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, path, &_db);
  if (!status.ok())
    std::cerr << "open db failed: " << status.ToString() << std::endl;
}

int LocalDB::load()
{
  {
    std::string value;
    _db->Get(rocksdb::ReadOptions(), std::string("o-checkpoint"), &value);
    _orderCheckpoint = std::atol(value.c_str());
  }

  {
    std::string value;
    _db->Get(rocksdb::ReadOptions(), std::string("o-remove"), &value);
    _orderRemove = std::atol(value.c_str());
  }

  {
    std::string value;
    _db->Get(rocksdb::ReadOptions(), std::string("t-remove"), &value);
    _txRemove = std::atol(value.c_str());
  }
  
  long i = _orderCheckpoint;
  
  for(;;)
    {
      char key[80];
      sprintf(key, "o%08ld", i++);
      std::string order;
      auto s = _db->Get(rocksdb::ReadOptions(), std::string(key), &order);
      if (s.ok())
	{
	  _orderHandler(order);
	  continue;
	}
      break;
    }
  
  return 0;
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
	  char key[80];
	  sprintf(key, "o%08ld", pair.first);
	  s = _db->Put(rocksdb::WriteOptions(), std::string(key), pair.second);
	}

      while(!_qtxs.empty())
	{
	  auto pair = _qtxs.pop();
	  char key[80];
	  sprintf(key, "t%08ld", pair.first);
	  s = _db->Put(rocksdb::WriteOptions(), std::string(key), pair.second);
	}
      
      if(now != last && (now-last) >= 10)
	{
	  last = now;

	  printf("db checkpoint\n");
	  
	  long i = _orderCheckpoint;
	  
	  for(;;)
	    {
	      char key[80];
	      sprintf(key, "o%08ld", i++);
	      std::string order;
	      auto s = _db->Get(rocksdb::ReadOptions(), std::string(key), &order);
	      if (s.ok() && !_isOrderAlive(order))
		continue;
	      
	      i--;
	      
	      if(i > _orderCheckpoint)
		{
		  s = _db->Put(rocksdb::WriteOptions(), std::string("o-checkpoint"), std::to_string(i));
		  _orderCheckpoint = i;
		}
	      break;
	    }

	  i = _orderRemove;

	  for(;;)
	    {
	      char key[80];
	      sprintf(key, "o%08ld", i++);
	      std::string order;
	      auto s = _db->Get(rocksdb::ReadOptions(), std::string(key), &order);
	      if (s.ok() && _isOrderTimeout4remove(order))
		{
		  _db->Delete(rocksdb::WriteOptions(), std::string(key));
		  continue;
		}
	      
	      i--;
	      
	      if(i > _orderRemove)
		{
		  s = _db->Put(rocksdb::WriteOptions(), std::string("o-remove"), std::to_string(i));
		  _orderRemove = i;
		}
	      break;
	    }

	  i = _txRemove;

	  for(;;)
	    {
	      char key[80];
	      sprintf(key, "t%08ld", i++);
	      std::string tx;
	      auto s = _db->Get(rocksdb::ReadOptions(), std::string(key), &tx);
	      if (s.ok() && _isTxTimeout4remove(tx))
		{
		  _db->Delete(rocksdb::WriteOptions(), std::string(key));
		  continue;
		}
	      
	      i--;
	      
	      if(i > _txRemove)
		{
		  s = _db->Put(rocksdb::WriteOptions(), std::string("t-remove"), std::to_string(i));
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
