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

#include "engine.h"

int g_log_level = INFO;

int main()
{
  volatile bool alive = true;
  
  exchange::MatchEngine engine;
  
  utils::Queue<exchange::OrderPtr> qorder;
  utils::Queue<exchange::TransactionPtr> qtx;
  
  engine.set_queues(&qorder, &qtx);

  static int idx = 0;

  // btc -> usdt
  qorder.push(std::make_shared<exchange::Order>(idx++, 0, 1, 0, 0.00, 50, 0, "alice0"));
  qorder.push(std::make_shared<exchange::Order>(idx++, 0, 1, 0, 0.00, 60, 0, "alice1"));
  qorder.push(std::make_shared<exchange::Order>(idx++, 0, 1, 1, 0.61, 70, 0, "alice2"));
  qorder.push(std::make_shared<exchange::Order>(idx++, 0, 1, 1, 0.62, 80, 40, "alice3"));

  // usdt -> btc
  qorder.push(std::make_shared<exchange::Order>(idx++, 1, 0, 0, 0.00, 50, 0, "bob0"));
  qorder.push(std::make_shared<exchange::Order>(idx++, 1, 0, 0, 0.00, 60, 0, "bob1"));
  qorder.push(std::make_shared<exchange::Order>(idx++, 1, 0, 1, 0.61, 70, 0, "bob2"));
  qorder.push(std::make_shared<exchange::Order>(idx++, 1, 0, 1, 0.62, 80, 40, "bob3"));

  std::thread tx_dump([&]{
      for(;;)
	{
	  auto tx = qtx.pop();
	  tx->dump();
	}
    });
  
  engine.run(&alive);
  
  tx_dump.join();
  
  return 0;
}


int main1()
{
  volatile bool alive = true;
  exchange::MatchEngine engine;
  
  utils::Queue<exchange::OrderPtr> qorder;
  utils::Queue<exchange::TransactionPtr> qtx;
  
  engine.set_queues(&qorder, &qtx);

  bool done = false;
  
  std::thread orderGenerator([&]{
      // from, to, type, rate, num, minNum, user, timeStamp

      std::vector<double> v;
      for(int i=0; i<1000; ++i)
	v.push_back(0.001*(i+1));
      std::random_shuffle(v.begin(),v.end());

      std::vector<int> v1;
      for(int i=1; i<500; ++i)
	v1.push_back((i%100)+1);
      std::random_shuffle(v1.begin(),v1.end());

      static int idx = 0;
      
      for(int i=0; i<10; i++)
	{
	  //sleep(1);
	  for(int j=0; j<v.size(); )
	    {
	      auto type = (i%3 == 0) ? 0 : 1;
	      qorder.push(std::make_shared<exchange::Order>(idx++, 0, 1, type, 0.5+v[j], v1[j%v1.size()], v1[j%v1.size()]/2, "alice3"));
	      j++;
	      qorder.push(std::make_shared<exchange::Order>(idx++, 1, 0, type, v[j], v1[j%v1.size()], v1[j%v1.size()]/2, "bob3"));
	      j++;
	    }
	}

      done = true;
      for(;;)
	{
	  int a;
	  std::cin >> a;
	  if(a == 0)
	    for(int i=0; i<1000; i++)
	      {
		qorder.push(std::make_shared<exchange::Order>(idx++, 0, 1, 1, 99.99, 50, 0, "alice3"));
	      }
	  if(a == 1)
	    for(int i=0; i<1000; i++)
	      {
		qorder.push(std::make_shared<exchange::Order>(idx++, 1, 0, 1, 0.0009, 50, 0, "bob3"));
	      }
	  if(a == 2)
	    {
	      for(auto a : engine._buyerOrderBook._marketPriceOrderQueue)
		a->dump();
	    }
	  if(a == 3)
	    {
	      for(auto a : engine._buyerOrderBook._limitPriceOrderSetByRate)
		a->dump();
	    }
	  if(a == 4)
	    {
	      for(auto a : engine._buyerOrderBook._retry4minNumQueue)
		a->dump();
	    }
	  if(a == 5)
	    {
	      for(auto a : engine._sellerOrderBook._marketPriceOrderQueue)
		a->dump();
	    }
	  if(a == 6)
	    {
	      for(auto a : engine._sellerOrderBook._limitPriceOrderSetByRate)
		a->dump();
	    }
	  if(a == 7)
	    {
	      for(auto a : engine._sellerOrderBook._retry4minNumQueue)
		a->dump();
	    }
	}

    });

  printf("-----------------000\n");
  while(!done) sleep(1);
  printf("-----------------001\n");
  
  std::thread matchProc(&exchange::MatchEngine::run, &engine, &alive);
  
  std::thread txDumpProc([&]{
      for(;;)
	{
	  auto tx = qtx.pop();
	  static int i = 0;
	  if(g_log_level >= INFO)
	    {
	      printf("%d ", i++);
	      tx->dump();
	    }
	}
    });
  
  orderGenerator.join();
  matchProc.join();
  txDumpProc.join();
  
  return 0;
}
