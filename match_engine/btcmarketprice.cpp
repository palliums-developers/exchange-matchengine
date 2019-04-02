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

#include "btcmarketprice.h"

bool BtcPriceUpdater::load()
{
  return do_get_btc_price() == 0;
}

int BtcPriceUpdater::do_get_btc_price()
{
  system("curl -s https://blockchain.info/ticker > a.txt");
  
  auto size = get_file_size("a.txt");
  if(size <= 0)
    {
      LOG(WARNING, "get btc market price from https://blockchain.info/ticker failed 1");
      return 1;
    }

  auto str = get_file_content("a.txt");
  if(str.empty())
    {
      LOG(WARNING, "get btc market price from https://blockchain.info/ticker failed 2");
      return 2;
    }
  
  auto v = json_get_object(str);
  
  if(v.count("USD") > 0 && v.count("CNY") > 0)
    {
      str = v["USD"];
      auto v11 = json_get_object(str);
      str = v["CNY"];
      auto v12 = json_get_object(str);
      if(v11.count("15m") > 0 && v12.count("15m") > 0)
	{
	  std::unique_lock<std::mutex> lk(_mtx);
	  _usd = std::atof(v11["15m"].c_str());
	  _cny = std::atof(v12["15m"].c_str());
	  return 0;
	}
    }
  
  LOG(WARNING, "get btc market price from https://blockchain.info/ticker failed 3");
  return 3;
}

void BtcPriceUpdater::run(volatile bool * alive)
{
  while(*alive)
    {
      do_get_btc_price();
      dump();
      sleep(60);
    }
}
