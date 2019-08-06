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
#include <memory>

#include <unistd.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <chrono>
#include <unordered_map>
#include <assert.h>

using namespace std::chrono_literals;

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/in.h> 
#include <sys/types.h>  
#include <sys/socket.h> 
#include <stdio.h>      
#include <stdlib.h>     
#include <string.h>

#include <mariadb/mysql.h>

#include "../utils/utilities.h"

int g_log_level = INFO;

const char * linesplit = "@#$";

	// `id` INT(11) NOT NULL,
	// `user` VARCHAR(50) NOT NULL,
	// `login_pwd_hash` VARCHAR(50) NOT NULL,
	// `tx_pwd_hash` VARCHAR(50) NOT NULL,
	// `login_pwd_salt` VARCHAR(50) NOT NULL,
	// `tx_pwd_salt` VARCHAR(50) NOT NULL,
	// `balance` DECIMAL(24,8) NOT NULL,
	// `phone` VARCHAR(50) NOT NULL,
	// `mail` VARCHAR(50) NOT NULL,
	// `recharge_addr` VARCHAR(50) NOT NULL,
	// `reward_point` VARCHAR(50) NOT NULL,
	// `timestamp` INT(11) NOT NULL,
	// `device_token` VARCHAR(50) NOT NULL,
	  
int main1()
{
  const char* book_pat = "{\"command\": \"booking\", \"seq\": 2, \"paras\": { \"product_no\": \"%s\", \"user_publickey\":\"%s\", \"amount\":0.001, \"timestamp\": 1558322863 } }";
  const char* cancel_pat = "{\"seq\": 645, \"command\": \"cancel_order\", \"paras\": {\"order_id\": %d, \"product_no\": \"%s\", \"user_publickey\": \"%s\"}}";

  const char* add_user_pat = "{\"command\":\"add_user\", \"seq\":2, \"paras\":{\"user\":\"%s\", \"login_pwd_hash\":\"aaa\", \"tx_pwd_hash\":\"bbb\", \"login_pwd_salt\":\"ccc\", \"tx_pwd_salt\":\"ddd\", \"balance\":3, \"phone\":\"1350123%d\", \"mail\":\"abc%d@qq.com\", \"recharge_addr\":\"eee\", \"reward_point\": 55, \"timestamp\":0, \"device_token\":\"fff\", \"withdraw_addr\":\"hhh\"} }";

  const char* get_user_pat = "{\"seq\": 645, \"command\": \"get_user\", \"paras\": {\"id\": %d}}";
  const char* get_user_pat1 = "{\"seq\": 645, \"command\": \"get_user\", \"paras\": {\"user\": %s}}";
  const char* get_user_pat2 = "{\"seq\": 645, \"command\": \"get_user\", \"paras\": {\"phone\": 1350123%d}}";
  const char* add_order_pat = "{\"seq\": 888, \"command\": \"add_order\", \"paras\": {\"type\":%d, \"from\":%d, \"to\":%d, \"amount\":%f, \"recharge_utxo\":\"utxo001\", \"utxo_confirmed\":1}}";
  const char* add_order_pat1 = "{\"seq\": 888, \"command\": \"add_order\", \"paras\": {\"type\":%d, \"from\":%d, \"to\":%d, \"amount\":%f, \"recharge_utxo\":\"utxo001\", \"utxo_confirmed\":1}}";
  const char* add_order_pat2 = "{\"seq\": 888, \"command\": \"add_order\", \"paras\": {\"type\":%d, \"from\":%d, \"to\":%d, \"amount\":%f, \"withdraw_utxo\":\"wu001\", \"withdraw_addr\":\"wa001\", \"withdraw_fee\":0.01}}";
  const char* get_orders_pat = "{\"seq\": 645, \"command\": \"get_orders\", \"paras\": {\"user_id\":%d, \"type\":%d, \"offset\":%d, \"limit\":10}}";
  
  bool connected = false;
  
  int sock = 0;

  auto do_connect = [&]
    {
      if(connected)
	return;
    
      for(;;)
	{
	  //sock = SocketHelper::connect("127.0.0.1", 60001);
	  sock = SocketHelper::connect("47.106.208.207", 60001);
	  if(sock > 0)
	    break;
	  sleep(3);
	}
    
      connected = true;
    };
  

  do_connect();
  
  std::thread sendthrd([&]{

      int useridx = 0;
      
      for(;;)
	{
	  std::vector<std::string> v;
      
	  char buf[512];
      
	  for(int i=0; i<1; ++i)
	    {
	      static int orderid = 33857;
	      //sprintf(buf, book_pat, "FM0021", "mousWBSN7Rsqi8qpmZp7C6VmRkBGPD5bFF"); v.push_back(buf);
	      //sprintf(buf, book_pat, "FM0069", username); v.push_back(buf);
	      //sprintf(buf, cancel_pat,  orderid++, "FM0065", username); v.push_back(buf);

	      useridx %= 200;
	      char username[512];
	      sprintf(username, "lmf_WbCdvY9c5iUDRSofhU8rFh5kW_%04d", useridx);
	      
	      //sprintf(buf, add_user_pat, username, useridx, useridx); v.push_back(buf);
	      //sprintf(buf, get_user_pat, useridx); v.push_back(buf);
	      //sprintf(buf, get_user_pat1, username); v.push_back(buf);
	      //sprintf(buf, get_user_pat2, useridx); v.push_back(buf);
	      //sprintf(buf, add_order_pat, 0, 0, 1, 0.1); v.push_back(buf);
	      //sprintf(buf, add_order_pat1, 1, 0, 276, 10.0); v.push_back(buf);
	      //sprintf(buf, add_order_pat2, 2, 0, 1, 0.3); v.push_back(buf);
	      //sprintf(buf, get_orders_pat, 0, 1, 0); v.push_back(buf);
	      //sprintf(buf, add_order_pat1, 1, 0, 203, 0.001); v.push_back(buf);
	      sprintf(buf, add_order_pat1, 1, 5, 6, 0.001); v.push_back(buf);
	      
	      useridx++;
	    }

	  for(auto a : v)
	    {
	      //std::string req = a + linesplit;
	      std::string req = a;
	      
	      while(!connected)
		sleep(1);
	      
	      unsigned short len = req.length()+3;
	      
	      auto cnt = send(sock, &len, 2, 0);
	      cnt = send(sock, req.c_str(), req.length()+1, 0);
	  
	      if(cnt != req.length()+1)
		{
		  connected = false;
		  close(sock);
		  LOG(WARNING, "send failed: %d should be %d", cnt, req.length());
		}
	      else
		{
		  static int idx = 0;
		  dot(".");
		  LOG(INFO, "send %d success: %s", idx++, req.c_str());
		}
	    }
	  sleep(3);
	  //usleep(1*1000);
	}
    });

  char buf[8192];
  buf[sizeof(buf)-1] = 0x00;
  int idx = 0;

  unsigned short len;

  auto close_and_connect = [&](int i)
    {
      LOG(WARNING, "close_and_connect called with %d", i);
      connected = false;
      close(sock);
      do_connect();
    };
  
  for(;;) {

    auto cnt = recv(sock, &len, 2, 0);

    if(cnt != 2)
      {
	close_and_connect(1); continue;
      }

    if(len > sizeof(buf))
      {
	close_and_connect(2); continue;
      }

    int left = len-2;
    int recved = 0;
    bool errored = false;
    
    while(left > 0) {
      cnt = recv(sock, &buf[recved], left, 0);
      if(cnt <= 0)
	{
	  close_and_connect(3);
	  errored = true; break; 
	}
      recved += cnt;
      left -= cnt;
    }

    if(errored) continue;
    
    if(buf[len-2-1] != 0x00)
      {
	close_and_connect(4); continue;
      }
    
    static int i = 0;
    dot("*");
    LOG(INFO, "recved msg %d:\n%s", i++, buf);
  }
  
  sendthrd.join();
  return 0;
}

int main()
{
  for(int i=0; i<1; ++i)
    {
      std::thread thrd(main1);
      thrd.detach();
    }
  
  sleep(33333);
}

