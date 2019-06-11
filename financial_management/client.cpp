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

int main1()
{
  const char* book_pat = "{\"command\": \"booking\", \"seq\": 2, \"paras\": { \"product_no\": \"%s\", \"user_publickey\":\"%s\", \"amount\":0.001, \"timestamp\": 1558322863 } }";
  const char* get_orders_pat = "{\"seq\": 1, \"command\": \"get_orders\", \"paras\": {\"user_publickey\": \"mousWBSN7Rsqi8qpmZp7C6VmRkBGPD5bFF\", \"product_no\": \"all\", \"status\": 1, \"offset\": 0, \"limit\": 10}}";
  const char* cancel_pat = "{\"seq\": 645, \"command\": \"cancel_order\", \"paras\": {\"order_id\": %d, \"product_no\": \"%s\", \"user_publickey\": \"%s\"}}";
  
  bool connected = false;
  
  int sock = 0;

  auto do_connect = [&]{
    
    if(connected)
      return;
    
    for(;;)
      {
	sock = SocketHelper::connect("127.0.0.1", 60001);
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
	      char username[512];
	      sprintf(username, "lmf_WbCdvY9c5iUDRSofhU8rFh5kW_%04d", (useridx++%200));
	      sprintf(buf, book_pat, "FM0069", username); v.push_back(buf);
	      //sprintf(buf, cancel_pat,  orderid++, "FM0065", username); v.push_back(buf);
	    }

	  for(auto a : v)
	    {
	      std::string req = a + linesplit;
	  
	      while(!connected)
		sleep(1);

	      //auto str = "{\"seq\": 645, \"command\": \"cancel_order\", \"paras\": {\"order_id\": 33838, \"product_no\": \"FM0065\", \"user_publickey\": \"mrmrQVRepZxBLDXKamDzqXkE3VNdR1eZkX\"}}@#$";
	      //auto str = "{\"seq\": 1, \"command\": \"get_orders\", \"paras\": {\"user_publickey\": \"mousWBSN7Rsqi8qpmZp7C6VmRkBGPD5bFF\", \"product_no\": \"all\", \"status\": 1, \"offset\": 0, \"limit\": 10}}@#$";
	      //auto cnt = send(sock, str, strlen(str), 0);
	      //sleep(1111);
	      
	      auto cnt = send(sock, req.c_str(), req.length(), 0);
	  
	      if(cnt != req.length())
		{
		  connected = false;
		  close(sock);
		  LOG(WARNING, "send failed: %d should be %d", cnt, req.length());
		}
	      else
		{
		  static int idx = 0;
		  LOG(INFO, "send %d success", idx++);
		}
	      //sleep(5);
	    }
	  sleep(3);
	  //usleep(2*1000);
	}
    });

  char buf[8192];
  buf[sizeof(buf)-1] = 0x00;
  int idx = 0;
  
  for(;;) {

    std::vector<std::string> v;
    
    int size = sizeof(buf)-idx-1;
    auto cnt = recv(sock, &buf[idx],  size, 0);
    
    if(cnt > 0)
      {
	idx += cnt;
	buf[idx] = 0x00;

	char* p = &buf[0];

	while(p < &buf[idx])
	  {
	    auto e = strstr(p, linesplit);
	    if(e == NULL)
	      break;
	    *e = 0x00;
	    v.push_back(p);
	    p = e + strlen(linesplit);
	  }
	
	int left = int(&buf[idx] - p);
	if(left > 0 && p != &buf[0])
	    memmove(&buf[0], p, left);
	
	idx = left;

	for(auto a : v)
	  {
	    static int i = 0;
	    LOG(INFO, "recved msg %d:\n%s", i++, a.c_str());
	  }
      }
    
    if(cnt <= 0)
      {
	connected = false;
	close(sock);
	LOG(WARNING, "recv failed: %d", cnt);
	
	do_connect();
      }
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

