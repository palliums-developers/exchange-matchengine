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

#include "financial_management.h"

int g_log_level = INFO;

void create_table();
void show_error(MYSQL *mysql);
void do_client();

int main()
{
  //do_client();
  //create_table();

  Config::instance()->parse("./config");
  
  FinancialManagement a;
  
  a.start();
  
  {
    // a.push_request("{\"command\": \"get_product_info\", \"seq\": 0, \"paras\": { \"product_no\": \"FM0001\"} }");
    // //a.push_request("{\"command\": \"get_product_list\", \"seq\": 1, \"paras\": { \"start_time\": 1558583587, \"end_time\": 1558588321} }");
    // a.push_request("{\"command\": \"get_product_list\", \"seq\": 1, \"paras\": { \"start_time\": 0, \"end_time\": 1559109332} }");
    // a.push_request("{\"command\": \"booking\", \"seq\": 2, \"paras\": { \"product_no\": \"FM0001\", \"user_publickey\":\"up001\", \"amount\":0.1, \"timestamp\": 1558322863 } }");
    // a.push_request("{\"command\": \"cancel_order\", \"seq\": 3, \"paras\": { \"product_no\": \"FM0001\", \"user_publickey\": \"up001\", \"order_id\": 10} }");
    // a.push_request("{\"command\": \"update_order_txid\", \"seq\": 4, \"paras\": { \"product_no\": \"FM0001\", \"user_publickey\": \"up001\", \"order_id\": 15, \"txid\":\"txid011\", \"investment_return_addr\":\"ira011\"} }");
    // a.push_request("{\"command\": \"order_txid_confirm\", \"seq\": 5, \"paras\": { \"txid\": \"txid011\", \"success\": \"true\", \"order_id\": 15} }");
    // a.push_request("{\"command\": \"order_txid_confirm\", \"seq\": 5, \"paras\": { \"txid\": \"txid011\", \"success\": \"false\", \"order_id\": 14} }");
    // a.push_request("{\"command\": \"payment_refund_success\", \"seq\": 7, \"paras\": { \"txid\": \"txid011\", \"order_id\": 15} }");
    // a.push_request("{\"command\": \"collect\", \"seq\": 3, \"paras\": { \"product_no\": \"FM0001\", \"user_publickey\": \"up001\"} }");
    // a.push_request("{\"command\": \"collect\", \"seq\": 3, \"paras\": { \"product_no\": \"FM0002\", \"user_publickey\": \"up001\"} }");
    // a.push_request("{\"command\": \"collect\", \"seq\": 3, \"paras\": { \"product_no\": \"FM0003\", \"user_publickey\": \"up001\"} }");
    // a.push_request("{\"command\": \"cancel_collection\", \"seq\": 3, \"paras\": { \"product_no\": \"FM0002\", \"user_publickey\": \"up001\"} }");
    // a.push_request("{\"command\": \"get_user_collections\", \"seq\": 3, \"paras\": { \"user_publickey\": \"up001\"} }");
    // a.push_request("{\"command\": \"get_order\", \"seq\": 7, \"paras\": { \"order_id\": 26} }");
    // a.push_request("{\"command\": \"get_user\", \"seq\": 7, \"paras\": { \"user_publickey\": \"up001\"} }");

    // a.push_request("{\"command\": \"get_orders\", \"seq\": 7, \"paras\": { \"product_no\": \"FM0001\", \"user_publickey\": \"all\", \"status\": 0, \"offset\":0, \"limit\":10 } }");
    
  }

  {
    for(int i=0; i<500; ++i)
      {
	//query("{\"command\": \"booking\", \"seq\": 1, \"paras\": { \"product_no\": \"FM0001\", \"user_publickey\":\"up001\", \"amount\":0.1, \"timestamp\": 1558322863 } }");
      }
  }
  
  //std::cout << "rsp: " << rsp << "\n";

  sleep(300000);
  return 0;
}

//INSERT INTO `test`.`aaa` (`a`, `b`) VALUES ('aaa', '3');

MYSQL *mysql;

int get_rows_count(const char* table)
{
  int count = 0;
      
  char query[256];
  snprintf(query, sizeof(query), "SELECT COUNT(*) FROM %s", table);
  if (mysql_real_query(mysql, query, strlen(query)))
    show_error(mysql);
      
  auto result= mysql_store_result(mysql);
  if(result == NULL)
    return 0;

  auto rowscnt = mysql_num_rows(result);
  auto fldscnt = mysql_num_fields(result);

  if(rowscnt == 1 && fldscnt == 1)
    {
      MYSQL_ROW row = mysql_fetch_row(result);
      count = std::atoi(row[0]);
    }
      
  mysql_free_result(result);

  return count;
};

void show_error(MYSQL *mysql)
{
  printf("Error(%d) [%s] \"%s\"", mysql_errno(mysql), mysql_sqlstate(mysql), mysql_error(mysql));
  mysql_close(mysql);
  exit(-1);
}

void do_client()
{
  struct sockaddr_in client_addr;
  bzero(&client_addr,sizeof(client_addr)); 
  client_addr.sin_family = AF_INET;    
  client_addr.sin_addr.s_addr = htons(INADDR_ANY);
  client_addr.sin_port = htons(0);   
  int client_socket = socket(AF_INET,SOCK_STREAM,0);
  if(client_socket < 0)
    {
      printf("Create Socket Failed!\n");
      exit(1);
    }
  if(bind(client_socket,(struct sockaddr*)&client_addr,sizeof(client_addr)))
    {
      printf("Client Bind Port Failed!\n"); 
      exit(1);
    }
 
  struct sockaddr_in server_addr;
  bzero(&server_addr,sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  if(inet_aton("127.0.0.1", &server_addr.sin_addr) == 0) 
    {
      printf("Server IP Address Error!\n");
      exit(1);
    }
  
  server_addr.sin_port = htons(60001);
  socklen_t server_addr_length = sizeof(server_addr);
  if(connect(client_socket,(struct sockaddr*)&server_addr, server_addr_length) < 0)
    {
      printf("Can Not Connect To %s!\n", "127.0.0.1");
      exit(1);
    }

  for(int i=0; i<100; ++i)
    {
      char buf[256];
      sprintf(buf, "%04d alsdfjalsdflasjdlfjsldfloiwefjlfas\n", i);
      printf("------001\n");
      send(client_socket, buf, strlen(buf), 0);
      auto cnt = recv(client_socket, buf, sizeof(buf), 0);
      if(cnt <= 0)
	{
	  printf("recv failed\n");
	  break;
	}
      buf[cnt] = 0x00;
      printf("recved: %s\n", buf);
      printf("------002\n");
      if(i % 8 == 0)
	sleep(3);
    }
  sleep(99999);
}


void create_table()
{
  const char *query;
  MYSQL_RES *result;

  mysql= mysql_init(NULL);
  if (!mysql_real_connect(mysql, "47.106.208.207", "root", "1234qwer", "test", 0, NULL, 0))
    show_error(mysql);

    if(0)
    {
      query= "DROP TABLE financial_management_projects"; 
      if (mysql_real_query(mysql, query, strlen(query)))
	show_error(mysql);
    }
  
  if(0)
    {
      //query= "CREATE TABLE financial_management_projects (id int not null, no char(16), name text, annualized_return_rate double, investment_period int, min_invest_amount double, max_invest_amount double, total_crowdfunding_amount double, booking_starting_time int, crowdfunding_ending_time int, status int, received_crowdfunding double, interest_type int, borrower_info int, crowdfunding_address char(72), product_description text, PRIMARY KEY(id))";

      //query= "CREATE TABLE financial_management_orders (id int not null, project_id int, user_id int, booking_timestamp int, booking_amount double, payment_txid char(72), payment_timestamp int, investment_return_addr char(72), accumulated_gain double, status int, PRIMARY KEY(id))";

      query= "CREATE TABLE financial_management_users (id int not null, publickey char(72), name char(32), collections text, PRIMARY KEY(id))";
      
      if (mysql_real_query(mysql, query, strlen(query)))
	show_error(mysql);
    }

  if(0)
    {
      auto now = (int)uint32_t(time(NULL));
      char query[256];
      int idx = get_rows_count("financial_management_projects");
      int cnt = 100;
      for(int i=idx; i<(idx+cnt); ++i)
	{
	  snprintf(query, sizeof(query), "INSERT INTO financial_management_projects VALUES (%d, 'FM%04d', 'money', %f, 30, 0.01, 0.1, 1, %d, %d, 0, 0, 0, 0, 'abcd0123', 'makemoney')",
		   i, i, 0.083+0.001*(i-idx), now+3, now+33);
	  if (mysql_real_query(mysql, query, strlen(query)))
	    show_error(mysql);
	}
    }
  
  if(0)
    {
      auto now = (int)uint32_t(time(NULL));
      char query[256];
      int idx = get_rows_count("financial_management_orders");
      int cnt = 10;
      for(int i=idx; i<(idx+cnt); ++i)
	{
	  snprintf(query, sizeof(query), "INSERT INTO financial_management_orders VALUES (%d, 0, 0, %d, 0.2, 'ab12cd34', %d, 'ab12cd78', 0, 0)",
		   i, now, now);
	  if (mysql_real_query(mysql, query, strlen(query)))
	    show_error(mysql);
	}
    }

  if(1)
    {
      auto now = (int)uint32_t(time(NULL));
      
      int idx = get_rows_count("bbb");
      int dst = idx +5000;
      while(idx < dst)
	{
	  std::string line;
	  for(int i=0; i<100; ++i)
	    {
	      char buf[256];
	      snprintf(buf, sizeof(buf), "(%d, 0, 0, %d, 0.2, 'ab12cd34', %d, 'ab12cd78', 0, 0)", idx++, now, now);
	      if(!line.empty())
		line += ",";
	      line += buf;
	    }
	  line = "INSERT INTO bbb VALUES" + line;
	  if (mysql_real_query(mysql, line.c_str(), line.length()))
	    show_error(mysql);
	}
      
      auto now2 = (int)uint32_t(time(NULL));
      printf("cost2 %d seconds\n", now2-now);
    }
  
  if(0)
    {
      auto now = (int)uint32_t(time(NULL));
      
      char query[256];
      int idx = get_rows_count("bbb");
      int cnt = 500;
      for(int i=idx; i<(idx+cnt); ++i)
	{
	  snprintf(query, sizeof(query), "INSERT INTO bbb VALUES (%d, 0, 0, %d, 0.2, 'ab12cd34', %d, 'ab12cd78', 0, 0)",
		   i, now, now);
	  if (mysql_real_query(mysql, query, strlen(query)))
	    show_error(mysql);
	}
      
      auto now2 = (int)uint32_t(time(NULL));
      printf("cost %d seconds\n", now2-now);
    }
  
  mysql_close(mysql);

  sleep(999999);
}

// mariadb -u root -p -h 47.106.208.207

// id
// project_id
// user_id
// booking_timestamp
// booking_amount
// payment_txid
// payment_timestamp
// investment_return_addr
// accumulated_gain
// status

// char(16)
// text
// float
// int
// double
// double
// double
// int
// int
// int
// double
// int
// int
// char(72)
// text

// no
// name
// annualized_return_rate
// investment_period
// min_invest_amount
// max_invest_amount
// total_crowdfunding_amount
// booking_starting_time
// crowdfunding_ending_time
// status
// received_crowdfunding
// interest_type
// borrower_info
// crowdfunding_address
// product_description
