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
#include <chrono>

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

void create_table();
void show_error(MYSQL *mysql);

int main()
{
  create_table();
  sleep(300000);
  return 0;
}

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

void print_result(MYSQL *mysql)
{
  auto result= mysql_store_result(mysql);
  if(result == NULL)
    {
      printf("empty set\n");
      return;
    }

  auto rowscnt = mysql_num_rows(result);
  auto fldscnt = mysql_num_fields(result);

  printf("rowscnt: %d, fldscnt: %d\n", (int)rowscnt, (int)fldscnt);
  
  std::string line;
  char buf[256];
  
  for(int i=0; i<rowscnt; ++i)
    {
      snprintf(buf, sizeof(buf), "line %02d: ", i);
      line = buf;

      MYSQL_ROW row = mysql_fetch_row(result);
      
      for(int j=0; j<fldscnt; ++j)
	{
	  if(j > 0)
	    line += ",";
	  line += row[j];
	}
      std::cout << line << "\n";
    }
  
  mysql_free_result(result);
}

void show_error(MYSQL *mysql)
{
  printf("Error(%d) [%s] \"%s\"", mysql_errno(mysql), mysql_sqlstate(mysql), mysql_error(mysql));
  mysql_close(mysql);
  exit(-1);
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

  // add new project
  if(1)
    {
      auto now = (int)uint32_t(time(NULL));
      char query[256];
      int idx = get_rows_count("financial_management_projects");
      int cnt = 1;
      for(int i=idx; i<(idx+cnt); ++i)
	{
	  snprintf(query, sizeof(query), "INSERT INTO financial_management_projects VALUES (%d, 'FM%04d', 'money%04d', %f, 30, 0.001, 0.1, 3333, %d, %d, 0, 0, 0, 0, 'mousWBSN7Rsqi8qpmZp7C6VmRkBGPD5bFF', 'makemoney%04d', '', '', '')", i, i, i, 0.083+0.001*(i-idx), now+3, now+3+3600*72, i);
	  if (mysql_real_query(mysql, query, strlen(query)))
	    show_error(mysql);
	}
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

  if(0)
    {
      auto now = (int)uint32_t(time(NULL));
      
      int idx = get_rows_count("bbb");
      int dst = idx + 10;
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
      {
	std::string line = "INSERT INTO bbb VALUES(211, 0, 0, 0, 0.2, 'ab12cd34', 9999, 'ab12cd78', 0, 0),(213, 0, 0, 0, 0.2, 'ab12cd34', 9999, 'ab12cd78', 0, 0)";
	if (mysql_real_query(mysql, line.c_str(), line.length()))
	  show_error(mysql);
      }
    }

  if(0)
    {
      {
	std::string line = "UPDATE bbb SET status = CASE id WHEN 211 THEN 4 WHEN 212 THEN 7 END WHERE id IN (300, 211, 212)";
	if(mysql_real_query(mysql, line.c_str(), line.length()))
	  show_error(mysql);
      }
    }
  
//   UPDATE categories SET
//     display_order = CASE id
//         WHEN 1 THEN 3
//         WHEN 2 THEN 4
//         WHEN 3 THEN 5
//     END
// WHERE id IN (1,2,3)
    
  if(0)
    {
      auto now = (int)uint32_t(time(NULL));
      
      char query[4096] = {0,};
      int idx = get_rows_count("bbb");
      int cnt = 20;
      printf("count: %d\n", idx);
      for(int i=0; i<cnt; )
	{
	  std::string str;
	  for(int j=0; j<10; ++j)
	    {
	      if(j > 0)
		str += " or ";
	      str += "id=" + std::to_string(j);

	      if(j == 5)
		str += " or id=8889"; 
	    }
	  i += 10;
	  
	  snprintf(query, sizeof(query), "SELECT * FROM bbb WHERE %s", str.c_str());
	  printf("query: %s\n", query);
	  
	  if (mysql_real_query(mysql, query, strlen(query)))
	    show_error(mysql);
	  else
	    print_result(mysql);
	}
      
      auto now2 = (int)uint32_t(time(NULL));
      printf("cost %d seconds, tps: %d\n", now2-now, cnt/(now2-now+1));
    }
  
  mysql_close(mysql);

  sleep(999999);
}

