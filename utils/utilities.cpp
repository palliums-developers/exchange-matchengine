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
#include <cctype>
#include <chrono>
#include <assert.h>

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <iconv.h>
#include <ctype.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/param.h> 
#include <sys/ioctl.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <netinet/if_ether.h>
#include <arpa/inet.h>
#include <net/if.h> 

using namespace std::chrono_literals;

#include "utilities.h"

std::string timestamp_2_string(uint32_t stamp)
{
  struct tm tformat = {0};
  time_t tt(stamp);
  localtime_r(&tt, &tformat); 
  char buf[80]; memset(buf, 0, sizeof(buf));
  strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tformat); 
  std::string ret = buf;
  return ret;
}

//int g_log_level = INFO;

std::string green_text(const char* text) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s%s%s", "\e[0;32m", text, "\e[0m");
  return std::string(buf);
}

std::string red_text(const char* text) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s%s%s", "\e[0;31m", text, "\e[0m");
  return std::string(buf);
}

std::string blue_text(const char* text) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s%s%s", "\e[0;34m", text, "\e[0m");
  return std::string(buf);
}

void log(int level, const char* file, int line, const char* func, const char *format, ...)
{
  if(level < g_log_level)
    return;
  
  static std::vector<std::string> lvlstrs{"DEBUG", "INFO", "WARNING", "ERROR", "FATAL"};
  auto now = uint32_t(time(NULL));

  std::string lvlstr;
  if(level == INFO)
    lvlstr = green_text(lvlstrs[level].c_str());
  if(level >= WARNING)
    lvlstr = red_text(lvlstrs[level].c_str());
  std::string task = blue_text("match-engine");
  
  char content[1024];
  va_list args;
  va_start(args, format);
  vsnprintf(content, sizeof(content), format, args);
  va_end(args);

  printf("{ \"time\": \"%s\", \"lvl\": \"%s\",  \"file\": \"%s\", \"line\": %d, \"func\": \"%s\", \"task\": \"%s\", \"content\": \"%s\" }\n"
	 , timestamp_2_string(now).c_str(), lvlstr.c_str(), file, line, func, task.c_str(), content);
  
  printf("\n");
}

void log2(int level, const char* file, int line, const char* func, const char *format, ...)
{
  if(level < g_log_level)
    return;
  
  static std::vector<std::string> lvlstrs{"DEBUG", "INFO", "WARNING", "ERROR", "FATAL"};
  auto now = uint32_t(time(NULL));

  std::string lvlstr;
  if(level == INFO)
    lvlstr = green_text(lvlstrs[level].c_str());
  if(level >= WARNING)
    lvlstr = red_text(lvlstrs[level].c_str());
  std::string task = blue_text("match-engine");
  
  char str[512];
  sprintf(str, "\n%s %s %s:%d %s %s\n", timestamp_2_string(now).c_str(), lvlstr.c_str(), file, line, func, task.c_str());
  printf("%s", str);
  
  va_list args;
  va_start(args, format);
  vprintf(format, args);
  va_end(args);
  
  printf("\n");
}

std::string string_join(const std::vector<std::string>& v, const std::string & s)
{
  std::string str;
  
  for(int i=0; i<v.size(); ++i)
    {
      if(i > 0)
	str += s;
      str += v[i];
    }

  return str;
}

std::vector<std::string> string_split(const std::string & str, const std::string & s)
{
  std::vector<std::string> v;
  size_t start = 0;
  
  while(start < str.length())
    {
      auto pos = str.find(s, start);
      if(pos == std::string::npos)
	break;
      v.push_back(str.substr(start, pos-start));
      start = pos + s.length();
    }
  
  v.push_back(str.substr(start, str.length()-start));

  for(auto & a: v)
    a = trim_space(a);
  
  return v;
}

std::string map2json(const std::map<std::string, std::string> & v)
{
  std::string str("{");
  int i = 0;
  for(auto & a : v)
    {
      if(i++ > 0)
	str += ",";
      str += a.first + ":" + a.second;
    }
  str += "}";
  return str;
}

std::map<std::string, std::string> json2map(const std::string & json)
{
  std::map<std::string, std::string> m;
  
  if(json.length() < 2)
    return m;
  
  auto str = json.substr(1, json.length()-2);
  
  auto v = string_split(str, ",");
  
  for(auto & a : v)
    {
      auto p = string_split(a, ":");
      if(p.size() != 2)
	continue;
      m[p[0]] = p[1];
    }

  return m;
}

std::string trim_space(std::string str)
{
  int i, j;
  const char* p = str.c_str();
  int n = str.length();
  for(i=0; i<n; ++i)
    if(0 == std::isspace(p[i]))
      break;
  for(j=n-1; j>=0; --j)
    if(0 == std::isspace(p[j]))
      break;
  if(j>=i)
    return str.substr(i, j-i+1);
  return "";
}

std::string trim_quote(std::string str)
{
  str = trim_space(str);
  
  if(str.length() >= 2)
    {
      if(str[0] == '\'' && str[str.length()-1] == '\'')
	return str.substr(1, str.length()-2);
      if(str[0] == '"' && str[str.length()-1] == '"')
	return str.substr(1, str.length()-2);
    }
  return str;
}

std::vector<std::string> json_split(std::string str)
{
  int i, s;
  i = s = 0;

  std::vector<std::string> v;
  
  bool inquote = false;
  char quote;
  int brackets = 0;
  
  str = trim_space(str);
  
  const char* p = str.c_str();
  auto n = str.length();

  for(i=0; i<n; ++i)
    {
      if(inquote)
	{
	  if(p[i] == quote && p[i-1] != '\\')
	    inquote = false;
	  continue;
	}

      if(p[i] == '\'' || p[i] == '"')
	{
	  inquote = true;
	  quote = p[i];
	  continue;
	}
      
      if(p[i] == '[' || p[i] == '{')
	{
	  brackets++;
	  continue;
	}
      
      if(p[i] == ']' || p[i] == '}')
	{
	  brackets--;
	  continue;
	}
      
      if(brackets > 0)
	continue;
      if(p[i] != ',')
	continue;

      if(i > s)
	v.push_back(str.substr(s, i-s));

      s = i+1;
    }

  if(i > s)
    v.push_back(str.substr(s, i-s));
  
  if(inquote || brackets > 0)
    {
      LOG(WARNING, "invalid json: %s", str.c_str());
      return std::vector<std::string>();
    }

  return v;
}

std::vector<std::string> json_split_kv(std::string str)
{
  std::vector<std::string> v;
  auto pos = str.find(':');
  if(pos == std::string::npos) {
    LOG(WARNING, "invalid json key value pair: %s", str.c_str());
    return v;
  }
  v.push_back(trim_quote(str.substr(0, pos)));
  v.push_back(trim_quote(str.substr(pos+1, str.length()-(pos+1))));
  return v;
}

std::map<std::string, std::string> json_get_object(std::string str)
{
  std::map<std::string, std::string> m;

  str = trim_space(str);

  if(str.length() < 2 || str[0] != '{' || str[str.length()-1] != '}')
    {
      LOG(WARNING, "invalid json object: %s", str.c_str());
      return m;
    }

  str = str.substr(1, str.length()-2);
  
  auto v = json_split(str);

  for(auto item : v)
    {
      auto kv = json_split_kv(item);
      if(kv.size() == 2)
	m[kv[0]] = kv[1];
    }
  
  return m;
}

std::vector<std::string> json_get_array(std::string str)
{
  str = trim_space(str);
  
  if(str.length() < 2 || str[0] != '[' || str[str.length()-1] != ']')
    {
      LOG(WARNING, "invalid json array: %s", str.c_str());
      return std::vector<std::string>();
    }
  
  str = str.substr(1, str.length()-2);
  
  return json_split(str);
}

bool Config::parse(const char* path)
{
  FILE* fh = fopen(path, "r");
  if(fh == NULL)
    {
      LOG(ERROR, "read config file failed: %s", path);
      return false;
    }

  for(;;)
    {
      char buf[512];
      auto p = fgets(buf, sizeof(buf), fh);
	
      if(p == NULL)
	break;

      auto str = trim_space(buf);

      if(str.empty())
	continue;
      
      auto v = string_split(str, "=");
	
      if(v.size() != 2)
	{
	  LOG(WARNING, "invalid config line: %s", buf);
	  continue;
	}

      auto key = v[0];
      auto val = v[1];
      key = trim_space(key);
      val = trim_space(val);

      if(key.empty() || val.empty())
	{
	  LOG(WARNING, "invalid config line: %s", buf);
	  continue;
	}

      _kvs[key] = val;
    }
    
  fclose(fh);

  LOG(INFO, "config parse success");
  return true;
}

unsigned long get_file_size(const char *path)
{
  unsigned long filesize = -1;
  FILE *fp;
  fp = fopen(path, "r");
  if(fp == NULL)
    return filesize;
  fseek(fp, 0L, SEEK_END);
  filesize = ftell(fp);
  fclose(fp);
  return filesize;
}

std::string get_file_content(const char* path)
{
  std::string str;
  
  char* buf = NULL;
  FILE* fp = NULL;
  int cnt;

  auto size = get_file_size(path);

  if(size <= 0)
    goto LERROR;
  
  buf = (char*)malloc(size+1);

  if(buf == NULL)
    goto LERROR;

  fp = fopen(path, "r");

  if(fp == NULL)
    goto LERROR;

  cnt = fread(buf, 1, size, fp);

  if(cnt <= 0 || cnt != size)
    goto LERROR;

  buf[cnt] = 0x00;

  str = buf;
  
  free(buf);
  fclose(fp);
  
  return str;
  
 LERROR:
  
  if(buf != NULL)
    free(buf);
  
  if(fp != NULL)
    fclose(fp);
  
  return "";
}

//-----------------------------------------------------------------------------------------

#if 1

void SocketHelper::set_non_blocking(int sock)
{
  if(sock <= 0)
    {
      LOG(WARNING, "invalid sock @ set_non_blocking");
      return;
    }
  
  int opts;
  opts=fcntl(sock,F_GETFL);
  if(opts<0) 
    perror("fcntl(sock,GETFL)");
  
  opts = opts|O_NONBLOCK;
  if(fcntl(sock,F_SETFL,opts)<0) 
    perror("fcntl(sock,SETFL,opts)");
}

int SocketHelper::accept(int sock)
{
  struct sockaddr_in clientaddr;
  socklen_t length = sizeof(clientaddr);
  
  int connfd = ::accept(sock,(sockaddr *)&clientaddr, &length);
  
  if(connfd < 0)
    {
      perror("accept");
      sleep(1); 
    }

  if(connfd == 0)
    {
      LOG(WARNING, "accept return 0");
      sleep(1); 
    }

  return connfd;
}

int SocketHelper::listen(int port)
{
  int ret = 0;
  int listenfd = 0;
  struct sockaddr_in server_addr;
  struct sockaddr_in client_addr;
  int yes = 1;
  
  listenfd = ::socket(AF_INET, SOCK_STREAM, 0);

  if(listenfd == -1)
    {
      perror("socket");
      return 0;
    }

  ret  = ::setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));

  if(ret == -1)
    {
      perror("setsockopt");
      goto ERROR;
    }
  
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = INADDR_ANY;
  memset(server_addr.sin_zero, '\0', sizeof(server_addr.sin_zero));
  
  ret = ::bind(listenfd, (struct sockaddr *)&(server_addr), sizeof(server_addr));

  if(ret == -1)
    {
      perror("bind");
      goto ERROR;
    }

  ret = ::listen(listenfd, 10000);

  if(ret == -1){
      perror("listen");
      goto ERROR;
  }
  
  return listenfd;

 ERROR:
  
  if(listenfd > 0)
    ::close(listenfd);
  
  return 0;
}

int SocketHelper::epoll_create(int sock, struct epoll_event** epoll_events, int client_cnt)
{
  struct epoll_event ev;
  struct epoll_event * events = (struct epoll_event *)malloc((client_cnt+1) * sizeof(struct epoll_event));
  
  if(events == NULL)
    {
      LOG(WARNING, "out of memory");
      exit(1);
    }
  
  int epfd = ::epoll_create(client_cnt+1);

  if(epfd <= 0)
    {
      LOG(WARNING, "epoll_create: %s", strerror(errno));
      exit(1);
    }

  ev.data.fd=sock;
  ev.events=EPOLLIN;

  int ret = ::epoll_ctl(epfd, EPOLL_CTL_ADD, sock, &ev);

  if(0 != ret)
    {
      LOG(WARNING, "epoll_ctl: %s", strerror(errno));
      exit(1);
    }

  *epoll_events = events;

  return epfd;
}

int SocketHelper::epoll_wait(int epfd, struct epoll_event* events, int client_cnt)
{
  int nfds= ::epoll_wait(epfd, events, client_cnt+1, 1000);
  if(nfds < 0)
    LOG(WARNING, "epoll_wait: %s", strerror(errno));
  return nfds;
}

void SocketHelper::epoll_add(int epfd, int connfd, void* ptr)
{
  struct epoll_event ev;

  if(ptr)
    ev.data.ptr = ptr;
  else
    ev.data.fd = connfd;
  ev.events = EPOLLIN;

  int ret = epoll_ctl(epfd,EPOLL_CTL_ADD,connfd,&ev);

  if(ret < 0) 
    LOG(WARNING, "epoll_ctl: %s", strerror(errno));
  
  return;
}

void SocketHelper::epoll_loop(int port, int client_cnt, std::function<std::shared_ptr<Client>(int)> client_creater)
{
  static std::map<Client*, std::shared_ptr<Client>> keeper;
  
  struct epoll_event * events = NULL;

  auto sock = SocketHelper::listen(port);
  
  SocketHelper::set_non_blocking(sock);
  
  int epfd = SocketHelper::epoll_create(sock, &events, client_cnt);

  LOG(INFO, "epoll_loop is ready for epoll_wait");
  
  for(;;)
    {
      int nfds =  SocketHelper::epoll_wait(epfd, events, client_cnt);
    
      for(int i=0; i<nfds; ++i)
	{
	  if(sock == events[i].data.fd)
	    {
	      int fd = SocketHelper::accept(sock);
	      if(fd <= 0)
		continue;
	      
	      SocketHelper::set_non_blocking(fd);
	      auto client = client_creater(fd);
	      SocketHelper::epoll_add(epfd, fd, (void*)client.get());
	      keeper[client.get()] = client;
	    }
	  else
	    {
	      Client* client = (Client*)events[i].data.ptr;
	      auto fd = client->get_fd();
	      auto cnt = ::recv(fd, client->buf(), client->space(), 0);
	      if(cnt > 0)
		{
		  client->recved(cnt);
		}
	      else
		{
		  client->set_fd(0);
		  ::epoll_ctl(epfd, EPOLL_CTL_DEL, fd, 0);
		  ::close(fd);
		  keeper.erase(client);
		  
		  if(cnt == 0)
		    LOG(WARNING, "client %s socket closed...", client->name().c_str());
		  if(cnt <  0)
		    LOG(WARNING, "client %s recv failed: %s", strerror(errno));
		}
	    }
	}
    }
  
  free(events);
}

#endif

//-----------------------------------------------------------------------------------------


