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
#include <cctype>
#include <chrono>

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

