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
  
  char str[512];
  sprintf(str, "%s %s %s:%d %s %s\n", timestamp_2_string(now).c_str(), lvlstr.c_str(), file, line, func, task.c_str());
  printf("%s", str);
  
  va_list args;
  va_start(args, format);
  vprintf(format, args);
  va_end(args);
  
  printf("\n");
}

