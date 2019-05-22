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

#include "financial_management.h"

int g_log_level = INFO;


int main()
{
  auto rsp = query("aaa");
  std::cout << "hello world!\n";
  return 0;
}
