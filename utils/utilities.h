#ifndef __UTILITIES_HPP__
#define __UTILITIES_HPP__

template<class T> void dot(const T & t)
{
  std::cout << t << std::flush;
}

std::string timestamp_2_string(uint32_t stamp);


// -------------------------------------------------------

enum {
  DEBUG = 0,
  INFO,
  WARNING,
  ERROR,
  FATAL,
};

extern int g_log_level;

std::string green_text(const char* text); 
std::string red_text(const char* text);
std::string blue_text(const char* text);

void log(int level, const char* file, int line, const char* func, const char *format, ...);
#define LOG(lvl, ...) log((lvl), __FILE__, __LINE__, __func__, __VA_ARGS__)

// -------------------------------------------------------

template<class Container, class Element>
  void remove_if_ex(Container & v, std::function<bool(Element)> func)
{
  for(auto it = v.begin(); it != v.end();) {
    if(func(*it))
      it = v.erase(it);
    else
      it++;
  }
}
  
// -------------------------------------------------------

std::string string_join(const std::vector<std::string>& v, const std::string & s);
std::vector<std::string> string_split(const std::string & str, const std::string & s);
std::string map2json(const std::map<std::string, std::string> & v);
std::map<std::string, std::string> json2map(const std::string & json);
std::string trim_space(std::string str);
std::string trim_quote(std::string str);
std::vector<std::string> json_split(std::string str);
std::vector<std::string> json_split_kv(std::string str);
std::map<std::string, std::string> json_get_object(std::string str);
std::vector<std::string> json_get_array(std::string str);

// -------------------------------------------------------

namespace utils {
  
  template<class T>
  struct Queue
  {
    void push(T const & t)
    {
      {
	std::unique_lock<std::mutex> lk(_mtx);
	_v.push_back(t);
      }
      _cv.notify_one();
    }
    T pop()
    {
      std::unique_lock<std::mutex> lk(_mtx);
      _cv.wait(lk, [this]{ return !_v.empty(); });
      auto t = _v.front();
      _v.pop_front();
      return t;
    }
    bool empty()
    {
      std::unique_lock<std::mutex> lk(_mtx);
      return _v.empty();
    }
    
  private:
    std::mutex _mtx;
    std::condition_variable _cv;
    std::list<T> _v;
  };
  
} // namespace utils

// -------------------------------------------------------

#endif
