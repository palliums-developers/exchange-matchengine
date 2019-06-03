#ifndef __UTILITIES_HPP__
#define __UTILITIES_HPP__

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
void log2(int level, const char* file, int line, const char* func, const char *format, ...);
#define LOG(lvl, ...) log2((lvl), __FILE__, __LINE__, __func__, __VA_ARGS__)

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

template<class T> void dot(const T & t)
{
  std::cout << t << std::flush;
}

std::string timestamp_2_string(uint32_t stamp);

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

unsigned long get_file_size(const char *path);
std::string get_file_content(const char* path);

// -------------------------------------------------------

struct Config
{
  static Config* instance()
  {
    static Config a; return &a;
  }

  bool parse(const char* path);

  bool check(std::string key)
  {
    if(_kvs.count(key) == 0)
      {
	LOG(WARNING, "can not find key in config: %s", key.c_str());
	return false;
      }
    return true;
  }
  
  std::string get(std::string key)
  {
    if(!check(key)) return "";
    return _kvs[key];
  }

  int get_int(std::string key)
  {
    if(!check(key)) return -1;
    return atoi(_kvs[key].c_str());
  }

  long get_long(std::string key)
  {
    if(!check(key)) return -1;
    return atol(_kvs[key].c_str());
  }

  double get_double(std::string key)
  {
    if(!check(key)) return -1;
    return atof(_kvs[key].c_str());
  }
  
private:
  
  std::map<std::string, std::string> _kvs;
};

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
    T timed_pop()
    {
      std::unique_lock<std::mutex> lk(_mtx);
      if(_cv.wait_for(lk, 1000ms, [this]{ return !_v.empty(); }))
	{
	  auto t = _v.front();
	  _v.pop_front();
	  return t;
	}
      return T();
    }
    bool empty()
    {
      std::unique_lock<std::mutex> lk(_mtx);
      return _v.empty();
    }

    T peek()
    {
      std::unique_lock<std::mutex> lk(_mtx);
      return _v.empty() ? T() : _v.front();
    }
    
    T try_pop()
    {
      std::unique_lock<std::mutex> lk(_mtx);
      if(_v.empty())
	return T();
      auto t = _v.front();
      _v.pop_front();
      return t;
    }

    int size()
    {
      std::unique_lock<std::mutex> lk(_mtx);
      return _v.size();
    }
    
  private:
    std::mutex _mtx;
    std::condition_variable _cv;
    std::list<T> _v;
  };
  
} // namespace utils

// ----------------------------------------------------------------------------

class Client;
class Message;
class ClientManager;

struct Client : std::enable_shared_from_this<Client>
{
  
  Client(int fd)
  {
    static long client_count = 0;

    _id = client_count++;
    
    _fd = fd;
    _buf[sizeof(_buf)-1] = 0x00;

    char str[80];
    sprintf(str, "client_%08d", _id);
    _name = str;
  }
  
  int get_fd() { return _fd; }
  void set_fd(int fd) { _fd = fd; }
  std::string name() { return _name; }
  int id() { return _id; }
  char* buf() { return &_buf[_pos]; }
  int space() { return sizeof(_buf) - _pos -1; }
  void set_qmsg(utils::Queue<std::shared_ptr<Message>>* qmsg) { _qmsg = qmsg; }
  bool send_closed() { return _send_closed; }
  void set_send_closed() { _send_closed = true; }
  
  void recved(int cnt)
  {
    _pos += cnt;
    
    split();
    
    if(_msgs.empty())
      return;

    auto msg = _msgs.front();
    _msgs.pop_front();

    assert(_qmsg != NULL);
    _qmsg->push(std::make_shared<Message>(shared_from_this(), msg));
  }

  void split()
  {
    const char* linesplit = "@#$";
    
    char* p = _buf;
    _buf[_pos] = 0x00;
    
    while(p < &_buf[_pos])
      {
	auto e = strstr(p, linesplit);
	if(e == NULL)
	  break;
	*e = 0x00;
	_msgs.push_back(p);
	p = e+strlen(linesplit);
      }

    int left = _pos-(p-&_buf[0]);
    
    if(left > 0 && p != &_buf[0])
      memmove(_buf, p, left);
    
    _pos = left;
  }

  utils::Queue<std::shared_ptr<Message>>* _qmsg = NULL;
  
private:
  
  std::list<std::string> _msgs;
  char _buf[8192];
  int _pos = 0;
  int _fd = 0;
  int _id = 0;
  std::string _name;
  bool _send_closed = false;
};

struct Message
{
  Message(std::shared_ptr<Client> client, std::string data)
  {
    _client = client;
    _data = data;
  }
  
  std::shared_ptr<Client> client() { return _client; }
  std::string data() { return _data; }
  void set_data(std::string data) { _data = data; }

private:
  std::shared_ptr<Client> _client;
  std::string _data;
};

struct ClientManager
{
  std::map<int, std::shared_ptr<Client>> _clients;
};

struct SocketHelper
{
  
  static void set_non_blocking(int sock);
  static int  accept(int sock);
  static int  listen(int port);
  
  static int  epoll_create(int sock, struct epoll_event** epoll_events, int client_cnt);
  static int  epoll_wait(int epfd, struct epoll_event* events, int client_cnt);
  static void epoll_add(int epfd, int connfd, void* ptr);
  static void epoll_loop(int port, int client_cnt, std::function<std::shared_ptr<Client>(int)> client_creater);
  static int  connect(const char * serverip, const int serverport);
  
};

// ----------------------------------------------------------------------------

#endif
