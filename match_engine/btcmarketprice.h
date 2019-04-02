#ifndef __BTC_MARKET_PRICE_HPP__
#define __BTC_MARKET_PRICE_HPP__

struct BtcPriceUpdater
{
  bool load();
  void run(volatile bool * alive);

  int do_get_btc_price();

  double get_btc_price_by_usd()
  {
    std::unique_lock<std::mutex> lk(_mtx);
    return _usd;
  }

  double get_btc_price_by_cny()
  {
    std::unique_lock<std::mutex> lk(_mtx);
    return _cny;
  }

  void dump()
  {
    LOG(INFO, "btc market price: usd %f, cny %f", _usd, _cny);
  }
  
private:
  
  double _usd;
  double _cny;
  std::mutex _mtx;
};

#endif
