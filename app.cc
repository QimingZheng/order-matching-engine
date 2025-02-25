#include <iostream>

#include "order_matching_engine.h"

int main() {
  OrderMatchingEngine engine(4);
  auto tickers = std::vector<ticker_t>{"GOOG", "MSFT", "META", "AMZN"};
  engine.SetUp(OrderBookType::PRIORITY_QUEUE, {"GOOG", "MSFT", "META", "AMZN"});
  auto mean_prices = std::vector<price_t>{100.0, 200.0, 300., 400.};
  for (auto i = 0; i < 1000000; i++) {
    auto id = random() % 4;
    auto ticker = tickers[id];
    engine.AddOrder(
        {random() % 2 ? Order::OrderType::BUY : Order::OrderType::SELL,
         tickers[id], mean_prices[id] + random() % 50, random() % 1000, i});
    if (i % 100000 == 0) {
      for (auto tickr : tickers) {
        auto buy_prices = engine.GetNthBuy(ticker, 5);
        auto sell_prices = engine.GetNthSell(ticker, 5);
        std::cout << ticker << "\n";
        for (auto price : buy_prices) {
          std::cout << "buy " << price.second << " @ " << price.first << "\n";
        }
        for (auto price : sell_prices) {
          std::cout << "sell " << price.second << " @ " << price.first << "\n";
        }
      }
    }
  }
  return 0;
}