#include <chrono>
#include <iostream>

#include "order_matching_engine.h"

constexpr auto kSamples = 10000000;

int main() {
  {
    auto beg = std::chrono::high_resolution_clock::now();
    OrderMatchingEngine engine(4);
    auto tickers = std::vector<ticker_t>{"GOOG", "MSFT", "META", "AMZN"};
    engine.SetUp(OrderBookType::TABLE, {"GOOG", "MSFT", "META", "AMZN"});
    auto mean_prices = std::vector<price_t>{100.0, 200.0, 300., 400.};
    for (auto i = 0; i < kSamples; i++) {
      auto id = random() % 4;
      auto ticker = tickers[id];
      engine.AddOrder(
          {random() % 2 ? Order::OrderSide::BUY : Order::OrderSide::SELL,
           tickers[id], mean_prices[id] + random() % 50, random() % 1000, i});
      if (i == kSamples - 1) {
        for (auto ticker : tickers) {
          auto buy_prices = engine.GetNthBuy(ticker, 5);
          auto sell_prices = engine.GetNthSell(ticker, 5);
          std::cout << ticker << "\n";
          for (auto price : buy_prices) {
            std::cout << "buy " << price.second << " @ " << price.first << "\n";
          }
          for (auto price : sell_prices) {
            std::cout << "sell " << price.second << " @ " << price.first
                      << "\n";
          }
        }
      }
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << kSamples * 1.0 /
                     std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                           beg)
                         .count()
              << " orders/us" << std::endl;
  }
  {
    auto beg = std::chrono::high_resolution_clock::now();
    OrderMatchingEngine engine(4);
    auto tickers = std::vector<ticker_t>{"GOOG", "MSFT", "META", "AMZN"};
    engine.SetUp(OrderBookType::PRIORITY_QUEUE,
                 {"GOOG", "MSFT", "META", "AMZN"});
    auto mean_prices = std::vector<price_t>{100.0, 200.0, 300., 400.};
    for (auto i = 0; i < kSamples; i++) {
      auto id = random() % 4;
      auto ticker = tickers[id];
      engine.AddOrder(
          {random() % 2 ? Order::OrderSide::BUY : Order::OrderSide::SELL,
           tickers[id], mean_prices[id] + random() % 50, random() % 1000, i});
      if (i == kSamples - 1) {
        for (auto ticker : tickers) {
          auto buy_prices = engine.GetNthBuy(ticker, 5);
          auto sell_prices = engine.GetNthSell(ticker, 5);
          std::cout << ticker << "\n";
          for (auto price : buy_prices) {
            std::cout << "buy " << price.second << " @ " << price.first << "\n";
          }
          for (auto price : sell_prices) {
            std::cout << "sell " << price.second << " @ " << price.first
                      << "\n";
          }
        }
      }
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << kSamples * 1.0 /
                     std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                           beg)
                         .count()
              << " orders/us" << std::endl;
  }
  return 0;
}