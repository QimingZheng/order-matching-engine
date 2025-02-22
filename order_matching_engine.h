#pragma once

#include <condition_variable>
#include <queue>
#include <thread>
#include <unordered_set>

using ticker_t = std::string;
using price_t = float;
using quantity_t = long;
using unix_time_t = long;
using order_id_t = long;

class OrderMatchingEngine;

class Order {
 public:
  enum class OrderType {
    BUY,
    SELL,
  };

  Order(OrderType type, ticker_t ticker, price_t price, quantity_t quantity,
        unix_time_t timestamp);

  bool operator<(const Order& other) const;
  bool operator>(const Order& other) const;

  ticker_t& GetTicker();

  bool IsBuyOrder() const;
  bool IsSellOrder() const;
  void SetOrderId(order_id_t id) const;
  order_id_t GetOrderId() const;
  price_t GetPrice() const;
  quantity_t GetQuantity() const;
  void Match(order_id_t, quantity_t) const;

 private:
  friend OrderMatchingEngine;
  Order();
  mutable order_id_t order_id_;

  ticker_t ticker_;
  OrderType type_;
  price_t price_;
  mutable quantity_t quantity_;
  unix_time_t timestamp_;

  mutable std::vector<std::pair<order_id_t, quantity_t>> matching_orders_;
};

class SingleTickerOrderBook {
 public:
  void ProcessNewOrder(Order& order);

  std::vector<std::pair<price_t, quantity_t>> GetNthBuy(int nth);
  std::vector<std::pair<price_t, quantity_t>> GetNthSell(int nth);

 private:
  ticker_t ticker_;
  std::priority_queue<Order, std::vector<Order>, std::less<Order>>
      buy_side_orders_;
  std::priority_queue<Order, std::vector<Order>, std::less<Order>>
      sell_side_orders_;
  std::mutex mtx_;

  std::vector<Order> fulfilled_orders_;
};

class OrderMatchingEngine {
 public:
  OrderMatchingEngine(size_t thread_num);
  ~OrderMatchingEngine();

  std::vector<std::pair<price_t, quantity_t>> GetNthBuy(ticker_t, int);
  std::vector<std::pair<price_t, quantity_t>> GetNthSell(ticker_t, int);

  void SetUp(const std::unordered_set<ticker_t>&);

  OrderMatchingEngine(const OrderMatchingEngine&) = delete;
  OrderMatchingEngine& operator=(const OrderMatchingEngine&) = delete;
  OrderMatchingEngine(OrderMatchingEngine&&) = delete;
  OrderMatchingEngine& operator=(OrderMatchingEngine&&) = delete;

  void AddOrder(Order&& order);

 private:
  void Execute();

  std::queue<Order> pending_orders_;
  std::mutex mtx_;
  std::condition_variable cv_;

  std::vector<std::thread> threads_;
  std::unordered_map<ticker_t, std::shared_ptr<SingleTickerOrderBook>> books_;

  std::atomic<order_id_t> next_order_id_;

  bool stopped_ = false;
};
