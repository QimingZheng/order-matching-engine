#include "order_matching_engine.h"

#include <assert.h>

#include <iostream>

Order::Order(OrderType type, ticker_t ticker, price_t price,
             quantity_t quantity, unix_time_t timestamp)
    : ticker_(ticker),
      type_(type),
      price_(price),
      quantity_(quantity),
      timestamp_(timestamp) {}

bool Order::operator<(const Order& other) const {
  if (type_ == OrderType::BUY) {
    if (price_ != other.price_) {
      return price_ < other.price_;
    }
    return timestamp_ < other.timestamp_;
  } else {
    if (price_ != other.price_) {
      return price_ > other.price_;
    }
    return timestamp_ < other.timestamp_;
  }
}

bool Order::operator>(const Order& other) const { return other < *this; }

ticker_t& Order::GetTicker() { return ticker_; }
bool Order::IsBuyOrder() const { return type_ == OrderType::BUY; }
bool Order::IsSellOrder() const { return type_ == OrderType::SELL; }
order_id_t Order::GetOrderId() const { return order_id_; }
price_t Order::GetPrice() const { return price_; }
quantity_t Order::GetQuantity() const { return quantity_; }

void Order::SetOrderId(order_id_t id) const { order_id_ = id; }

Order::Order() {}

void Order::Match(order_id_t order_id, quantity_t quantity) const {
  quantity_ -= quantity;
  return matching_orders_.push_back({order_id, quantity});
}

std::vector<std::pair<price_t, quantity_t>> SingleTickerOrderBook::GetNthBuy(
    int nth) {
  std::unique_lock<std::mutex> _(mtx_);
  std::vector<Order> top_orders;
  while (top_orders.size() < nth && !buy_side_orders_.empty()) {
    top_orders.push_back(std::move(buy_side_orders_.top()));
    buy_side_orders_.pop();
  }
  std::vector<std::pair<price_t, quantity_t>> ret(top_orders.size());
  std::transform(top_orders.begin(), top_orders.end(), ret.begin(),
                 [](const Order& order) -> std::pair<price_t, quantity_t> {
                   return {order.GetPrice(), order.GetQuantity()};
                 });
  while (!top_orders.empty()) {
    buy_side_orders_.emplace(std::move(top_orders.back()));
    top_orders.pop_back();
  }
  return ret;
}

std::vector<std::pair<price_t, quantity_t>> SingleTickerOrderBook::GetNthSell(
    int nth) {
  std::unique_lock<std::mutex> _(mtx_);
  std::vector<Order> top_orders;
  while (top_orders.size() < nth && !sell_side_orders_.empty()) {
    top_orders.push_back(std::move(sell_side_orders_.top()));
    sell_side_orders_.pop();
  }
  std::vector<std::pair<price_t, quantity_t>> ret(top_orders.size());
  std::transform(top_orders.begin(), top_orders.end(), ret.begin(),
                 [](const Order& order) -> std::pair<price_t, quantity_t> {
                   return {order.GetPrice(), order.GetQuantity()};
                 });
  while (!top_orders.empty()) {
    sell_side_orders_.emplace(std::move(top_orders.back()));
    top_orders.pop_back();
  }
  return ret;
}

void SingleTickerOrderBook::ProcessNewOrder(Order& order) {
  std::unique_lock<std::mutex> _(mtx_);
  if (order.IsBuyOrder()) {
    while (!sell_side_orders_.empty() && order.GetQuantity() > 0) {
      auto& best_sell = sell_side_orders_.top();
      if (best_sell.GetPrice() <= order.GetPrice()) {
        if (best_sell.GetQuantity() > order.GetQuantity()) {
          auto match_quantity = order.GetQuantity();
          order.Match(best_sell.GetOrderId(), match_quantity);
          fulfilled_orders_.emplace_back(std::move(order));
          best_sell.Match(order.GetOrderId(), match_quantity);
          break;
        } else {
          auto match_quantity = best_sell.GetQuantity();
          order.Match(best_sell.GetOrderId(), match_quantity);
          best_sell.Match(order.GetOrderId(), match_quantity);
          fulfilled_orders_.emplace_back(std::move(best_sell));
          sell_side_orders_.pop();
        }
      } else {
        break;
      }
    }
    if (order.GetQuantity() > 0) buy_side_orders_.emplace(order);
  } else {
    while (!buy_side_orders_.empty() && order.GetQuantity() > 0) {
      auto& best_buy = buy_side_orders_.top();
      if (best_buy.GetPrice() >= order.GetPrice()) {
        if (best_buy.GetQuantity() > order.GetQuantity()) {
          auto match_quantity = order.GetQuantity();
          order.Match(best_buy.GetOrderId(), match_quantity);
          fulfilled_orders_.emplace_back(std::move(order));
          best_buy.Match(order.GetOrderId(), match_quantity);
          break;
        } else {
          auto match_quantity = best_buy.GetQuantity();
          order.Match(best_buy.GetOrderId(), match_quantity);
          best_buy.Match(order.GetOrderId(), match_quantity);
          fulfilled_orders_.emplace_back(std::move(best_buy));
          buy_side_orders_.pop();
        }
      } else {
        break;
      }
    }
    if (order.GetQuantity() > 0) sell_side_orders_.emplace(order);
  }
}

OrderMatchingEngine::OrderMatchingEngine(size_t thread_num) {
  for (auto i = 0; i < thread_num; i++) {
    threads_.emplace_back(std::thread(&OrderMatchingEngine::Execute, this));
  }
}

OrderMatchingEngine::~OrderMatchingEngine() {
  {
    std::unique_lock<std::mutex> lock(mtx_);
    stopped_ = true;
  }
  cv_.notify_all();
  for (auto& thread : threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

void OrderMatchingEngine::AddOrder(Order&& order) {
  order.SetOrderId(next_order_id_.fetch_add(1));
  {
    std::unique_lock<std::mutex> lock(mtx_);

    pending_orders_.emplace(std::forward<Order>(order));
  }
  cv_.notify_one();
}

void OrderMatchingEngine::SetUp(const std::unordered_set<ticker_t>& tickers) {
  for (auto ticker : tickers) {
    books_[ticker] = std::make_shared<SingleTickerOrderBook>();
  }
}

std::vector<std::pair<price_t, quantity_t>> OrderMatchingEngine::GetNthBuy(
    ticker_t ticker, int nth) {
  auto iter = books_.find(ticker);
  assert(iter != books_.end());
  std::shared_ptr<SingleTickerOrderBook> book = iter->second;
  return book->GetNthBuy(nth);
}

std::vector<std::pair<price_t, quantity_t>> OrderMatchingEngine::GetNthSell(
    ticker_t ticker, int nth) {
  auto iter = books_.find(ticker);
  assert(iter != books_.end());
  std::shared_ptr<SingleTickerOrderBook> book = iter->second;
  return book->GetNthSell(nth);
}

void OrderMatchingEngine::Execute() {
  while (true) {
    Order order;
    {
      std::unique_lock<std::mutex> lock(mtx_);
      cv_.wait(lock, [this] { return stopped_ || !pending_orders_.empty(); });
      if (stopped_ && pending_orders_.empty()) {
        return;
      }
      order = std::move(pending_orders_.front());
      pending_orders_.pop();
    }
    auto iter = books_.find(order.GetTicker());
    assert(iter != books_.end());
    std::shared_ptr<SingleTickerOrderBook> book = iter->second;
    book->ProcessNewOrder(order);
  }
}
