#include "order_matching_engine.h"

#include <assert.h>

#include <algorithm>
#include <iostream>

Order::Order(OrderSide side, ticker_t ticker, price_t price,
             quantity_t quantity, unix_time_t timestamp)
    : order_impl_(new Order::OrderImpl{
          0, ticker, side, price, quantity, timestamp, {}}) {}

bool Order::operator<(const Order& other) const {
  if (order_impl_->side == OrderSide::BUY) {
    if (order_impl_->price != other.order_impl_->price) {
      return order_impl_->price < other.order_impl_->price;
    }
    return order_impl_->timestamp < other.order_impl_->timestamp;
  } else {
    if (order_impl_->price != other.order_impl_->price) {
      return order_impl_->price > other.order_impl_->price;
    }
    return order_impl_->timestamp < other.order_impl_->timestamp;
  }
}

bool Order::operator>(const Order& other) const { return other < *this; }

ticker_t& Order::GetTicker() {
  assert(order_impl_ != nullptr);
  return order_impl_->ticker;
}
bool Order::IsBuyOrder() const {
  assert(order_impl_ != nullptr);
  return order_impl_->side == OrderSide::BUY;
}
bool Order::IsSellOrder() const {
  assert(order_impl_ != nullptr);
  return order_impl_->side == OrderSide::SELL;
}
order_id_t Order::GetOrderId() const {
  assert(order_impl_ != nullptr);
  return order_impl_->order_id;
}
price_t Order::GetPrice() const { return order_impl_->price; }
quantity_t Order::GetQuantity() const {
  assert(order_impl_ != nullptr);
  return order_impl_->quantity;
}
time_t Order::GetTimestamp() const {
  assert(order_impl_ != nullptr);
  return order_impl_->timestamp;
}

void Order::SetOrderId(order_id_t id) const {
  assert(order_impl_ != nullptr);
  order_impl_->order_id = id;
}

Order::Order() {}

void Order::Match(order_id_t order_id, quantity_t quantity) const {
  assert(order_impl_ != nullptr);
  order_impl_->quantity -= quantity;
  return order_impl_->matching_orders.push_back({order_id, quantity});
}

std::vector<std::pair<price_t, quantity_t>>
PriorityQueueBasedSingleTickerOrderBook::GetNthBuy(int nth) {
  std::unique_lock<std::mutex> _(mtx_);
  std::vector<Order> top_orders;
  while (top_orders.size() < nth && !buy_side_orders_.empty()) {
    top_orders.push_back(buy_side_orders_.top());
    buy_side_orders_.pop();
  }
  std::vector<std::pair<price_t, quantity_t>> ret(top_orders.size());
  std::transform(top_orders.begin(), top_orders.end(), ret.begin(),
                 [](const Order& order) -> std::pair<price_t, quantity_t> {
                   return {order.GetPrice(), order.GetQuantity()};
                 });
  while (!top_orders.empty()) {
    buy_side_orders_.emplace(top_orders.back());
    top_orders.pop_back();
  }
  return ret;
}

std::vector<std::pair<price_t, quantity_t>>
PriorityQueueBasedSingleTickerOrderBook::GetNthSell(int nth) {
  std::unique_lock<std::mutex> _(mtx_);
  std::vector<Order> top_orders;
  while (top_orders.size() < nth && !sell_side_orders_.empty()) {
    top_orders.push_back(sell_side_orders_.top());
    sell_side_orders_.pop();
  }
  std::vector<std::pair<price_t, quantity_t>> ret(top_orders.size());
  std::transform(top_orders.begin(), top_orders.end(), ret.begin(),
                 [](const Order& order) -> std::pair<price_t, quantity_t> {
                   return {order.GetPrice(), order.GetQuantity()};
                 });
  while (!top_orders.empty()) {
    sell_side_orders_.emplace(top_orders.back());
    top_orders.pop_back();
  }
  return ret;
}

void PriorityQueueBasedSingleTickerOrderBook::ProcessNewOrder(Order& order) {
  std::unique_lock<std::mutex> _(mtx_);
  auto unfulfilled_quantity = order.GetQuantity();
  if (order.IsBuyOrder()) {
    while (!sell_side_orders_.empty() && order.GetQuantity() > 0) {
      auto& best_sell = sell_side_orders_.top();
      if (best_sell.GetPrice() <= order.GetPrice()) {
        if (best_sell.GetQuantity() > order.GetQuantity()) {
          auto match_quantity = order.GetQuantity();
          order.Match(best_sell.GetOrderId(), match_quantity);
          best_sell.Match(order.GetOrderId(), match_quantity);
          unfulfilled_quantity = order.GetQuantity();
          fulfilled_orders_.emplace_back(order);
          break;
        } else {
          auto match_quantity = best_sell.GetQuantity();
          order.Match(best_sell.GetOrderId(), match_quantity);
          best_sell.Match(order.GetOrderId(), match_quantity);
          unfulfilled_quantity = order.GetQuantity();
          fulfilled_orders_.emplace_back(best_sell);
          sell_side_orders_.pop();
        }
      } else {
        break;
      }
    }
    if (unfulfilled_quantity > 0) buy_side_orders_.emplace(order);
  } else {
    while (!buy_side_orders_.empty() && order.GetQuantity() > 0) {
      auto& best_buy = buy_side_orders_.top();
      if (best_buy.GetPrice() >= order.GetPrice()) {
        if (best_buy.GetQuantity() > order.GetQuantity()) {
          auto match_quantity = order.GetQuantity();
          order.Match(best_buy.GetOrderId(), match_quantity);
          best_buy.Match(order.GetOrderId(), match_quantity);
          unfulfilled_quantity = order.GetQuantity();
          fulfilled_orders_.emplace_back(order);
          break;
        } else {
          auto match_quantity = best_buy.GetQuantity();
          order.Match(best_buy.GetOrderId(), match_quantity);
          best_buy.Match(order.GetOrderId(), match_quantity);
          fulfilled_orders_.emplace_back(best_buy);
          unfulfilled_quantity = order.GetQuantity();
          buy_side_orders_.pop();
        }
      } else {
        break;
      }
    }
    if (unfulfilled_quantity > 0) sell_side_orders_.emplace(order);
  }
}

void TableBasedSingleTickerOrderBook::ProcessNewOrder(Order& order) {
  //
  std::unique_lock<std::mutex> _(mtx_);
  auto unfulfilled_quantity = order.GetQuantity();
  if (order.IsBuyOrder()) {
    while (!sell_side_orders_.empty() && order.GetQuantity() > 0) {
      auto best_sell = GetLowestSell();
      if (best_sell.GetPrice() <= order.GetPrice()) {
        if (best_sell.GetQuantity() > order.GetQuantity()) {
          auto match_quantity = order.GetQuantity();
          order.Match(best_sell.GetOrderId(), match_quantity);
          best_sell.Match(order.GetOrderId(), match_quantity);
          unfulfilled_quantity = order.GetQuantity();
          fulfilled_orders_.emplace_back(order);
          break;
        } else {
          auto match_quantity = best_sell.GetQuantity();
          order.Match(best_sell.GetOrderId(), match_quantity);
          best_sell.Match(order.GetOrderId(), match_quantity);
          unfulfilled_quantity = order.GetQuantity();
          fulfilled_orders_.emplace_back(best_sell);
          PopLowestSell();
        }
      } else {
        break;
      }
    }
    if (unfulfilled_quantity > 0) InsertBuy(order);
  } else {
    while (!buy_side_orders_.empty() && order.GetQuantity() > 0) {
      auto best_buy = GetHighestBuy();
      if (best_buy.GetPrice() >= order.GetPrice()) {
        if (best_buy.GetQuantity() > order.GetQuantity()) {
          auto match_quantity = order.GetQuantity();
          order.Match(best_buy.GetOrderId(), match_quantity);
          best_buy.Match(order.GetOrderId(), match_quantity);
          unfulfilled_quantity = order.GetQuantity();
          fulfilled_orders_.emplace_back(order);
          break;
        } else {
          auto match_quantity = best_buy.GetQuantity();
          order.Match(best_buy.GetOrderId(), match_quantity);
          best_buy.Match(order.GetOrderId(), match_quantity);
          fulfilled_orders_.emplace_back(best_buy);
          unfulfilled_quantity = order.GetQuantity();
          PopHighestBuy();
        }
      } else {
        break;
      }
    }
    if (unfulfilled_quantity > 0) InsertSell(order);
  }
}

Order TableBasedSingleTickerOrderBook::GetHighestBuy() {
  return buy_side_orders_.begin()->second.begin()->second;
}
Order TableBasedSingleTickerOrderBook::GetLowestSell() {
  return sell_side_orders_.begin()->second.begin()->second;
}
void TableBasedSingleTickerOrderBook::PopHighestBuy() {
  buy_side_orders_[buy_side_orders_.begin()->first].erase(
      buy_side_orders_.begin()->second.begin()->first);

  if (buy_side_orders_.begin()->second.empty()) {
    buy_side_orders_.erase(buy_side_orders_.begin());
  }
}
void TableBasedSingleTickerOrderBook::PopLowestSell() {
  sell_side_orders_[sell_side_orders_.begin()->first].erase(
      sell_side_orders_.begin()->second.begin()->first);
  if (sell_side_orders_.begin()->second.empty()) {
    sell_side_orders_.erase(sell_side_orders_.begin());
  }
}
void TableBasedSingleTickerOrderBook::InsertBuy(Order& order) {
  if (buy_side_orders_.find(order.GetPrice()) == buy_side_orders_.end()) {
    buy_side_orders_.insert({order.GetPrice(), {}});
  }
  buy_side_orders_[order.GetPrice()].insert({order.GetTimestamp(), order});
}
void TableBasedSingleTickerOrderBook::InsertSell(Order& order) {
  if (sell_side_orders_.find(order.GetPrice()) == sell_side_orders_.end()) {
    sell_side_orders_.insert({order.GetPrice(), {}});
  }
  sell_side_orders_[order.GetPrice()].insert({order.GetTimestamp(), order});
}

std::vector<std::pair<price_t, quantity_t>>
TableBasedSingleTickerOrderBook::GetNthBuy(int nth) {
  std::unique_lock<std::mutex> _(mtx_);
  auto ret = std::vector<std::pair<price_t, quantity_t>>{};
  for (auto& buys : buy_side_orders_) {
    if (ret.size() >= nth) break;
    auto sum = 0.0;
    for (auto buy : buys.second) sum += buy.second.GetQuantity();
    ret.push_back({buys.first, sum});
  }
  return ret;
}

std::vector<std::pair<price_t, quantity_t>>
TableBasedSingleTickerOrderBook::GetNthSell(int nth) {
  std::unique_lock<std::mutex> _(mtx_);
  auto ret = std::vector<std::pair<price_t, quantity_t>>{};
  for (auto& sells : sell_side_orders_) {
    if (ret.size() >= nth) break;
    auto sum = 0.0;
    for (auto sell : sells.second) sum += sell.second.GetQuantity();
    ret.push_back({sells.first, sum});
  }
  return ret;
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

void OrderMatchingEngine::SetUp(OrderBookType type,
                                const std::unordered_set<ticker_t>& tickers) {
  for (auto ticker : tickers) {
    switch (type) {
      case OrderBookType::PRIORITY_QUEUE:
        books_[ticker] =
            std::make_shared<PriorityQueueBasedSingleTickerOrderBook>();
        break;

      case OrderBookType::TABLE:
        books_[ticker] = std::make_shared<TableBasedSingleTickerOrderBook>();
        break;

      default:
        throw std::runtime_error("Unknown OrderBookType");
        break;
    }
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
      order = pending_orders_.front();
      pending_orders_.pop();
    }
    auto iter = books_.find(order.GetTicker());
    assert(iter != books_.end());
    std::shared_ptr<SingleTickerOrderBook> book = iter->second;
    book->ProcessNewOrder(order);
  }
}
