use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
};

use tracing::trace;

/// Represents an order in the order book.
#[derive(Clone, Debug)]
pub struct Order {
    /// The exchange where the order is located.
    pub exchange: String,
    /// The price of the order.
    pub price: f64,
    /// The amount of the order.
    pub amount: f64,
    /// The time when the order was placed.
    pub timestamp: std::time::SystemTime,
    /// The side of the order, either bid or ask.
    pub order_side: OrderSide,
}

/// The side of an order, either bid or ask.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OrderSide {
    Bid,
    Ask,
}

// Arrange by price first and next by time order was received.
impl Ord for Order {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.price < other.price {
            match self.order_side {
                OrderSide::Bid => Ordering::Less,
                OrderSide::Ask => Ordering::Greater,
            }
        } else if self.price > other.price {
            match self.order_side {
                OrderSide::Bid => Ordering::Greater,
                OrderSide::Ask => Ordering::Less,
            }
        } else {
            // next first order in wins
            other.timestamp.cmp(&self.timestamp)
        }
    }
}

impl PartialOrd for Order {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Order {
    fn eq(&self, other: &Self) -> bool {
        if self.price != other.price {
            false
        } else {
            self.timestamp == other.timestamp
        }
    }
}

impl Eq for Order {}

/// An order book, containing bid and ask orders.
#[derive(Debug, Default)]
pub struct OrderBook {
    /// The bid orders in the order book.
    bid_price_levels: HashMap<String, Order>,
    /// The ask orders in the order book.
    ask_price_levels: HashMap<String, Order>,
}

impl OrderBook {
    /// Creates a new, empty order book.
    pub fn new() -> Self {
        Self {
            bid_price_levels: HashMap::new(),
            ask_price_levels: HashMap::new(),
        }
    }

    /// Adds a bid order to the order book.
    pub fn add_bid(&mut self, exchange: &str, price: f64, amount: f64) {
        self.bid_price_levels
            .entry(format!("{}:{}", exchange, price))
            .and_modify(|order| {
                order.amount += amount;
            })
            .or_insert(Order {
                exchange: exchange.to_string(),
                price,
                amount,
                timestamp: std::time::SystemTime::now(),
                order_side: OrderSide::Bid,
            });
    }

    /// Adds an ask order to the order book.
    pub fn add_ask(&mut self, exchange: &str, price: f64, amount: f64) {
        self.ask_price_levels
            .entry(format!("{}:{}", exchange, price))
            .and_modify(|order| {
                order.amount += amount;
            })
            .or_insert(Order {
                exchange: exchange.to_string(),
                price,
                amount,
                timestamp: std::time::SystemTime::now(),
                order_side: OrderSide::Ask,
            });
    }

    /// Returns the top n bid orders from the order book.
    pub fn top_bids(&self, n: usize) -> Vec<Order> {
        let mut cloned_data = self.bid_price_levels.clone();
        OrderBook::sort_and_filter_values(n, &mut cloned_data)
    }

    /// Returns the top n ask orders from the order book.
    pub fn top_asks(&self, n: usize) -> Vec<Order> {
        let mut cloned_data = self.ask_price_levels.clone();
        OrderBook::sort_and_filter_values(n, &mut cloned_data)
    }

    /// Clears out the order book. Since we're not matching any orders, or receiving any
    /// updates or cancels, this can be used to clear out the orders to only show the latest
    /// most revelant orders.
    pub fn flush_order_book(&mut self, threshold: usize) -> bool {
        let mut flushed = false;
        if self.bid_price_levels.len() > threshold {
            trace!("Flushing order book");
            self.bid_price_levels.clear();
            self.ask_price_levels.clear();
            flushed = true;
        }
        flushed
    }

    // Sort the values in the passed in hashmap and then grab the top n values.
    fn sort_and_filter_values(n: usize, data: &mut HashMap<String, Order>) -> Vec<Order> {
        let mut sorted_data: Vec<Order> = Vec::new();
        // Convert values of HashMap into a BinaryHeap
        let mut binary_heap: BinaryHeap<Order> = BinaryHeap::new();
        binary_heap.extend(data.values().cloned());
        // Grab the top n values
        for _ in 0..n {
            if let Some(data) = binary_heap.pop() {
                sorted_data.push(data);
            }
        }
        sorted_data
    }

    /// Returns the spread of the order book, which is the difference between the best ask price
    /// and the best bid price.
    pub fn spread(&self) -> Option<f64> {
        let mut bids_sorted: BinaryHeap<Order> = BinaryHeap::new();
        bids_sorted.extend(self.bid_price_levels.values().cloned());
        let best_bid = bids_sorted.peek();

        let mut asks_sorted: BinaryHeap<Order> = BinaryHeap::new();
        asks_sorted.extend(self.ask_price_levels.values().cloned());
        let best_ask = asks_sorted.peek();

        match (best_bid, best_ask) {
            (Some(bid), Some(ask)) => Some(bid.price - ask.price),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use super::*;

    #[test]
    fn test_add_bid() {
        let mut order_book = OrderBook::new();
        order_book.add_bid("binance", 0.06745, 12.7339);

        assert_eq!(order_book.bid_price_levels.len(), 1);

        let bid = order_book.bid_price_levels.get("binance:0.06745");
        assert!(bid.is_some());

        let bid = bid.unwrap();
        assert_eq!(bid.exchange, "binance");
        assert_eq!(bid.price, 0.06745);
        assert_eq!(bid.amount, 12.7339);
        assert_eq!(bid.order_side, OrderSide::Bid);
    }

    #[test]
    fn test_add_ask() {
        let mut order_book = OrderBook::new();
        order_book.add_ask("binance", 0.06745, 8.7224);

        assert_eq!(order_book.ask_price_levels.len(), 1);

        let ask = order_book.ask_price_levels.get("binance:0.06745");
        assert!(ask.is_some());

        let ask = ask.unwrap();
        assert_eq!(ask.exchange, "binance");
        assert_eq!(ask.price, 0.06745);
        assert_eq!(ask.amount, 8.7224);
        assert_eq!(ask.order_side, OrderSide::Ask);
    }

    #[test]
    fn test_top_bids() {
        let mut order_book = OrderBook::new();
        order_book.add_bid("binance", 0.06745, 12.7339);
        order_book.add_bid("binance", 0.06746, 13.7297);
        order_book.add_bid("binance", 0.06747, 11.5123);

        let top_bids = order_book.top_bids(2);
        assert_eq!(top_bids.len(), 2);

        let bid1 = top_bids.get(0).unwrap();
        assert_eq!(bid1.exchange, "binance");
        assert_eq!(bid1.price, 0.06747);
        assert_eq!(bid1.amount, 11.5123);
        assert_eq!(bid1.order_side, OrderSide::Bid);

        let bid2 = top_bids.get(1).unwrap();
        assert_eq!(bid2.exchange, "binance");
        assert_eq!(bid2.price, 0.06746);
        assert_eq!(bid2.amount, 13.7297);
        assert_eq!(bid2.order_side, OrderSide::Bid);
    }

    #[test]
    fn test_top_asks() {
        let mut order_book = OrderBook::new();
        order_book.add_ask("binance", 0.06745, 8.7224);
        order_book.add_ask("binance", 0.06746, 13.748);
        order_book.add_ask("binance", 0.06747, 9.5123);

        let top_asks = order_book.top_asks(2);
        assert_eq!(top_asks.len(), 2);

        let ask1 = top_asks.get(0).unwrap();
        assert_eq!(ask1.exchange, "binance");
        assert_eq!(ask1.price, 0.06745);
        assert_eq!(ask1.amount, 8.7224);
        assert_eq!(ask1.order_side, OrderSide::Ask);

        let ask2 = top_asks.get(1).unwrap();
        assert_eq!(ask2.exchange, "binance");
        assert_eq!(ask2.price, 0.06746);
        assert_eq!(ask2.amount, 13.748);
        assert_eq!(ask2.order_side, OrderSide::Ask);
    }

    #[test]
    fn test_spread_with_data() {
        let mut order_book = OrderBook::new();
        order_book.add_bid("binance", 0.06745, 12.7339);
        order_book.add_ask("binance", 0.06746, 8.7224);

        let spread = order_book.spread().unwrap();
        assert_approx_eq::assert_approx_eq!(spread, 0.00001, 2f64);
    }

    #[test]
    fn test_spread_without_data() {
        let order_book = OrderBook::new();

        let spread = order_book.spread();
        assert_eq!(spread, None);
    }

    #[test]
    fn test_order_comparison() {
        let timestamp1 = SystemTime::now();
        let timestamp2 = timestamp1 + Duration::new(5, 0);
        assert!(timestamp1 < timestamp2);
        let order1 = Order {
            exchange: "exchange1".to_string(),
            price: 0.06745,
            amount: 8.7224,
            timestamp: timestamp1,
            order_side: OrderSide::Bid,
        };
        let order2 = Order {
            exchange: "exchange2".to_string(),
            price: 0.06745,
            amount: 8.7224,
            timestamp: timestamp2,
            order_side: OrderSide::Bid,
        };
        let order3 = Order {
            exchange: "exchange3".to_string(),
            price: 0.06750,
            amount: 8.7224,
            timestamp: timestamp1,
            order_side: OrderSide::Bid,
        };
        assert!(order1 > order2); // older order wins, FIFO
        assert_eq!(order1, order1.clone());
        assert!(order1 < order3);
    }

    #[test]
    fn test_add_existing_bid() {
        let mut order_book = OrderBook::new();
        order_book.add_bid("binance", 0.06745, 12.7339);
        assert_eq!(order_book.bid_price_levels.len(), 1);
        order_book.add_bid("binance", 0.06745, 7.2661);
        assert_eq!(order_book.bid_price_levels.len(), 1);
        let bid = order_book.bid_price_levels.get("binance:0.06745").unwrap();
        assert_eq!(bid.amount, 20.0);
    }

    #[test]
    fn test_flush_order_book() {
        let mut order_book = OrderBook::new();
        for i in 0..10 {
            order_book.add_bid("binance", 0.06745 + (i as f64 * 0.00001), 12.7339);
        }
        assert_eq!(order_book.bid_price_levels.len(), 10);
        assert!(!order_book.flush_order_book(11));
        assert_eq!(order_book.bid_price_levels.len(), 10);
        assert!(order_book.flush_order_book(5));
        assert_eq!(order_book.bid_price_levels.len(), 0);
    }

    #[test]
    fn test_top_bids_more_than_orders() {
        let mut order_book = OrderBook::new();
        order_book.add_bid("binance", 0.06745, 12.7339);
        let top_bids = order_book.top_bids(2);
        assert_eq!(top_bids.len(), 1);
    }

    #[test]
    fn test_spread_no_bids_or_asks() {
        let order_book = OrderBook::new();
        assert_eq!(order_book.spread(), None);
    }
}
