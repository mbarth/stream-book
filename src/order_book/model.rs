use std::collections::HashSet;
use std::time::UNIX_EPOCH;
use std::{cmp::Ordering, collections::BinaryHeap};

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

impl Default for OrderSide {
    fn default() -> Self {
        Self::Bid
    }
}

impl Default for Order {
    fn default() -> Self {
        Self {
            exchange: String::from("DefaultExchange"),
            price: 0.0,
            amount: 0.0,
            timestamp: UNIX_EPOCH, // Earliest possible time
            order_side: OrderSide::default(),
        }
    }
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
#[derive(Debug)]
pub struct OrderBook {
    /// The bid orders in the order book.
    bid_orders: BinaryHeap<Order>,
    /// The ask orders in the order book.
    ask_orders: BinaryHeap<Order>,

    exchanges: HashSet<String>,
}

/// Snapshot containing only the top bids, asks, and the spread.
#[derive(Debug, Clone)]
pub struct OrderBookSnapshot {
    pub top_bids: Vec<Order>,
    pub top_asks: Vec<Order>,
    pub spread: f64,
    pub timestamp: std::time::SystemTime,
}

impl Default for OrderBookSnapshot {
    fn default() -> Self {
        Self {
            top_bids: Vec::new(),
            top_asks: Vec::new(),
            spread: 0.0,
            timestamp: UNIX_EPOCH, // Earliest possible time
        }
    }
}

impl OrderBook {
    /// Creates a new, empty order book.
    pub fn new() -> Self {
        Self {
            bid_orders: BinaryHeap::new(),
            ask_orders: BinaryHeap::new(),
            exchanges: HashSet::new(),
        }
    }

    /// Adds a bid order to the order book.
    pub fn add_bid(&mut self, exchange: &str, price: f64, amount: f64) {
        self.bid_orders.push(Order {
            exchange: exchange.to_string(),
            price,
            amount,
            timestamp: std::time::SystemTime::now(),
            order_side: OrderSide::Bid,
        });
        self.exchanges.insert(exchange.to_string());
    }

    /// Adds an ask order to the order book.
    pub fn add_ask(&mut self, exchange: &str, price: f64, amount: f64) {
        self.ask_orders.push(Order {
            exchange: exchange.to_string(),
            price,
            amount,
            timestamp: std::time::SystemTime::now(),
            order_side: OrderSide::Ask,
        });
        self.exchanges.insert(exchange.to_string());
    }

    pub fn exchanges_count(&self) -> usize {
        self.exchanges.len()
    }

    // Returns the top n bid orders from the order book.
    fn top_bids(&self, n: usize) -> Vec<Order> {
        let mut cloned_data = self.bid_orders.clone();
        cloned_data.drain().take(n).collect()
    }

    // Returns the top n ask orders from the order book.
    fn top_asks(&self, n: usize) -> Vec<Order> {
        let mut cloned_data = self.ask_orders.clone();
        cloned_data.drain().take(n).collect()
    }

    pub fn generate_snapshot(&mut self, n: usize) -> OrderBookSnapshot {
        let top_bids = self.top_bids(n);
        let top_asks = self.top_asks(n);
        trace!("bids len: {} asks len: {}", top_bids.len(), top_asks.len());
        let spread = self.calculate_spread(&top_bids, &top_asks);
        OrderBookSnapshot {
            top_bids,
            top_asks,
            spread,
            timestamp: std::time::SystemTime::now(),
        }
    }

    fn calculate_spread(&self, top_bids: &[Order], top_asks: &[Order]) -> f64 {
        match (top_bids.get(0), top_asks.get(0)) {
            (Some(bid), Some(ask)) => bid.price - ask.price,
            _ => 0.0,
        }
    }
}
#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_add_bid() {
        let mut order_book = OrderBook::new();
        order_book.add_bid("test_exchange", 5000.0, 1.0);
        assert_eq!(order_book.bid_orders.len(), 1);
    }

    #[test]
    fn test_add_ask() {
        let mut order_book = OrderBook::new();
        order_book.add_ask("test_exchange", 5000.0, 1.0);
        assert_eq!(order_book.ask_orders.len(), 1);
    }

    #[test]
    fn test_top_bids() {
        let mut order_book = OrderBook::new();
        order_book.add_bid("test_exchange", 5000.0, 1.0);
        order_book.add_bid("test_exchange", 6000.0, 1.5);
        let top_bids = order_book.top_bids(1);
        assert_eq!(top_bids.len(), 1);
        assert_eq!(top_bids[0].price, 6000.0);
    }

    #[test]
    fn test_top_asks() {
        let mut order_book = OrderBook::new();
        order_book.add_ask("test_exchange", 5000.0, 1.0);
        order_book.add_ask("test_exchange", 4000.0, 1.5);
        let top_asks = order_book.top_asks(1);
        assert_eq!(top_asks.len(), 1);
        assert_eq!(top_asks[0].price, 4000.0);
    }

    #[test]
    fn test_calculate_spread() {
        let mut order_book = OrderBook::new();
        order_book.add_bid("test_exchange", 5000.0, 1.0);
        order_book.add_ask("test_exchange", 4000.0, 1.0);
        let top_bids = order_book.top_bids(1);
        let top_asks = order_book.top_asks(1);
        let spread = order_book.calculate_spread(&top_bids, &top_asks);
        assert_eq!(spread, 1000.0);
    }

    #[test]
    fn test_calculate_spread_no_orders() {
        let order_book = OrderBook::new();
        let top_bids = order_book.top_bids(1);
        let top_asks = order_book.top_asks(1);
        let spread = order_book.calculate_spread(&top_bids, &top_asks);
        assert_eq!(spread, 0.0);
    }

    #[test]
    fn test_orders_from_multiple_exchanges() {
        let mut order_book = OrderBook::new();
        order_book.add_bid("exchange1", 5000.0, 1.0);
        order_book.add_bid("exchange2", 6000.0, 1.5);
        assert_eq!(order_book.exchanges_count(), 2);
        let top_bids = order_book.top_bids(2);
        assert_eq!(top_bids.len(), 2);
        assert!(top_bids.iter().any(|order| order.exchange == "exchange1"));
        assert!(top_bids.iter().any(|order| order.exchange == "exchange2"));
    }

    #[test]
    fn test_order_order_by_price_and_time() {
        let mut order_book = OrderBook::new();
        order_book.add_bid("exchange1", 5000.0, 1.0);
        thread::sleep(Duration::from_millis(10)); // to ensure a different timestamp
        order_book.add_bid("exchange1", 5000.0, 1.5);
        let top_bids = order_book.top_bids(2);
        assert_eq!(top_bids.len(), 2);
        assert!(top_bids[0].timestamp < top_bids[1].timestamp);
    }
}
