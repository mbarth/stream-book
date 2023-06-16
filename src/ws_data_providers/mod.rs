use serde::{Deserialize, Serialize};

use crate::ws_data_providers::ws_async_client_factory::{BINANCE_EXCHANGE, BITSTAMP_EXCHANGE};

mod binance_ws_client;
mod bitstamp_client;
mod ws_async_client_factory;
pub mod ws_listeners;

// ########################### Generalized structs ############################

#[derive(Deserialize, Serialize, Debug)]
pub struct ExchangePriceLevel {
    pub exchange: String,
    pub price: f64,
    pub amount: f64,
}

impl ExchangePriceLevel {
    pub fn key(&self) -> String {
        format!("{}:{}:{}", self.exchange, self.price, self.amount)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ExchangeSummary {
    pub spread: f64,
    pub asks: Vec<ExchangePriceLevel>,
    pub bids: Vec<ExchangePriceLevel>,
}

#[derive(Serialize, Debug)]
struct ExchangeOrderBookMessage {
    bids: Vec<ExchangePriceLevel>,
    asks: Vec<ExchangePriceLevel>,
}

// ########################### Bitstamp structs ############################

#[derive(Serialize)]
struct SubscribeData {
    channel: String,
}

#[derive(Serialize)]
struct SubscribeMessage {
    event: String,
    data: SubscribeData,
}

#[derive(Deserialize, Debug)]
struct BitstampPriceLevel {
    price: String,
    amount: String,
}

#[derive(Deserialize)]
struct BitstampOrderBookMessage {
    data: BitstampOrderBookData,
}

#[derive(Deserialize)]
struct BitstampOrderBookData {
    bids: Vec<BitstampPriceLevel>,
    asks: Vec<BitstampPriceLevel>,
}

impl From<BitstampPriceLevel> for ExchangePriceLevel {
    fn from(level: BitstampPriceLevel) -> Self {
        ExchangePriceLevel {
            exchange: BITSTAMP_EXCHANGE.to_string(),
            price: level.price.parse::<f64>().unwrap_or(0.0),
            amount: level.amount.parse::<f64>().unwrap_or(0.0),
        }
    }
}

impl From<BitstampOrderBookMessage> for ExchangeOrderBookMessage {
    fn from(msg: BitstampOrderBookMessage) -> Self {
        ExchangeOrderBookMessage {
            bids: msg
                .data
                .bids
                .into_iter()
                .map(ExchangePriceLevel::from)
                .collect(),
            asks: msg
                .data
                .asks
                .into_iter()
                .map(ExchangePriceLevel::from)
                .collect(),
        }
    }
}

// ########################### Binance structs ############################

#[derive(Deserialize, Debug)]
struct BinancePriceLevel {
    price: String,
    amount: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct BinanceOrderBookMessage {
    bids: Vec<BinancePriceLevel>,
    asks: Vec<BinancePriceLevel>,
}

impl From<BinancePriceLevel> for ExchangePriceLevel {
    fn from(level: BinancePriceLevel) -> Self {
        ExchangePriceLevel {
            exchange: BINANCE_EXCHANGE.to_string(),
            price: level.price.parse::<f64>().unwrap_or(0.0),
            amount: level.amount.parse::<f64>().unwrap_or(0.0),
        }
    }
}

impl From<BinanceOrderBookMessage> for ExchangeOrderBookMessage {
    fn from(msg: BinanceOrderBookMessage) -> Self {
        ExchangeOrderBookMessage {
            bids: msg.bids.into_iter().map(ExchangePriceLevel::from).collect(),
            asks: msg.asks.into_iter().map(ExchangePriceLevel::from).collect(),
        }
    }
}
