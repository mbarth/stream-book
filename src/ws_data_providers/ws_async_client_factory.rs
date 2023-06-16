use std::collections::HashMap;
use std::fmt::Debug;

use anyhow::Result;
use futures::prelude::stream::{SplitSink, SplitStream};
use tokio::net::TcpStream;
use tokio_native_tls::TlsStream;
use tokio_tungstenite::{tungstenite::protocol::Message, WebSocketStream};

use async_trait::async_trait;

use crate::utils::config::Config;
use crate::ws_data_providers::binance_ws_client::BinanceWsClient;
use crate::ws_data_providers::bitstamp_client::BitstampWsClient;

pub const BINANCE_EXCHANGE: &str = "binance";
pub const BITSTAMP_EXCHANGE: &str = "bitstamp";

pub type WsSink = SplitSink<WebSocketStream<TlsStream<TcpStream>>, Message>;
pub type WsStream = SplitStream<WebSocketStream<TlsStream<TcpStream>>>;

#[async_trait]
pub trait WsAsyncClient<Sink, Stream>: Debug {
    // Function to establish a WebSocket connection to a specific exchange. A `sink` and `stream` are
    // returned to the caller. The sink is used to send messages to the exchange such as a PING message
    // keeping the connection alive. The stream is used to receive messages from the exchange.
    async fn get_sink_and_stream(&self) -> Result<(Sink, Stream)>;
}

pub trait WsClient<Sink, Stream> {
    fn get_ws_async_client(&self) -> Result<Box<dyn WsAsyncClient<Sink, Stream>>>;
}

pub struct WsAsyncClientFactory {
    factory_map: HashMap<String, Box<dyn WsClient<WsSink, WsStream>>>,
}

impl WsAsyncClientFactory {
    pub fn new(config: &Config) -> Self {
        let mut factory_map: HashMap<String, Box<dyn WsClient<WsSink, WsStream>>> = HashMap::new();
        let binance_client = BinanceWsClient {
            address: config.exchanges.binance.address.clone(),
        };
        let bitstamp_client = BitstampWsClient {
            address: config.exchanges.bitstamp.address.clone(),
            event: config.exchanges.bitstamp.event.clone(),
            channel: config.exchanges.bitstamp.channel.clone(),
        };
        factory_map.insert(BINANCE_EXCHANGE.to_string(), Box::new(binance_client));
        factory_map.insert(BITSTAMP_EXCHANGE.to_string(), Box::new(bitstamp_client));
        WsAsyncClientFactory { factory_map }
    }

    pub fn get_ws_async_client(
        &self,
        name: &str,
    ) -> Result<Box<dyn WsAsyncClient<WsSink, WsStream>>> {
        match self.factory_map.get(name) {
            Some(ws_client) => ws_client.get_ws_async_client(),
            None => Err(anyhow::anyhow!("No client registered with name: {}", name)),
        }
    }
}
