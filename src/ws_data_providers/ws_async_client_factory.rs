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

pub const BINANCE_CLIENT_ID: &str = "binance";
pub const BITSTAMP_CLIENT_ID: &str = "bitstamp";

pub type WsSink = SplitSink<WebSocketStream<TlsStream<TcpStream>>, Message>;
pub type WsStream = SplitStream<WebSocketStream<TlsStream<TcpStream>>>;

#[async_trait]
pub trait WsClient<Sink, Stream>: Debug {
    // Function to establish a WebSocket connection to a specific exchange. A `sink` and `stream` are
    // returned to the caller. The sink is used to send messages to the exchange such as a PING message
    // keeping the connection alive. The stream is used to receive messages from the exchange.
    async fn get_sink_and_stream(&self) -> Result<(Sink, Stream)>;
}

pub trait WSAsyncClient<Sink, Stream> {
    fn get_ws_client(&self) -> Result<Box<dyn WsClient<Sink, Stream>>>;
}

pub struct WsAsyncClientFactory {
    ws_async_client_factory_map: HashMap<String, Box<dyn WSAsyncClient<WsSink, WsStream>>>,
}

impl WsAsyncClientFactory {
    pub fn new(config: &Config) -> Self {
        let mut ws_async_client_factory_map = HashMap::new();
        let binance_client = BinanceWsClient {
            address: config.exchanges.binance.address.clone(),
        };
        let bitstamp_client = BitstampWsClient {
            address: config.exchanges.bitstamp.address.clone(),
            event: config.exchanges.bitstamp.event.clone(),
            channel: config.exchanges.bitstamp.channel.clone(),
        };
        ws_async_client_factory_map.insert(
            BINANCE_CLIENT_ID.to_string(),
            Box::new(binance_client) as Box<dyn WSAsyncClient<WsSink, WsStream>>,
        );
        ws_async_client_factory_map.insert(
            BITSTAMP_CLIENT_ID.to_string(),
            Box::new(bitstamp_client) as Box<dyn WSAsyncClient<WsSink, WsStream>>,
        );
        WsAsyncClientFactory {
            ws_async_client_factory_map,
        }
    }

    pub fn get_ws_client(&self, name: &str) -> Result<Box<dyn WsClient<WsSink, WsStream>>> {
        match self.ws_async_client_factory_map.get(name) {
            Some(client_factory) => client_factory.get_ws_client(),
            None => Err(anyhow::anyhow!("No client registered with name: {}", name)),
        }
    }
}
