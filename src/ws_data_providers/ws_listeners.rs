use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use anyhow::Context;
use crossbeam_channel::Sender;
use futures::{
    future::{self, AbortHandle},
    prelude::stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use native_tls::TlsConnector as NativeTlsConnector;
use serde::de::DeserializeOwned;
use tokio::{net::TcpStream, sync::RwLock, task::JoinHandle};
use tokio_native_tls::{TlsConnector, TlsStream};
use tokio_tungstenite::{
    client_async_with_config, tungstenite::protocol::Message, WebSocketStream,
};
use tracing::{debug, error, info, trace};
use url::Url;

use crate::order_book::model::OrderBook;
use crate::utils::config::Config;
use crate::ws_data_providers::{
    BinanceOrderBookMessage, BitstampOrderBookMessage, ExchangeOrderBookMessage, SubscribeData,
    SubscribeMessage,
};

// Function to start WebSocket listeners for different exchanges and aggregates the data into a shared OrderBook
pub async fn start_ws_listeners(
    order_book: Arc<RwLock<OrderBook>>,
    config: &Config,
) -> anyhow::Result<JoinHandle<()>> {
    // set up ctrl-c handler to stop the listener threads
    let (abort_handle, should_abort) = abort_handlers()?;

    // Initialize WebSocket clients for first and second configured exchanges (Binance and Bitstamp).
    let (first_exchange_sink, first_exchange_stream) = ws_async_client(
        &config.exchanges.first_exchange.websocket.address,
        "binance",
        None,
        None,
    )
    .await?;
    let (second_exchange_sink, second_exchange_stream) = ws_async_client(
        &config.exchanges.second_exchange.websocket.address,
        "bitstamp",
        config.exchanges.second_exchange.websocket.event.as_deref(),
        config
            .exchanges
            .second_exchange
            .websocket
            .channel
            .as_deref(),
    )
    .await?;

    // Create an unbounded channel for sending and receiving messages between tasks
    let (channel_sender, channel_receiver) = crossbeam_channel::unbounded();

    // Spawn tasks to listen and parse messages from Binance and Bitstamp
    let first_exchange_parser_task = spawn_message_listener(
        first_exchange_sink,
        first_exchange_stream,
        channel_sender.clone(),
        message_parser::<BinanceOrderBookMessage>,
        should_abort.clone(),
        abort_handle.clone(),
    );
    let second_exchange_parser_task = spawn_message_listener(
        second_exchange_sink,
        second_exchange_stream,
        channel_sender,
        message_parser::<BitstampOrderBookMessage>,
        should_abort.clone(),
        abort_handle.clone(),
    );

    // HashSet used to exclude duplicates values received from the exchanges
    let mut exclude_duplicates = HashSet::new();

    // Spawn a task to aggregate order book data received from Binance and Bitstamp
    let order_book_clone = Arc::clone(&order_book);
    let websocket_flush_threshold = config.exchanges.websocket_flush_threshold;
    let top_bids_and_asks_count = config.app.top_bids_and_asks_count;
    let aggregator_task = tokio::spawn(async move {
        info!("Aggregator_task thread started");
        for message in channel_receiver {
            // handler for ctrl-c
            if should_abort.load(Ordering::SeqCst) {
                abort_handle.abort();
                break;
            }

            // NOTE: Only care about bids/asks we haven't seen before, tracking them in new vectors
            // to minimize the time spent holding a write lock.
            let mut bids_to_include = vec![];
            for bid in message.bids.iter() {
                if exclude_duplicates.insert(format!("bid:{}", &bid.key())) {
                    bids_to_include.push(bid);
                }
            }
            let mut asks_to_include = vec![];
            for ask in message.asks.iter() {
                if exclude_duplicates.insert(format!("ask:{}", &ask.key())) {
                    asks_to_include.push(ask);
                }
            }
            let mut write_guard = order_book_clone.write().await;
            for bid in bids_to_include {
                write_guard.add_bid(&bid.exchange, bid.price, bid.amount);
            }
            for ask in asks_to_include {
                write_guard.add_ask(&ask.exchange, ask.price, ask.amount);
            }
            write_guard.generate_snapshot(top_bids_and_asks_count);
            // NOTE: Since we're not matching or cancelling/updating the order book, we periodically
            // flush it so the results reflect the most recent data received from the exchanges.
            if write_guard.flush_order_book(websocket_flush_threshold) {
                trace!("exclude_duplicates cleared");
                exclude_duplicates.clear();
            }
        }
    });

    // Spawn a main task to manage the three tasks (Binance listener, Bitstamp listener, and aggregator)
    let handle = tokio::task::spawn(async move {
        match tokio::try_join!(
            first_exchange_parser_task,
            second_exchange_parser_task,
            aggregator_task
        ) {
            Ok(_) => debug!("WS Subscribers completed."),
            Err(_) => error!("One or more tasks failed."),
        }
    });

    Ok(handle)
}

// Service will not stop when the websocket client listeners running. These ctrl-c handlers are used
// to gracefully shutdown the WebSocket client listeners.
fn abort_handlers() -> anyhow::Result<(AbortHandle, Arc<AtomicBool>)> {
    let (abort_handle, _) = future::AbortHandle::new_pair();
    let should_abort = Arc::new(AtomicBool::new(false));
    let should_abort_for_ctrlc = Arc::clone(&should_abort);
    ctrlc::set_handler(move || {
        should_abort_for_ctrlc.store(true, Ordering::SeqCst);
    })
    .context("Error setting up Ctrl-C handler")?;
    Ok((abort_handle, should_abort))
}

// Function to parse a message into a specific type
fn message_parser<T>(message: &str) -> Result<ExchangeOrderBookMessage, serde_json::Error>
where
    T: DeserializeOwned,
    ExchangeOrderBookMessage: From<T>,
{
    let parsed_result: T = serde_json::from_str(message)?;
    Ok(ExchangeOrderBookMessage::from(parsed_result))
}

// Function to spawn a WebSocket message listener for a specific exchange
fn spawn_message_listener<Parser>(
    mut ws_sink: SplitSink<WebSocketStream<TlsStream<TcpStream>>, Message>,
    mut ws_stream: SplitStream<WebSocketStream<TlsStream<TcpStream>>>,
    channel_sender: Sender<ExchangeOrderBookMessage>,
    message_parser: Parser,
    should_abort: Arc<AtomicBool>,
    abort_handle: AbortHandle,
) -> JoinHandle<()>
where
    Parser: Fn(&str) -> Result<ExchangeOrderBookMessage, serde_json::Error> + Send + 'static,
{
    tokio::spawn(async move {
        while let Some(message) = ws_stream.next().await {
            if should_abort.load(Ordering::SeqCst) {
                abort_handle.abort();
                break;
            }

            match message {
                Ok(msg) => {
                    if msg.is_ping() {
                        trace!("Received ping {}", msg);
                        if let Err(e) = ws_sink.send(Message::Pong(msg.into_data())).await {
                            let msg = format!("Error sending pong: {:?}", e);
                            crate::emit_error!(
                                tracing::Level::ERROR,
                                "spawn_message_listener",
                                msg
                            );
                        }
                    } else {
                        let text = match msg.to_text() {
                            Ok(text) => text,
                            Err(e) => {
                                let msg =
                                    format!("Failed to convert message {} to text: {:?}", msg, e);
                                crate::emit_error!(
                                    tracing::Level::ERROR,
                                    "spawn_message_listener",
                                    msg
                                );
                                continue;
                            }
                        };
                        match message_parser(text) {
                            Ok(parsed_result) => {
                                if let Err(e) = channel_sender.send(parsed_result) {
                                    let msg = format!("Error sending result on channel {:?}", e);
                                    crate::emit_error!(
                                        tracing::Level::ERROR,
                                        "spawn_message_listener",
                                        msg
                                    );
                                }
                            }
                            Err(e) => {
                                // NOTE: Bitstamp returns a `bts:subscription_succeeded` event message that fails parsing.
                                // Okay to ignore this message.
                                let msg =
                                    format!("Parsing failed on message: {} error: {}", text, e);
                                crate::emit_error!(
                                    tracing::Level::ERROR,
                                    "spawn_message_listener",
                                    msg
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    let msg = format!("Error retrieving message: {:?}", e);
                    crate::emit_error!(tracing::Level::ERROR, "spawn_message_listener", msg);
                }
            }
        }
    })
}

// Function to establish a WebSocket connection to a specific exchange. A `sink` and `stream` are
// returned to the caller. The sink is used to send messages to the exchange such as a PING message
// keeping the connection alive. The stream is used to receive messages from the exchange.
async fn ws_async_client(
    address: &str,
    exchange: &str,
    event: Option<&str>,
    channel: Option<&str>,
) -> anyhow::Result<(
    SplitSink<WebSocketStream<TlsStream<TcpStream>>, Message>,
    SplitStream<WebSocketStream<TlsStream<TcpStream>>>,
)> {
    // connection setup and handshake code
    let url = Url::parse(address)?;
    let domain = url
        .domain()
        .ok_or_else(|| anyhow::anyhow!("Domain not found in URL {}", url))?
        .to_string();
    let socket = TcpStream::connect((domain.as_str(), url.port().unwrap_or(443)))
        .await
        .context(format!("Failed to connect to {} host", exchange))?;
    let connector = NativeTlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .context(format!(
            "Failed to create TLS connector for {} host",
            exchange
        ))?;
    let stream = TlsConnector::from(connector)
        .connect(domain.as_str(), socket)
        .await
        .context(format!("Failed TLS handshake for {}", exchange))?;

    info!("Connecting to {}", address);

    let (ws_stream, _) = client_async_with_config(url, stream, None)
        .await
        .context(format!(
            "Failed to complete websocket handshake for host {}",
            exchange
        ))?;

    info!(
        "WebSocket handshake has been successfully completed for {}",
        exchange
    );
    let (mut write, read) = ws_stream.split();

    // NOTE: This function is shared by exchanges that work with a simple URL or another that requires
    // submitting a subscription request. If either `event` or `channel` is provided, it will be used to
    // construct the subscription request.
    if let (Some(event), Some(channel)) = (event, channel) {
        let subscribe_message = SubscribeMessage {
            event: String::from(event),
            data: SubscribeData {
                channel: String::from(channel),
            },
        };
        let subscribe_message_json = serde_json::to_string(&subscribe_message)?;
        write.send(Message::Text(subscribe_message_json)).await?;
    }

    Ok((write, read))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_parser_valid() {
        let message = "{\"bids\": [{\"price\": \"0.0\", \"amount\": \"0.0\"}],\"asks\": [{\"price\": \"0.0\", \"amount\": \"0.0\"}]}";
        let result = message_parser::<BinanceOrderBookMessage>(message);
        assert!(result.is_ok());
        let parsed_result = result.unwrap();
        assert_eq!(parsed_result.bids.len(), 1);
        assert_eq!(parsed_result.bids[0].exchange, "binance");

        let message = "{\"data\": {\"bids\": [{\"price\": \"0.0\", \"amount\": \"0.0\"}],\"asks\": [{\"price\": \"0.0\", \"amount\": \"0.0\"}]}}";
        let result = message_parser::<BitstampOrderBookMessage>(message);
        assert!(result.is_ok());
        let parsed_result = result.unwrap();
        assert_eq!(parsed_result.bids.len(), 1);
        assert_eq!(parsed_result.bids[0].exchange, "bitstamp");
    }

    #[test]
    fn test_message_parser_invalid() {
        let message = "invalid message";
        let result = message_parser::<BinanceOrderBookMessage>(message);
        assert!(result.is_err());

        let message = "invalid message";
        let result = message_parser::<BitstampOrderBookMessage>(message);
        assert!(result.is_err());
    }
}
