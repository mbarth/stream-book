use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use anyhow::Context;
use futures::{
    future::{self, AbortHandle},
    prelude::stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde::de::DeserializeOwned;
use tokio::sync::broadcast::{Sender as BroadcastSender, Sender};
use tokio::sync::mpsc::{Receiver, Sender as MpscSender};
use tokio::{net::TcpStream, task::JoinHandle};
use tokio_native_tls::TlsStream;
use tokio_tungstenite::{tungstenite::protocol::Message, WebSocketStream};
use tracing::{debug, error, info, trace};

use crate::order_book::model::{OrderBook, OrderBookSnapshot};
use crate::utils::config::Config;
use crate::ws_data_providers::ws_async_client_factory::{
    WsAsyncClientFactory, BINANCE_EXCHANGE, BITSTAMP_EXCHANGE,
};
use crate::ws_data_providers::{
    BinanceOrderBookMessage, BitstampOrderBookMessage, ExchangeOrderBookMessage,
};

// Function to start WebSocket listeners for different exchanges and aggregates the data into a shared OrderBook
pub async fn start_ws_listeners(
    snapshot_sender: BroadcastSender<OrderBookSnapshot>,
    config: &Config,
) -> anyhow::Result<JoinHandle<()>> {
    // set up ctrl-c handler to stop the listener threads
    let (abort_handle, should_abort) = abort_handlers()?;

    // Retrieve WebSocket clients for binance and bitstamp exchanges.
    let factory = WsAsyncClientFactory::new(config);
    let binance_ws_client = factory.get_ws_client(BINANCE_EXCHANGE)?;
    let bitstamp_ws_client = factory.get_ws_client(BITSTAMP_EXCHANGE)?;
    let (binance_sink, binance_stream) = binance_ws_client.get_sink_and_stream().await?;
    let (bitstamp_sink, bitstamp_stream) = bitstamp_ws_client.get_sink_and_stream().await?;

    // Create channel for sending and receiving messages between tasks
    let (message_sender, message_receiver) = tokio::sync::mpsc::channel(100);

    // Spawn tasks to listen and parse messages from Binance and Bitstamp
    let binance_parser_task = spawn_message_listener(
        binance_sink,
        binance_stream,
        message_sender.clone(),
        message_parser::<BinanceOrderBookMessage>,
        should_abort.clone(),
        abort_handle.clone(),
    );
    let bitstamp_parser_task = spawn_message_listener(
        bitstamp_sink,
        bitstamp_stream,
        message_sender,
        message_parser::<BitstampOrderBookMessage>,
        should_abort.clone(),
        abort_handle.clone(),
    );

    let aggregator_task = spawn_order_book_aggregator(
        config,
        message_receiver,
        snapshot_sender,
        abort_handle,
        should_abort,
    );

    // Spawn a main task to manage the three tasks (Binance listener, Bitstamp listener, and aggregator)
    let handle = tokio::task::spawn(async move {
        match tokio::try_join!(binance_parser_task, bitstamp_parser_task, aggregator_task) {
            Ok(_) => debug!("WS Subscribers completed."),
            Err(_) => error!("One or more tasks failed."),
        }
    });

    Ok(handle)
}

fn spawn_order_book_aggregator(
    config: &Config,
    mut channel_receiver: Receiver<ExchangeOrderBookMessage>,
    snapshot_sender: Sender<OrderBookSnapshot>,
    abort_handle: AbortHandle,
    should_abort: Arc<AtomicBool>,
) -> JoinHandle<()> {
    // HashSet used to exclude duplicates values received from the exchanges
    let mut exclude_duplicates = HashSet::new();

    // Spawn a task to aggregate order book data received from Binance and Bitstamp
    let mut order_book = OrderBook::new();
    let top_bids_and_asks_count = config.app.top_bids_and_asks_count;
    tokio::spawn(async move {
        info!("Aggregator_task thread started");
        while let Some(message) = channel_receiver.recv().await {
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

            for bid in bids_to_include {
                order_book.add_bid(&bid.exchange, bid.price, bid.amount);
            }
            for ask in asks_to_include {
                order_book.add_ask(&ask.exchange, ask.price, ask.amount);
            }

            // NOTE: aggregating data from 2 exchanges, therefore once we have data from both exchanges,
            // we generate a new snapshot and send it to the broadcast channel.
            if order_book.exchanges_count() > 1 {
                let snapshot = order_book.generate_snapshot(top_bids_and_asks_count);
                match snapshot_sender.send(snapshot.clone()) {
                    Ok(_) => {
                        trace!("Snapshot sent {:?}", snapshot);
                    }
                    Err(e) => {
                        error!("Error sending snapshot to snapshot_sender: {}", e);
                    }
                }
                order_book = OrderBook::new();
                exclude_duplicates.clear();
            }
        }
    })
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
    channel_sender: MpscSender<ExchangeOrderBookMessage>,
    message_parser: Parser,
    should_abort: Arc<AtomicBool>,
    abort_handle: AbortHandle,
) -> JoinHandle<()>
where
    Parser: Fn(&str) -> Result<ExchangeOrderBookMessage, serde_json::Error> + Send + Sync + 'static,
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
                            crate::emit_event!(
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
                                crate::emit_event!(
                                    tracing::Level::ERROR,
                                    "spawn_message_listener",
                                    msg
                                );
                                continue;
                            }
                        };
                        match message_parser(text) {
                            Ok(parsed_result) => {
                                if let Err(e) = channel_sender.send(parsed_result).await {
                                    let msg = format!("Error sending result on channel {:?}", e);
                                    crate::emit_event!(
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
                                crate::emit_event!(
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
                    crate::emit_event!(tracing::Level::ERROR, "spawn_message_listener", msg);
                }
            }
        }
    })
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
