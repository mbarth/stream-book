//! Module implementing the OrderbookAggregator service.

use std::{pin::Pin, sync::Arc};

use futures::Stream;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

use order_book_proto::{orderbook_aggregator_server::OrderbookAggregator, Empty, Level, Summary};

use crate::order_book::model::OrderBook;
use crate::utils::config::Config;

/// Generated protocol buffers definitions from the orderbook.proto file.
pub mod order_book_proto {
    tonic::include_proto!("orderbook");
}

/// Structure encapsulating an OrderBook shared among threads,
/// enabling concurrent access to the order book.
#[derive(Debug, Default)]
pub struct StreamBookAggregator {
    pub orderbook: Arc<RwLock<OrderBook>>,
    pub config: Config,
}

/// Implementation of the OrderbookAggregator service defined in the .proto file.
/// It provides methods to interact with the OrderBook.
#[tonic::async_trait]
impl OrderbookAggregator for StreamBookAggregator {
    /// Type of the stream returned by the `book_summary` method.
    type BookSummaryStream =
        Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send + Sync + 'static>>;

    /// Async function that streams a summary of the order book to the client upon request.
    /// This summary includes the spread, top 10 bids, and top 10 asks.
    async fn book_summary(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::BookSummaryStream>, Status> {
        info!("Got a gRPC request: {:?}", _request);

        // Create an asynchronous channel with a buffer size of 4.

        // NOTE: I'm not sure what the appropriate buffer size should be. Increasing the buffer size
        // allows more messages to be sent to the channel without waiting for the receiver to
        // process them. This improves throughput if we're receiving messages faster than they are
        // consumed, meaning the service can continue processing other work while the channel is
        // full. However, this comes at a cost of increased memory usage. It can also lead to higher
        // latency for individual messages, because they might end up waiting in the channel longer
        // before being processed. Need to figure out if the producer and consumer can keep pace
        // with each other in which case a smaller buffer size should be appropriate. If the
        // producer (binance/bitstamp) occasionally bursts many messages, a larger buffer size might
        // be beneficial to handle the bursts.

        let (tx, rx) = tokio::sync::mpsc::channel(4);

        // Clone Arc reference to order book.
        let orderbook = Arc::clone(&self.orderbook);

        // Spawn new task that fetches and sends order book summaries.
        let top_bids_and_asks_count = self.config.app.top_bids_and_asks_count;
        tokio::spawn(async move {
            loop {
                // Acquire read lock on order book.
                let read_guard = orderbook.read().await;

                // Get top 10 bids and asks and construct a summary.
                let mut bids = vec![];
                let mut asks = vec![];
                for bid in read_guard.top_bids(top_bids_and_asks_count) {
                    bids.push(Level {
                        exchange: bid.exchange.clone(),
                        price: bid.price,
                        amount: bid.amount,
                    });
                }
                for ask in read_guard.top_asks(top_bids_and_asks_count) {
                    asks.push(Level {
                        exchange: ask.exchange.clone(),
                        price: ask.price,
                        amount: ask.amount,
                    });
                }
                let summary = Summary {
                    spread: read_guard.spread().unwrap_or(0.0),
                    bids,
                    asks,
                };

                // since we're flushing the order book periodically, only send the summary when
                // there's something to send.
                if !summary.bids.is_empty() {
                    // Send the summary to the client. If sending fails, assume the client has disconnected.
                    if tx.send(Ok(summary.clone())).await.is_err() {
                        debug!("Client disconnected");
                        break;
                    }
                }
            }
        });

        // Return a response that includes a stream of order book summaries.
        Ok(Response::new(Box::pin(futures::stream::unfold(
            rx,
            |mut rx| async {
                // Receive the next item from the stream. If receiving fails, log an error and return a Status.
                let item = match rx.recv().await {
                    Some(Ok(value)) => Ok(value),
                    Some(Err(err)) => {
                        return Some((Err(err), rx));
                    }
                    None => {
                        error!("Failed to receive item");
                        return Some((Err(Status::unknown("Failed to receive item")), rx));
                    }
                };
                Some((item, rx))
            },
        ))))
    }
}

mod tests {
    // NOTE: Can't figure out why clippy complains about these imports. It could be because the
    // tests should actually go in the generated code. Since it can't we just ignore the errors.
    #[allow(unused_imports)]
    use futures::StreamExt;

    #[allow(unused_imports)]
    use super::*;

    #[tokio::test]
    async fn book_summary_test() {
        // Set up the OrderBook with known values.
        let mut orderbook = OrderBook::new();
        orderbook.add_bid("binance", 0.0677, 16.312);
        orderbook.add_ask("bitstamp", 0.06767424, 1.85489701);

        let order_book = Arc::new(RwLock::new(orderbook));
        let config = Config::new().unwrap(); // Initialize with test configuration

        let aggregator = StreamBookAggregator {
            orderbook: Arc::clone(&order_book),
            config,
        };

        // Issue a book_summary request.
        let request = Request::new(Empty {});
        let response = aggregator.book_summary(request).await.unwrap();

        // Get the stream from the response.
        let mut stream = response.into_inner();

        // Get the first item from the stream.
        let first_item = stream.next().await;

        // Check that the first item is as expected.
        // let diff: f64 = 0.0677 - 0.06767424;
        match first_item {
            Some(Ok(summary)) => {
                assert_eq!(summary.spread, (0.0677 - 0.06767424));

                // Assert the bids.
                for (_i, level) in summary.bids.iter().enumerate() {
                    assert_eq!(level.price, 0.0677);
                    assert_eq!(level.amount, 16.312);
                    assert_eq!(level.exchange, "binance");
                }

                // Assert the asks.
                for (_i, level) in summary.asks.iter().enumerate() {
                    assert_eq!(level.price, 0.06767424);
                    assert_eq!(level.amount, 1.85489701);
                    assert_eq!(level.exchange, "bitstamp");
                }
            }
            Some(Err(e)) => panic!("Received error from stream: {:?}", e),
            None => panic!("Stream ended unexpectedly"),
        };
    }
}
