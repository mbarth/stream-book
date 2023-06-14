//! Module implementing the OrderbookAggregator service.

use std::pin::Pin;

use futures::Stream;
use tokio::sync::broadcast::{error::RecvError, Receiver};
use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

use order_book_proto::{orderbook_aggregator_server::OrderbookAggregator, Empty, Level, Summary};

use crate::order_book::model::OrderBookSnapshot;

/// Generated protocol buffers definitions from the orderbook.proto file.
pub mod order_book_proto {
    tonic::include_proto!("orderbook");
}

/// Structure encapsulating an OrderBook shared among threads,
/// enabling concurrent access to the order book.
#[derive(Debug)]
pub struct StreamBookAggregator {
    pub snapshot_receiver: Receiver<OrderBookSnapshot>,
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
        let mut snapshot_receiver = self.snapshot_receiver.resubscribe();
        let stream = async_stream::stream! {
            loop {
                match snapshot_receiver.recv().await {
                    Ok(order_book_snapshot) => {
                        yield Ok(Summary::from(order_book_snapshot));
                    },
                    Err(RecvError::Lagged(skipped)) => {
                        error!("Lagged with skipped: {}", skipped);
                        continue;
                    },
                    Err(RecvError::Closed) => {
                        debug!("Client disconnected");
                        break;
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }
}

impl From<OrderBookSnapshot> for Summary {
    fn from(snapshot: OrderBookSnapshot) -> Self {
        let bids = snapshot
            .top_bids
            .into_iter()
            .map(|bid| Level {
                exchange: bid.exchange,
                price: bid.price,
                amount: bid.amount,
            })
            .collect();

        let asks = snapshot
            .top_asks
            .into_iter()
            .map(|ask| Level {
                exchange: ask.exchange,
                price: ask.price,
                amount: ask.amount,
            })
            .collect();

        Summary {
            spread: snapshot.spread,
            bids,
            asks,
        }
    }
}
