# Streambook

This is a mini project written in Rust that connects to two exchanges' WebSocket feeds
simultaneously, pulls order books for a given traded pair of currencies from each
exchange, merges and sorts the order books to create a combined order book, and
publishes the spread, top ten bids, and top ten asks as a stream through a gRPC server.

![Results Visualization](static/streambook.png)

## Requirements
Rust (version 1.5 or later)

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/mbarth/stream-book.git
   ```
2. Change to the project directory:

    ```bash
    cd stream-book
    ```

3. Build the project:

    ```bash
    cargo build --release
    ```
   
## Usage

1. The `local.toml` file under the `/config` directory holds the current URLs for the two 
   exchanges. Make any changes necessary if you would like a different currency pair. Currently,
   the currency pair used is `ETHBTC`.
   
    ```toml
    [exchanges.binance]
    address = "wss://stream.binance.com:9443/ws/ethbtc@depth20@100ms"
    
    [exchanges.bitstamp]
    address = "wss://ws.bitstamp.net"
    event = "bts:subscribe"
    channel = "order_book_ethbtc"
    ```

2. Start the gRPC server:

    ```bash
    cargo run --release
    ```

3. The gRPC server will be running and ready to accept client connections. You can now 
   connect to the server and consume the streaming spread, top ten bids, and top ten asks.
   The [grpcurl](https://github.com/fullstorydev/grpcurl) utility is an easy way to connect
   and test the gRPC server endpoints.
   
   ```bash
   # using reflection
   grpcurl -d '{}' -plaintext localhost:50051 orderbook.OrderbookAggregator/BookSummary
   
   # under the project's root directory and using the protobuf file
   grpcurl -plaintext -import-path ./proto -proto orderbook.proto -d '{}' 'localhost:50051' orderbook.OrderbookAggregator/BookSummary
   ```
   
4. Alternatively, there is a websocket endpoint available that returns similar data. The 
   [websocat](https://github.com/vi/websocat) utility is a simple client to view the results
   from the websocket endpoint.
   
   ```bash
   websocat ws://localhost:8080/ws
   ```
   
5. Lastly, there is a webpage built using the websocket endpoint available at http://localhost:8080
   that can be used to visualize the results.
   
6. To stop the server, issue a `ctrl+c` command in the same terminal where the service was started.

## Testing Instructions

1. To run the unit tests:

   ```bash
   cargo test
   ```
   
## Configurable Options

1. The number of Top Bids and Asks displayed is controlled by the `app.top_bids_and_asks_count` option. By default,
   the Bitstamp exchange returns 100 results and Binance results is controlled by its `depth20` value, here returning 
   20 results. Therefore, at most this value can be set to 120. Set this value accordingly to your results setting.
  
## Credit

* Thanks to [@idibidiart](https://gist.github.com/idibidiart/42e8abf6fde52f54cec58064f9fd5582) for 
  a sample of how to render the bids/asks chart using [D3.js](https://d3js.org/). The code is modified to work with 
  streaming websocket data.

* Thanks to [@dgtony](https://github.com/dgtony/orderbook-rs/tree/master) for their OrderBook Bid/Ask ordering logic,
  lines [18-53](https://github.com/dgtony/orderbook-rs/blob/master/src/engine/order_queues.rs#L18-L53).
  It's modified [slightly](https://github.com/mbarth/stream-book/blob/master/src/order_book/model.rs#L30-L66) 
  but the logic is basically the same.
