use std::sync::Arc;

use actix_files::NamedFile;
use actix_web::{
    get,
    web::{self, Payload},
    Error, HttpRequest, HttpResponse, Responder, Result,
};
use actix_ws::Message;
use futures::stream::StreamExt;
use serde_json::json;
use tokio::sync::{broadcast::Receiver, Mutex};
use tracing::{debug, info};

use crate::emit_event;
use crate::order_book::model::OrderBookSnapshot;
use crate::utils::config::Config;
use crate::ws_data_providers::{ExchangePriceLevel, ExchangeSummary};

/// Sanity check to ensure that the server is running.
#[get("/health")]
pub async fn health() -> impl Responder {
    HttpResponse::Ok().json(json!({ "status": "ok" }))
}

/// This is a websocket handler for the orderbook.
#[get("/ws")]
pub async fn ws(
    snapshot_receiver: web::Data<Arc<Mutex<Receiver<OrderBookSnapshot>>>>,
    req: HttpRequest,
    body: Payload,
) -> Result<HttpResponse, Error> {
    info!("Got a ws request: {:?}", req);
    let (response, mut session, mut msg_stream) = actix_ws::handle(&req, body)?;
    let mut session_clone = session.clone();
    actix_rt::spawn(async move {
        // Lock the Mutex and resubscribe
        let mut snapshot_receiver = snapshot_receiver.lock().await.resubscribe();
        while let Ok(order_book_snapshot) = snapshot_receiver.recv().await {
            let json_string =
                match serde_json::to_string(&ExchangeSummary::from(order_book_snapshot)) {
                    Ok(json_string) => json_string,
                    Err(err) => {
                        let msg = format!("Failed to serialize summary: {}", err);
                        emit_event!(tracing::Level::ERROR, "websocket_handler", msg);
                        continue;
                    }
                };
            if let Err(err) = session_clone.text(json_string).await {
                // Send the json to the client. If sending fails, assume the client has disconnected.
                debug!("Client disconnected: {}", err);
                break;
            }
        }
    });

    actix_rt::spawn(async move {
        while let Some(Ok(msg)) = msg_stream.next().await {
            match msg {
                Message::Ping(bytes) => {
                    if session.pong(&bytes).await.is_err() {
                        return;
                    }
                }
                Message::Pong(_) => (),
                Message::Close(reason) => {
                    let _ = session.close(reason).await;
                    info!("Session closed");
                    return;
                }
                _ => (),
            }
        }
        let _ = session.close(None).await;
    });

    Ok(response)
}

impl From<OrderBookSnapshot> for ExchangeSummary {
    fn from(snapshot: OrderBookSnapshot) -> Self {
        let bids = snapshot
            .top_bids
            .into_iter()
            .map(|bid| ExchangePriceLevel {
                exchange: bid.exchange,
                price: bid.price,
                amount: bid.amount,
            })
            .collect();

        let asks = snapshot
            .top_asks
            .into_iter()
            .map(|ask| ExchangePriceLevel {
                exchange: ask.exchange,
                price: ask.price,
                amount: ask.amount,
            })
            .collect();

        ExchangeSummary {
            spread: snapshot.spread,
            bids,
            asks,
        }
    }
}

/// Favicon handler
#[get("/favicon")]
pub async fn favicon() -> Result<impl Responder> {
    Ok(NamedFile::open("static/favicon.ico")?)
}

/// Default webpage display order book results.
#[get("/")]
pub async fn index(config: web::Data<Config>) -> HttpResponse {
    let s = r#"
<!DOCTYPE html>
<head>
    <meta charset="utf-8">
    <title>Streambook</title>
    <script type="text/javascript" src="https://d3js.org/d3.v4.min.js"></script>
    <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/lodash@4.17.4/lodash.min.js"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/normalize/8.0.1/normalize.min.css" integrity="sha512-NhSC1YmyruXifcj/KFRWoC561YpHpc5Jtzgvbuzx5VozKpWvQ+4nXhPdFgmx8xqexRcpAglTj9sIBWINXa8x5w==" crossorigin="anonymous" referrerpolicy="no-referrer" />
    <link rel="shortcut icon" type="image/x-icon" href="/favicon">
    <style>
        body {
            font-family: 'Helvetica Neue', Arial, sans-serif;
            font-size: 14px;
            line-height: 1.6;
            font-weight: 400;
            color: #333;
            margin: 10px;
        }
        
        .binance {
            color: #DAA520;
        }
        
        .bitstamp {
            color: #118E3B;
        }
        
        .float-right {
            float: right;
            margin-right: 20px;
        }
        
        .clear {
            clear: both;
        }

        .bar-bids {
            fill: #3c7df3;
        }

        .bar-asks {
            fill: #fca1b0;
        }

        .depth-chart {
            display: inline-block;
            float: left;
        }

        .container {
            display: grid;
            grid-template-columns: 1fr 1fr;
            grid-gap: 10px;
        }

        @media screen and (max-width: 600px) {
            .container {
                grid-template-columns: 1fr; /* Display as a single column on small screens */
            }
        }

        .centered-div {
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 10px;
        }
        
        .loader {
            border: 16px solid #f3f3f3;
            border-top: 16px solid #3498db;
            border-radius: 50%;
            width: 120px;
            height: 120px;
            animation: spin 2s linear infinite;
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
        }

        @-webkit-keyframes spin {
            0% { -webkit-transform: rotate(0deg); }
            100% { -webkit-transform: rotate(360deg); }
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        #content {
            display: none;
        }
    </style>
</head>
<body>
    <div class="loader" id="loadingSpinner"></div>
    <div id="content">
        <img class="float-right" width="200" height="200" src="/static/android-chrome-512x512.png">
        <h1>Streambook</h1>
        <div class="clear"></div>
        <h2 title="Spread is the difference between the top Bid and top Ask (Bid - Ask)">Spread: <span id="spread"></span></h2>
        <div class="centered-div">
            <div id="bids-chart" class="depth-chart"></div>
            <div id="asks-chart" class="depth-chart"></div>
        </div>
        <div class="container">
            <div class="column">
                <h2 title="Bids are offers to buy assets at a specific price.">Top {top_bids_and_asks_count} Bids</h2>
                <div id="bids"></div>
            </div>
            <div class="column">
                <h2 title="Asks are offers to sell assets at a specific price.">Top {top_bids_and_asks_count} Asks</h2>
                <div id="asks"></div>
            </div>
        </div>
    </div>
    
<script>
    /** BIDS SETUP =============================================== */
    let bidsData = []
    let __bidsCumulativeData = []
    let bidsMargin = {top: 12, right: 0, bottom: 12, left: 12},
        bidsWidth = 220 - bidsMargin.left - bidsMargin.right,
        bidsHeight = 220 - bidsMargin.top - bidsMargin.bottom

    // set bid ranges
    let bidsX = d3.scaleBand()
        .range([0, bidsWidth])
        .padding(0.1)
    let bidsY = d3.scaleLinear()
        .range([bidsHeight, 0])

    // append the svg object to a div ID
    let bidsSvg = d3.select('#bids-chart').append('svg')
        .attr('width', bidsWidth + bidsMargin.left + bidsMargin.right)
        .attr('height', bidsHeight + bidsMargin.top + bidsMargin.bottom)
        .append('g')
        .attr('transform',
            'translate(' + bidsMargin.left + ',' + bidsMargin.top + ')')

    /** ASKS SETUP =============================================== */
    let asksData = []
    let __asksCumulativeData = []
    let asksMargin = {top: 12, right: 12, bottom: 12, left: 0},
        asksWidth = 220 - asksMargin.left - asksMargin.right,
        asksHeight = 220 - asksMargin.top - asksMargin.bottom

    // set ask ranges
    let asksX = d3.scaleBand()
        .range([0, asksWidth])
        .padding(0.1)
    let asksY = d3.scaleLinear()
        .range([asksHeight, 0])

    // append the svg object to a div ID
    let asksSvg = d3.select('#asks-chart').append('svg')
        .attr('width', asksWidth + asksMargin.left + asksMargin.right)
        .attr('height', asksHeight + asksMargin.top + asksMargin.bottom)
        .append('g')
        .attr('transform',
            'translate(' + asksMargin.left + ',' + asksMargin.top + ')')

    let prefixSum = function (arr) {
        let builder = function (acc, n) {
            let lastNum = acc.length > 0 ? acc[acc.length - 1] : 0;
            acc.push(lastNum + n);
            return acc;
        };
        return _.reduce(arr, builder, []);
    }

    function onLoad() {
        console.log("Starting websocket connection");
        const socket = new WebSocket("ws://localhost:8080/ws");
        // Connection opened
        socket.addEventListener('open', (event) => {
            socket.send('Hello Server!');
            startHeartbeat();
        });

        // Listen for messages
        socket.addEventListener('message', (event) => {
            let data = JSON.parse(event.data);
            if (data.bids.length === {top_bids_and_asks_count}) {
                updatePage(data);
            }
        });
        
        socket.onerror = function(error) {
            document.body.innerHTML = 'Error: Unable to connect to the server.';
        };

        function startHeartbeat() {
            setInterval(() => {
                if (socket.readyState === WebSocket.OPEN) {
                    socket.send(JSON.stringify({action: "heartbeat"}));
                }
            }, 5000);
        }
        
        function exchangeElement(exchange) {
            let element = '<span class="binance">Binance</span>'
            if (exchange === "bitstamp") {
                element = '<span class="bitstamp">Bitstamp</span>'
            }
            return element
        }

        function updatePage(data) {
            // Get the elements
            let spreadElement = document.getElementById('spread');
            let asksElement = document.getElementById('asks');
            let bidsElement = document.getElementById('bids');

            // Clear the elements
            spreadElement.innerHTML = '';
            asksElement.innerHTML = '';
            bidsElement.innerHTML = '';

            // Add the spread, ask, and bid data
            spreadElement.innerHTML = `${data.spread}`;
            // Change the color based on the value
            if (data.spread < 0) {
                spreadElement.style.color = 'red';
            } else if (data.spread > 0) {
                spreadElement.style.color = 'green';
            } else {
                spreadElement.style.color = 'black';
            }
            
            for (let ask of data.asks) {
                asksElement.innerHTML += `Exchange: ${exchangeElement(ask.exchange)}, Price: ${ask.price}, Amount: ${ask.amount} <br/>`;
            }

            for (let bid of data.bids) {
                bidsElement.innerHTML += `Exchange: ${exchangeElement(bid.exchange)}, Price: ${bid.price}, Amount: ${bid.amount} <br/>`;
            }

            updateBidsChart(data.bids)
            updateAsksChart(data.asks)
            
            document.getElementById('content').style.display = 'block';
            document.getElementById('loadingSpinner').style.display = 'none'
        }

        window.addEventListener("beforeunload", () => {
            socket.close()
        });
    }

    if (document.readyState === "complete") {
        onLoad()
    } else {
        document.addEventListener("DOMContentLoaded", onLoad, false);
    }

    function updateBidsChart(ws_data_bids) {
        bidsData = []

        // create cumulative data array
        __bidsCumulativeData = []
        for (let i = 0; i < ws_data_bids.length; i++) {
            __bidsCumulativeData.push(ws_data_bids[i].amount)
        }
        let cum_data_array = prefixSum(__bidsCumulativeData)

        // final data array
        for (let i = 0; i < ws_data_bids.length; i++) {
            bidsData.push({
                idx: ws_data_bids[i].price,
                orders: cum_data_array[i],
            })
        }

        // reverse data for bids
        bidsData = _.reverse(bidsData)

        bidsData.forEach(function (d) {
            d.orders = +d.orders
        })

        // Scale the range of the data in the domains
        bidsX.domain(bidsData.map(function (d) {
            return d.idx
        }))
        bidsY.domain([0, d3.max(bidsData, function (d) {
            return d.orders
        })])

        bidsSvg.selectAll('.bar-bids').remove('rect')

        // append the rectangles for the bar chart
        bidsSvg.selectAll('.bar')
            .data(bidsData)
            .enter().append('rect')
            .attr('class', 'bar bar-bids')
            .attr('x', function (d) {
                return bidsX(d.idx)
            })
            .attr('width', bidsX.bandwidth())
            .attr('y', function (d) {
                return bidsY(d.orders)
            })
            .attr('height', function (d) {
                return bidsHeight - bidsY(d.orders)
            })
    }

    function updateAsksChart(ws_data_asks) {
        asksData = []
        __asksCumulativeData = []

        for (let i = 0; i < ws_data_asks.length; i++) {
            __asksCumulativeData.push(ws_data_asks[i].amount)
        }

        // create cumulative data array
        let cum_data_array = prefixSum(__asksCumulativeData)

        // final data array
        for (let i = 0; i < ws_data_asks.length; i++) {
            asksData.push({
                idx: ws_data_asks[i].price,
                orders: cum_data_array[i],
            })
        }

        asksData.forEach(function (d) {
            d.orders = +d.orders
        })

        // Scale the range of the data in the domains
        asksX.domain(asksData.map(function (d) {
            return d.idx
        }))
        asksY.domain([0, d3.max(asksData, function (d) {
            return d.orders
        })])

        asksSvg.selectAll('.bar-asks').remove('rect')

        // append the rectangles for the bar chart
        asksSvg.selectAll('.bar')
            .data(asksData)
            .enter().append('rect')
            .attr('class', 'bar bar-asks')
            .attr('x', function (d) {
                return asksX(d.idx);
            })
            .attr('width', asksX.bandwidth())
            .attr('y', function (d) {
                return asksY(d.orders)
            })
            .attr('height', function (d) {
                return asksHeight - asksY(d.orders)
            })

    }
</script>

</body>
    "#.replace("{top_bids_and_asks_count}", &config.app.top_bids_and_asks_count.to_string());

    HttpResponse::Ok().content_type("text/html").body(s)
}
