use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use actix_files::Files;
use actix_web::{middleware, web, App, HttpServer};
use anyhow::Context;
use futures::future::Future;
use to_unit::ToUnit;
use tokio::sync::broadcast;
use tonic::transport::{Error, Server};
use tracing::info;

use grpc_routes::grpc_impl::{
    order_book_proto::orderbook_aggregator_server::OrderbookAggregatorServer, StreamBookAggregator,
};

use crate::utils::{config::Config, telemetry::new_tracing_subscriber};
use crate::web_routes::web_impl::{favicon, health, index, ws};
use crate::ws_data_providers::ws_listeners::start_ws_listeners;

mod grpc_routes;
mod order_book;
mod utils;
mod web_routes;
mod ws_data_providers;

static FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../proto/orderbook.bin");

/// The entry point of the application.
///
/// It sets up the application configuration, starts the Binance and Bitstamp WebSocket listeners,
/// the Actix HTTP and the Tonic gRPC servers, and then runs them all.
///
/// It uses the Tokio async runtime.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var("RUST_BACKTRACE", "full");

    // Load the application configuration
    let config = Config::new().context("Failed to load configuration")?;
    // Setup telemetry (logging)
    new_tracing_subscriber(config.clone(), "info").init();

    info!("Starting up");

    // Create a broadcast channel (100 capacity) for sending and receiving snapshot messages
    let (snapshot_sender, snapshot_receiver) = broadcast::channel(100);

    // Start the WebSocket (Binance and Bitstamp) listeners
    let ws_listeners_handle = start_ws_listeners(snapshot_sender, &config).await?;

    info!(
        "Starting HTTP server at http://{}:{}",
        config.app.ip, config.app.api_port
    );

    // Building Actix Web server
    let cloned_config = config.clone();
    // Wrap the receiver in an Arc<Mutex<>>
    let locked_snapshot_receiver =
        Arc::new(tokio::sync::Mutex::new(snapshot_receiver.resubscribe()));
    let actix_future = HttpServer::new(move || {
        // Clone the Arc
        let cloned_snapshot_receiver = Arc::clone(&locked_snapshot_receiver);
        App::new()
            .app_data(web::Data::new(cloned_snapshot_receiver))
            .app_data(web::Data::new(cloned_config.clone()))
            .wrap(middleware::Compress::default())
            .wrap(middleware::Logger::default())
            .service(favicon)
            .service(health)
            .service(index)
            .service(ws)
            .service(Files::new("/static", "static").show_files_listing())
    })
    .bind((config.app.ip, config.app.api_port))?
    .workers(1)
    .run();

    // Building Tonic gRPC server with an order book summary handler
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build()?;

    let tonic_future = {
        let order_book_service = StreamBookAggregator { snapshot_receiver };
        let addr = SocketAddr::new(config.app.ip.into(), config.app.websocket_port);
        Server::builder()
            .add_service(OrderbookAggregatorServer::new(order_book_service))
            .add_service(reflection_service)
            .serve(addr)
    };
    // Running Actix server in background. Actix-web handles a graceful shutdown.
    // NOTE: Clippy is very pedantic about this "non-binding `let` on a future", but we're doing it
    // so we can run both the actix-web and the Tonic gRPC servers in parallel.
    #[allow(clippy::let_underscore_future)]
    let _ = tokio::task::spawn(actix_future);
    // Run Tonic server with a ctrl-c handler
    tonic_runner(tonic_future).await?;
    ws_listeners_handle.await?;

    Ok(())
}

/// Run tonic gRPC server with a ctrl-c handler
async fn tonic_runner(
    tonic_future: impl Future<Output = Result<(), Error>>,
) -> Result<(), tonic::transport::Error> {
    let (tonic, abort_handle) = futures::future::abortable(tonic_future);

    let ctrl_c_future = async move {
        tokio::signal::ctrl_c().await.to_unit();
        abort_handle.abort();
    };

    let (tonic_result, _) = futures::future::join(tonic, ctrl_c_future).await;
    match tonic_result {
        Ok(Err(e)) => Err(e),
        _ => Ok(()),
    }
}
