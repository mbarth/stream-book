use std::fmt::Debug;

use anyhow::{Context, Result};
use futures::StreamExt;
use native_tls::TlsConnector as NativeTlsConnector;
use tokio::net::TcpStream;
use tokio_native_tls::TlsConnector;
use tokio_tungstenite::client_async_with_config;
use tracing::info;
use url::Url;

use async_trait::async_trait;

use crate::ws_data_providers::ws_async_client_factory::{
    WsAsyncClient, WsClient, WsSink, WsStream,
};

#[derive(Debug, Clone)]
pub(super) struct BinanceWsClient {
    pub address: String,
}

impl WsClient<WsSink, WsStream> for BinanceWsClient {
    fn get_ws_async_client(&self) -> Result<Box<dyn WsAsyncClient<WsSink, WsStream>>> {
        Ok(Box::new(self.clone()))
    }
}

#[async_trait]
impl WsAsyncClient<WsSink, WsStream> for BinanceWsClient {
    async fn get_sink_and_stream(&self) -> Result<(WsSink, WsStream)> {
        // connection setup and handshake code
        let url = Url::parse(&self.address)?;
        let domain = url
            .domain()
            .ok_or_else(|| anyhow::anyhow!("Domain not found in URL {}", url))?
            .to_string();
        let socket = TcpStream::connect((domain.as_str(), url.port().unwrap_or(443)))
            .await
            .context("Failed to connect to binance host")?;
        let connector = NativeTlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .context("Failed to create TLS connector for binance host")?;
        let stream = TlsConnector::from(connector)
            .connect(domain.as_str(), socket)
            .await
            .context("Failed TLS handshake for binance")?;

        info!("Connecting to {}", &self.address);

        let (ws_stream, _) = client_async_with_config(url, stream, None)
            .await
            .context("Failed to complete websocket handshake for host binance")?;

        info!("WebSocket handshake has been successfully completed for binance");
        Ok(ws_stream.split())
    }
}
