use std::fmt::Debug;

use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use native_tls::TlsConnector as NativeTlsConnector;
use tokio::net::TcpStream;
use tokio_native_tls::TlsConnector;
use tokio_tungstenite::{client_async_with_config, tungstenite::protocol::Message};
use tracing::info;
use url::Url;

use async_trait::async_trait;

use crate::ws_data_providers::ws_async_client_factory::{
    WSAsyncClient, WsClient, WsSink, WsStream,
};
use crate::ws_data_providers::{SubscribeData, SubscribeMessage};

#[derive(Debug, Clone)]
pub(super) struct BitstampWsClient {
    pub address: String,
    pub event: String,
    pub channel: String,
}

impl WSAsyncClient<WsSink, WsStream> for BitstampWsClient {
    fn get_ws_client(&self) -> Result<Box<dyn WsClient<WsSink, WsStream>>> {
        Ok(Box::new(self.clone()))
    }
}

#[async_trait]
impl WsClient<WsSink, WsStream> for BitstampWsClient {
    async fn get_sink_and_stream(&self) -> Result<(WsSink, WsStream)> {
        // connection setup and handshake code
        let url = Url::parse(&self.address)?;
        let domain = url
            .domain()
            .ok_or_else(|| anyhow::anyhow!("Domain not found in URL {}", url))?
            .to_string();
        let socket = TcpStream::connect((domain.as_str(), url.port().unwrap_or(443)))
            .await
            .context("Failed to connect to bitstamp host")?;
        let connector = NativeTlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .context("Failed to create TLS connector for bitstamp host")?;
        let stream = TlsConnector::from(connector)
            .connect(domain.as_str(), socket)
            .await
            .context("Failed TLS handshake for bitstamp")?;

        info!("Connecting to {}", &self.address);

        let (ws_stream, _) = client_async_with_config(url, stream, None)
            .await
            .context("Failed to complete websocket handshake for host bitstamp")?;

        info!("WebSocket handshake has been successfully completed for bitstamp");
        let (mut write, read) = ws_stream.split();
        let subscribe_message = SubscribeMessage {
            event: self.event.clone(),
            data: SubscribeData {
                channel: self.channel.clone(),
            },
        };
        let subscribe_message_json = serde_json::to_string(&subscribe_message)?;
        write.send(Message::Text(subscribe_message_json)).await?;

        Ok((write, read))
    }
}
