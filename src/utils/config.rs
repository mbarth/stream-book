use std::env;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tracing::warn;

/// Top-level configuration.
#[derive(Clone, Debug, Deserialize, Default)]
pub struct Config {
    pub app: AppConfig,
    pub deploy: DeployConfig,
    pub exchanges: ExchangesConfig,

    /// Logging configuration
    ///
    /// This is passed to [with_env_filter].
    /// If not set, uses the value of RUST_LOG.
    /// If RUST_LOG is not set, defaults to INFO.
    ///
    /// [with_env_filter]: https://docs.rs/tracing-subscriber/0.2.15/tracing_subscriber/fmt/struct.SubscriberBuilder.html#method.with_env_filter
    pub rust_log: Option<String>,
}

/// Application configuration.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct AppConfig {
    pub ip: std::net::Ipv4Addr,
    pub websocket_port: u16,
    pub api_port: u16,
    pub top_bids_and_asks_count: usize,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            ip: std::net::Ipv4Addr::new(0, 0, 0, 0),
            websocket_port: 50051,
            api_port: 8080,
            top_bids_and_asks_count: 10,
        }
    }
}

/// Deploy environment configuration.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct DeployConfig {
    pub env: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExchangesConfig {
    pub first_exchange: FirstExchangeConfig,
    pub second_exchange: SecondExchangeConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FirstExchangeConfig {
    pub websocket: WebsocketConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SecondExchangeConfig {
    pub websocket: WebsocketConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WebsocketConfig {
    pub address: String,
    pub event: Option<String>,
    pub channel: Option<String>,
}

impl Config {
    /// Load configuration values from config folder.
    ///
    /// Use the `config` directory by default unless the `STREAMBOOK__CONFIG__PREFIX` environment
    /// variable is set. You can override individual default configuration by setting environment variables
    /// using the `STREAMBOOK__` environment variable prefix.
    ///
    pub fn new() -> anyhow::Result<Self> {
        // path prefix indicated by STREAMBOOK_CONFIG_PREFIX setting, otherwise, fallback to config dir
        let prefix = PathBuf::from(
            env::var("STREAMBOOK__CONFIG__PREFIX").unwrap_or_else(|_| "config".to_string()),
        );
        // use any env variables if defined
        let env_source = config::Environment::with_prefix("STREAMBOOK").separator("__");
        let mut cfg_builder = config::Config::builder()
            .set_default("deploy.env", "local")?
            // grab default settings
            .add_source(config::File::from(prefix.join("default")))
            .add_source(env_source);
        // grab specific environment settings, comes from terraform env specific config tfvars files
        if let Ok(env_name) = env::var("STREAMBOOK__DEPLOY__ENV") {
            cfg_builder = cfg_builder.set_override("deploy.env", env_name.clone())?;
            let env_cfg_file = config::File::from(prefix.join(&env_name));
            match config::Source::collect(&env_cfg_file) {
                Ok(_) => {
                    cfg_builder = cfg_builder.add_source(env_cfg_file);
                }
                Err(err) => {
                    warn!("Skipping {} config file because: {}", &env_name, err);
                }
            }
        } else {
            // default to local if no environment settings
            cfg_builder =
                cfg_builder.add_source(config::File::from(prefix.join("local")).required(false));
        }
        let config: config::Config = cfg_builder.build()?;
        // convert to app config
        Ok(config.try_deserialize::<Config>()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_config_defaults() {
        let config = AppConfig::default();

        assert_eq!(config.ip, std::net::Ipv4Addr::new(0, 0, 0, 0));
        assert_eq!(config.websocket_port, 50051);
        assert_eq!(config.api_port, 8080);
        assert_eq!(config.top_bids_and_asks_count, 10);
    }
}
