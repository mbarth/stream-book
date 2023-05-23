//! Provides a function to create a new tracing subscriber.
use tracing_subscriber::EnvFilter;

use crate::utils::config::Config;

/// Creates and returns a new tracing subscriber.
///
/// The subscriber is configured with the following parameters:
///
/// * The log level is determined by the `RUST_LOG` environment variable, falling back to the `default_level` argument if it is not set.
/// * The subscriber logs span events when they are entered and exited.
/// * The subscriber logs the thread ID and name for each event.
///
/// # Arguments
///
/// * `config` - A `Config` object that holds configuration options for the tracing subscriber.
/// * `default_level` - The default log level to use if the `RUST_LOG` environment variable is not set.
///
/// # Returns
///
/// Returns a `SubscriberBuilder` that can be used to create a `Subscriber`.
pub fn new_tracing_subscriber(
    config: Config,
    default_level: &str,
) -> tracing_subscriber::fmt::SubscriberBuilder<
    tracing_subscriber::fmt::format::DefaultFields,
    tracing_subscriber::fmt::format::Format,
    EnvFilter,
> {
    let binding = std::env::var("RUST_LOG").ok();
    let rust_log = binding.as_ref();
    let env_filter: EnvFilter = config
        .rust_log
        .as_ref()
        .or(rust_log)
        .cloned()
        .unwrap_or_else(|| default_level.to_string())
        .into();

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_level(true)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::ACTIVE)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
}

#[macro_export]
macro_rules! emit_error {
    ($level:expr, $kind:expr, $reason:expr) => {
        {
            let target = module_path!();
            let source = format!("{}:{}", file!(), line!());
            let thread = std::thread::current();
            let thread_id = format!("{:?}", thread.id());
            let thread_name = thread.name().unwrap_or("null");

            tracing::event!($level,
                            ?target,
                            ?source,
                            kind = $kind,
                            level = %$level,
                            thread.id = %thread_id,
                            thread.name = %thread_name,
                            "{}", $reason);
        }
    };
}
