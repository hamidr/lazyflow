//! Server-Sent Events (SSE) source.
//!
//! Connects to an HTTP endpoint streaming `text/event-stream` and
//! returns a `Pipe<SseEvent>`. Supports auto-reconnection with
//! `Last-Event-ID` resume per the W3C EventSource specification.
//!
//! The returned pipe is single-use -- cloning and materializing both
//! clones returns an error (same as `Pipe::generate_once`).
//!
//! ```ignore
//! use lazyflow_http::sse;
//!
//! sse::connect("https://example.com/events")
//!     .filter(|e| e.event.as_deref() == Some("update"))
//!     .for_each(|e| println!("{}", e.data))
//!     .await?;
//! ```

use std::time::Duration;

use lazyflow::pipeline::Pipe;
use lazyflow::pull::PipeError;

mod parser;

use parser::EventStreamParser;

/// A parsed SSE event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    /// Event type (from `event:` field). None means default "message".
    pub event: Option<String>,
    /// Event payload (from `data:` field(s), joined by newlines).
    pub data: String,
    /// Last event ID (from `id:` field). Used for reconnection.
    pub id: Option<String>,
    /// Server-suggested retry interval (from `retry:` field).
    pub retry: Option<Duration>,
}

/// Configuration for an SSE connection.
#[derive(Debug, Clone)]
pub struct SseConfig {
    /// Target URL.
    pub url: String,
    /// Additional HTTP headers.
    pub headers: Vec<(String, String)>,
    /// Auto-reconnect on disconnect or 5xx errors. Default: true.
    /// Does not reconnect on 4xx client errors.
    pub reconnect: bool,
    /// Maximum reconnection delay (exponential backoff cap). Default: 30s.
    pub max_reconnect_delay: Duration,
    /// Initial reconnection delay. Default: 1s.
    pub initial_reconnect_delay: Duration,
    /// Resume from this event ID on first connect.
    pub last_event_id: Option<String>,
    /// Most bytes an event may hold while it is read: its data, type and
    /// ID plus the line in progress. A server exceeding it ends the pipe
    /// with an error, without reconnecting. Default: 16 MiB.
    pub max_event_size: usize,
}

impl Default for SseConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            headers: Vec::new(),
            reconnect: true,
            max_reconnect_delay: Duration::from_secs(30),
            initial_reconnect_delay: Duration::from_secs(1),
            last_event_id: None,
            max_event_size: 16 * 1024 * 1024,
        }
    }
}

/// Connect to an SSE endpoint with default settings.
///
/// Returns a `Pipe<SseEvent>` that streams parsed events.
/// Auto-reconnects on disconnect or 5xx errors with exponential backoff.
pub fn connect(url: impl Into<String>) -> Pipe<SseEvent> {
    connect_with(SseConfig {
        url: url.into(),
        ..Default::default()
    })
}

/// Connect to an SSE endpoint with custom configuration.
pub fn connect_with(config: SseConfig) -> Pipe<SseEvent> {
    Pipe::generate_once(move |tx| async move {
        let client = reqwest::Client::new();
        let mut parser =
            EventStreamParser::new(config.last_event_id.clone(), config.max_event_size);
        let mut reconnect_delay = config.initial_reconnect_delay;
        let mut first_connect = true;

        loop {
            if !first_connect {
                if !config.reconnect {
                    return Ok(());
                }
                tokio::time::sleep(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(config.max_reconnect_delay);
            }
            first_connect = false;

            let mut req = client
                .get(&config.url)
                .header("Accept", "text/event-stream")
                .header("Cache-Control", "no-cache");

            if let Some(id) = parser.last_event_id() {
                req = req.header("Last-Event-ID", id);
            }

            for (key, value) in &config.headers {
                req = req.header(key.as_str(), value.as_str());
            }

            let response = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    if config.reconnect {
                        continue;
                    }
                    return Err(PipeError::Custom(Box::new(e)));
                }
            };

            let status = response.status();
            if !status.is_success() {
                // 4xx: client error, reconnecting won't help
                if status.is_client_error() {
                    return Err(PipeError::Custom(
                        format!("SSE server returned {status}").into(),
                    ));
                }
                // 5xx: server error, retry if configured
                if config.reconnect {
                    continue;
                }
                return Err(PipeError::Custom(
                    format!("SSE server returned {status}").into(),
                ));
            }

            // Reset backoff on successful connection
            reconnect_delay = config.initial_reconnect_delay;

            parser.restart();
            let result =
                read_event_stream(&tx, response, &mut parser, &mut reconnect_delay, &config).await;
            match result {
                StreamEnd::Disconnected(_) if config.reconnect => continue,
                StreamEnd::Disconnected(e) | StreamEnd::Fatal(e) => return Err(e),
            }
        }
    })
}

/// Why reading one response's event stream stopped.
enum StreamEnd {
    /// The connection failed or closed; reconnecting may resume it.
    Disconnected(PipeError),
    /// Reconnecting would not help: the server sent an oversized event,
    /// or the consumer is gone.
    Fatal(PipeError),
}

/// Read and parse the SSE event stream from an HTTP response.
async fn read_event_stream(
    tx: &lazyflow::pipeline::Emitter<SseEvent>,
    response: reqwest::Response,
    parser: &mut EventStreamParser,
    reconnect_delay: &mut Duration,
    config: &SseConfig,
) -> StreamEnd {
    use futures_core::Stream;
    use std::pin::Pin;

    let mut byte_stream = response.bytes_stream();

    loop {
        let chunk = {
            use std::future::poll_fn;
            poll_fn(|cx| Pin::new(&mut byte_stream).poll_next(cx)).await
        };
        let bytes = match chunk {
            Some(Ok(bytes)) => bytes,
            Some(Err(e)) => return StreamEnd::Disconnected(PipeError::Custom(Box::new(e))),
            None => return StreamEnd::Disconnected(PipeError::Closed),
        };

        let events = match parser.feed(&bytes) {
            Ok(events) => events,
            Err(e) => return StreamEnd::Fatal(e),
        };
        if let Some(retry) = parser.take_retry() {
            *reconnect_delay = retry.min(config.max_reconnect_delay);
        }
        for event in events {
            if let Err(e) = tx.emit(event).await {
                return StreamEnd::Fatal(e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = SseConfig::default();
        assert!(config.reconnect);
        assert_eq!(config.max_reconnect_delay, Duration::from_secs(30));
        assert_eq!(config.initial_reconnect_delay, Duration::from_secs(1));
        assert!(config.last_event_id.is_none());
        assert_eq!(config.max_event_size, 16 * 1024 * 1024);
    }
}
