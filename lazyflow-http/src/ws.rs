//! WebSocket source and sink.
//!
//! Connects to a WebSocket endpoint and returns a `Pipe<WsMessage>` for
//! incoming messages plus a cloneable [`WsSender`] for outgoing messages.
//!
//! The connection is established lazily on first pull. The pipe is
//! single-use (same as `Pipe::generate_once`).
//!
//! No automatic reconnection -- WebSocket has no built-in resume
//! protocol (unlike SSE's Last-Event-ID). Use pipe's `retry()` or
//! `handle_error_with()` for application-level reconnection.
//!
//! ```ignore
//! use lazyflow_http::ws;
//!
//! let (incoming, sender) = ws::connect("wss://example.com/ws");
//!
//! // Send messages from any task
//! let s = sender.clone();
//! tokio::spawn(async move { s.send_text("hello").await });
//!
//! // Process incoming as a pipe
//! incoming
//!     .filter(|m| m.is_text())
//!     .and_then(|m| m.text())
//!     .for_each(|text| println!("{text}"))
//!     .await?;
//! ```

use std::sync::Arc;

use lazyflow::pipeline::Pipe;
use lazyflow::pull::PipeError;

/// Maximum incoming message size: 16 MiB. Prevents OOM from oversized frames.
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// A WebSocket message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsMessage {
    Text(String),
    Binary(Vec<u8>),
    Close,
}

impl WsMessage {
    /// Returns true if this is a text message.
    pub fn is_text(&self) -> bool {
        matches!(self, WsMessage::Text(_))
    }

    /// Returns true if this is a binary message.
    pub fn is_binary(&self) -> bool {
        matches!(self, WsMessage::Binary(_))
    }

    /// Returns true if this is a close frame.
    pub fn is_close(&self) -> bool {
        matches!(self, WsMessage::Close)
    }

    /// Extract text payload. Returns None for non-text messages.
    pub fn text(self) -> Option<String> {
        match self {
            WsMessage::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Extract binary payload. Returns None for non-binary messages.
    pub fn bytes(self) -> Option<Vec<u8>> {
        match self {
            WsMessage::Binary(b) => Some(b),
            _ => None,
        }
    }

    /// Extract payload as bytes regardless of text/binary framing.
    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            WsMessage::Binary(b) => b,
            WsMessage::Text(s) => s.into_bytes(),
            WsMessage::Close => Vec::new(),
        }
    }
}

/// Cloneable handle for sending WebSocket messages.
///
/// Sends are delivered via an internal channel to the write task,
/// decoupling user sends from protocol-level pong responses.
/// The connection lives as long as the incoming pipe: once that pipe
/// ends or is dropped, the socket closes and sends return
/// `PipeError::Closed`.
#[derive(Clone)]
pub struct WsSender {
    tx: tokio::sync::mpsc::Sender<OutgoingMsg>,
}

enum OutgoingMsg {
    Text(String),
    Binary(Vec<u8>),
    Close,
}

impl WsSender {
    /// Send a text message.
    pub async fn send_text(&self, text: impl Into<String>) -> Result<(), PipeError> {
        self.tx
            .send(OutgoingMsg::Text(text.into()))
            .await
            .map_err(|_| PipeError::Closed)
    }

    /// Send a binary message.
    pub async fn send_binary(&self, data: Vec<u8>) -> Result<(), PipeError> {
        self.tx
            .send(OutgoingMsg::Binary(data))
            .await
            .map_err(|_| PipeError::Closed)
    }

    /// Send a close frame, initiating graceful shutdown.
    pub async fn close(&self) -> Result<(), PipeError> {
        self.tx
            .send(OutgoingMsg::Close)
            .await
            .map_err(|_| PipeError::Closed)
    }
}

/// Connect to a WebSocket endpoint.
///
/// Returns a `(Pipe<WsMessage>, WsSender)` pair. The pipe streams
/// incoming messages; the sender allows sending outgoing messages
/// from any task. Ping/pong frames are handled automatically.
///
/// The connection is established lazily on first pull of the pipe.
/// If the pipe is never materialized, no connection is made.
///
/// Incoming messages larger than 16 MiB are rejected with an error.
pub fn connect(url: impl Into<String>) -> (Pipe<WsMessage>, WsSender) {
    let url = url.into();
    let (outgoing_tx, outgoing_rx) = tokio::sync::mpsc::channel::<OutgoingMsg>(64);
    let sender = WsSender { tx: outgoing_tx };

    let incoming = Pipe::generate_once(move |tx| async move {
        let mut ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
        ws_config.max_message_size = Some(MAX_MESSAGE_SIZE);
        ws_config.max_frame_size = Some(MAX_MESSAGE_SIZE);

        let (stream, _response) =
            tokio_tungstenite::connect_async_with_config(&url, Some(ws_config), false)
                .await
                .map_err(|e| PipeError::Custom(Box::new(e)))?;

        let (write_half, mut read_half) = futures_util::StreamExt::split(stream);
        let write_half = Arc::new(tokio::sync::Mutex::new(write_half));

        // Writer task: drains both user sends and pong responses. Held in a
        // JoinSet, which aborts it on drop, so the writer and its half of
        // the socket go away however the read loop ends, cancellation
        // included.
        let writer = Arc::clone(&write_half);
        let mut outgoing_rx = outgoing_rx;
        let mut writer_task = tokio::task::JoinSet::new();
        writer_task.spawn(async move {
            use futures_util::SinkExt;
            while let Some(msg) = outgoing_rx.recv().await {
                let ws_msg = match msg {
                    OutgoingMsg::Text(s) => tokio_tungstenite::tungstenite::Message::text(s),
                    OutgoingMsg::Binary(b) => tokio_tungstenite::tungstenite::Message::binary(b),
                    OutgoingMsg::Close => {
                        let _ = writer
                            .lock()
                            .await
                            .send(tokio_tungstenite::tungstenite::Message::Close(None))
                            .await;
                        return;
                    }
                };
                if writer.lock().await.send(ws_msg).await.is_err() {
                    return;
                }
            }
        });

        // Read loop
        let pong_writer = Arc::clone(&write_half);
        let result = async {
            use futures_util::StreamExt;
            while let Some(result) = read_half.next().await {
                match result {
                    Ok(msg) => {
                        let ws_msg = match msg {
                            tokio_tungstenite::tungstenite::Message::Text(s) => {
                                WsMessage::Text(s.to_string())
                            }
                            tokio_tungstenite::tungstenite::Message::Binary(b) => {
                                WsMessage::Binary(b.to_vec())
                            }
                            tokio_tungstenite::tungstenite::Message::Ping(data) => {
                                use futures_util::SinkExt;
                                let pong = tokio_tungstenite::tungstenite::Message::Pong(data);
                                let _ = pong_writer.lock().await.send(pong).await;
                                continue;
                            }
                            tokio_tungstenite::tungstenite::Message::Pong(_) => continue,
                            tokio_tungstenite::tungstenite::Message::Close(_) => {
                                tx.emit(WsMessage::Close).await?;
                                return Ok(());
                            }
                            tokio_tungstenite::tungstenite::Message::Frame(_) => continue,
                        };
                        tx.emit(ws_msg).await?;
                    }
                    Err(e) => {
                        return Err(PipeError::Custom(Box::new(e)));
                    }
                }
            }
            Ok(())
        }
        .await;

        drop(writer_task);
        result
    });

    (incoming, sender)
}
