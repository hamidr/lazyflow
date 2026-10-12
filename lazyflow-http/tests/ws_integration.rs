use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::extract::WebSocketUpgrade;
use axum::extract::ws::{Message, WebSocket};
use axum::routing::get;
use lazyflow_http::ws::{self, WsMessage};

async fn start_server(app: Router) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_basic_receive() {
    let app = Router::new().route(
        "/ws",
        get(|upgrade: WebSocketUpgrade| async {
            upgrade.on_upgrade(|mut socket: WebSocket| async move {
                for i in 0..5 {
                    let _ = socket.send(Message::Text(format!("msg-{i}").into())).await;
                }
                let _ = socket.send(Message::Close(None)).await;
            })
        }),
    );
    let (addr, _server) = start_server(app).await;

    let (incoming, _sender) = ws::connect(format!("ws://{addr}/ws"));
    let messages: Vec<WsMessage> = incoming
        .filter(|m| m.is_text())
        .take(5)
        .collect()
        .await
        .unwrap();

    assert_eq!(messages.len(), 5);
    for (i, msg) in messages.iter().enumerate() {
        assert_eq!(msg, &WsMessage::Text(format!("msg-{i}")));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_bidirectional_echo() {
    let app = Router::new().route(
        "/ws",
        get(|upgrade: WebSocketUpgrade| async {
            upgrade.on_upgrade(|mut socket: WebSocket| async move {
                while let Some(Ok(msg)) = socket.recv().await {
                    match msg {
                        Message::Text(t) => {
                            let echo = format!("echo:{t}");
                            if socket.send(Message::Text(echo.into())).await.is_err() {
                                break;
                            }
                        }
                        Message::Close(_) => break,
                        _ => {}
                    }
                }
            })
        }),
    );
    let (addr, _server) = start_server(app).await;

    let (incoming, sender) = ws::connect(format!("ws://{addr}/ws"));

    let send_handle = tokio::spawn(async move {
        for i in 0..3 {
            sender.send_text(format!("hello-{i}")).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The connection ends with the incoming pipe; once take(3) is
        // satisfied there may be nothing left to close.
        let _ = sender.close().await;
    });

    let replies: Vec<String> = incoming
        .and_then(|m| m.text())
        .take(3)
        .collect()
        .await
        .unwrap();

    send_handle.await.unwrap();
    assert_eq!(
        replies,
        vec!["echo:hello-0", "echo:hello-1", "echo:hello-2"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_binary_messages() {
    let app = Router::new().route(
        "/ws",
        get(|upgrade: WebSocketUpgrade| async {
            upgrade.on_upgrade(|mut socket: WebSocket| async move {
                let _ = socket.send(Message::Binary(vec![1, 2, 3].into())).await;
                let _ = socket.send(Message::Binary(vec![4, 5].into())).await;
                let _ = socket.send(Message::Close(None)).await;
            })
        }),
    );
    let (addr, _server) = start_server(app).await;

    let (incoming, _sender) = ws::connect(format!("ws://{addr}/ws"));
    let messages: Vec<Vec<u8>> = incoming.and_then(|m| m.bytes()).collect().await.unwrap();

    assert_eq!(messages, vec![vec![1, 2, 3], vec![4, 5]]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_close_frame_received() {
    let app = Router::new().route(
        "/ws",
        get(|upgrade: WebSocketUpgrade| async {
            upgrade.on_upgrade(|mut socket: WebSocket| async move {
                let _ = socket.send(Message::Text("before-close".into())).await;
                let _ = socket.send(Message::Close(None)).await;
            })
        }),
    );
    let (addr, _server) = start_server(app).await;

    let (incoming, _sender) = ws::connect(format!("ws://{addr}/ws"));
    let messages: Vec<WsMessage> = incoming.collect().await.unwrap();

    assert!(messages.iter().any(|m| m.is_text()));
    assert!(messages.iter().any(|m| m.is_close()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_pipe_operators() {
    let app = Router::new().route(
        "/ws",
        get(|upgrade: WebSocketUpgrade| async {
            upgrade.on_upgrade(|mut socket: WebSocket| async move {
                for i in 0..10 {
                    let _ = socket.send(Message::Text(format!("{i}").into())).await;
                }
                let _ = socket.send(Message::Close(None)).await;
            })
        }),
    );
    let (addr, _server) = start_server(app).await;

    let (incoming, _sender) = ws::connect(format!("ws://{addr}/ws"));
    let result: Vec<i64> = incoming
        .and_then(|m| m.text())
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|x| x % 2 == 0)
        .collect()
        .await
        .unwrap();

    assert_eq!(result, vec![0, 2, 4, 6, 8]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_cancellation_via_take() {
    let app = Router::new().route(
        "/ws",
        get(|upgrade: WebSocketUpgrade| async {
            upgrade.on_upgrade(|mut socket: WebSocket| async move {
                for i in 0..10000 {
                    if socket
                        .send(Message::Text(format!("{i}").into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            })
        }),
    );
    let (addr, _server) = start_server(app).await;

    let (incoming, _sender) = ws::connect(format!("ws://{addr}/ws"));
    let messages: Vec<String> = incoming
        .and_then(|m| m.text())
        .take(3)
        .collect()
        .await
        .unwrap();

    assert_eq!(messages.len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_connect_failure() {
    let (incoming, _sender) = ws::connect("ws://127.0.0.1:1/nonexistent");
    let result = incoming.collect().await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_sender_detects_closed_connection() {
    let app = Router::new().route(
        "/ws",
        get(|upgrade: WebSocketUpgrade| async {
            upgrade.on_upgrade(|mut socket: WebSocket| async move {
                let _ = socket.send(Message::Close(None)).await;
            })
        }),
    );
    let (addr, _server) = start_server(app).await;

    let (incoming, sender) = ws::connect(format!("ws://{addr}/ws"));

    // Drain incoming to completion
    let _ = incoming.collect().await;

    // Give writer task time to notice the closure
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Sender should fail after connection is gone
    let result = sender.send_text("should fail").await;
    assert!(result.is_err());
}

/// Sends one message, then reports whether the client shut the connection
/// down within a second (close frame, error, or end of stream).
async fn hello_then_watch_for_shutdown() -> (SocketAddr, tokio::sync::oneshot::Receiver<bool>) {
    let (shut_tx, shut_rx) = tokio::sync::oneshot::channel();
    let shut_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(shut_tx)));
    let app = Router::new().route(
        "/ws",
        get(move |upgrade: WebSocketUpgrade| async move {
            upgrade.on_upgrade(move |mut socket: WebSocket| async move {
                let _ = socket.send(Message::Text("hello".into())).await;
                let watch = async {
                    loop {
                        match socket.recv().await {
                            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return,
                            Some(Ok(_)) => continue,
                        }
                    }
                };
                let shut = tokio::time::timeout(Duration::from_secs(1), watch)
                    .await
                    .is_ok();
                if let Some(tx) = shut_tx.lock().unwrap().take() {
                    let _ = tx.send(shut);
                }
            })
        }),
    );
    let (addr, _server) = start_server(app).await;
    (addr, shut_rx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_connection_shuts_down_when_first_returns() {
    let (addr, shut) = hello_then_watch_for_shutdown().await;
    let (incoming, sender) = ws::connect(format!("ws://{addr}/ws"));
    assert!(incoming.first().await.unwrap().is_some());
    assert!(shut.await.unwrap(), "the server still had a live peer");
    assert!(sender.send_text("after-drop").await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_connection_shuts_down_when_take_is_satisfied() {
    let (addr, shut) = hello_then_watch_for_shutdown().await;
    let (incoming, sender) = ws::connect(format!("ws://{addr}/ws"));
    assert_eq!(incoming.take(1).collect().await.unwrap().len(), 1);
    assert!(shut.await.unwrap(), "the server still had a live peer");
    assert!(sender.send_text("after-drop").await.is_err());
}
