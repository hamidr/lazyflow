use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Router;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::get;
use futures_core::Stream;
use lazyflow_http::sse::{self, SseConfig, SseEvent};

async fn start_server(app: Router) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, handle)
}

struct CountingStream {
    current: usize,
    max: usize,
}

impl Stream for CountingStream {
    type Item = Result<Event, std::convert::Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.current >= self.max {
            return Poll::Ready(None);
        }
        let i = self.current;
        self.current += 1;
        let event = Event::default()
            .event("count")
            .data(format!("{i}"))
            .id(format!("id-{i}"));
        Poll::Ready(Some(Ok(event)))
    }
}

fn counting_stream(n: usize) -> CountingStream {
    CountingStream { current: 0, max: n }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn basic_sse_stream() {
    let app = Router::new().route("/events", get(|| async { Sse::new(counting_stream(5)) }));
    let (addr, _server) = start_server(app).await;

    let events: Vec<SseEvent> = sse::connect(format!("http://{addr}/events"))
        .take(5)
        .collect()
        .await
        .unwrap();

    assert_eq!(events.len(), 5);
    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.event.as_deref(), Some("count"));
        assert_eq!(event.data, format!("{i}"));
        assert_eq!(event.id.as_deref(), Some(&format!("id-{i}") as &str));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_with_pipe_operators() {
    let app = Router::new().route("/events", get(|| async { Sse::new(counting_stream(10)) }));
    let (addr, _server) = start_server(app).await;

    let result: Vec<i64> = sse::connect(format!("http://{addr}/events"))
        .take(10)
        .map(|e| e.data.parse::<i64>().unwrap_or(-1))
        .filter(|x| *x % 2 == 0)
        .collect()
        .await
        .unwrap();

    assert_eq!(result, vec![0, 2, 4, 6, 8]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_cancellation_via_drop() {
    let app = Router::new().route(
        "/events",
        get(|| async { Sse::new(counting_stream(10000)).keep_alive(KeepAlive::default()) }),
    );
    let (addr, _server) = start_server(app).await;

    // Take only 3 events from a stream that would produce 10000
    let events = sse::connect(format!("http://{addr}/events"))
        .take(3)
        .collect()
        .await
        .unwrap();

    assert_eq!(events.len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_no_reconnect_on_error() {
    let config = SseConfig {
        url: "http://127.0.0.1:1/nonexistent".into(),
        reconnect: false,
        ..Default::default()
    };

    let result = sse::connect_with(config).collect().await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_multiline_data() {
    let app: Router = Router::new().route(
        "/events",
        get(|| async {
            let body = "event: multi\ndata: line1\ndata: line2\ndata: line3\n\n";
            axum::response::Response::builder()
                .header("content-type", "text/event-stream")
                .header("cache-control", "no-cache")
                .body(axum::body::Body::from(body))
                .unwrap()
        }),
    );
    let (addr, _server) = start_server(app).await;

    let events = sse::connect(format!("http://{addr}/events"))
        .take(1)
        .collect()
        .await
        .unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.as_deref(), Some("multi"));
    assert_eq!(events[0].data, "line1\nline2\nline3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_custom_headers() {
    use std::sync::{Arc, Mutex};

    let captured = Arc::new(Mutex::new(None::<String>));
    let captured2 = captured.clone();

    let app = Router::new().route(
        "/events",
        get(move |headers: axum::http::HeaderMap| {
            let captured = captured2.clone();
            async move {
                if let Some(val) = headers.get("X-Custom") {
                    *captured.lock().unwrap() = Some(val.to_str().unwrap().to_owned());
                }
                Sse::new(counting_stream(1))
            }
        }),
    );
    let (addr, _server) = start_server(app).await;

    let config = SseConfig {
        url: format!("http://{addr}/events"),
        headers: vec![("X-Custom".into(), "test-value".into())],
        reconnect: false,
        ..Default::default()
    };

    let _ = sse::connect_with(config).take(1).collect().await;

    assert_eq!(captured.lock().unwrap().as_deref(), Some("test-value"));
}

/// Serves `fragments` as separate HTTP chunks, pausing between them so
/// each reaches the client as its own body chunk.
async fn fragmented_sse(fragments: Vec<&'static [u8]>) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = vec![0u8; 4096];
        let _ = socket.read(&mut request).await.unwrap();
        let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\
                    transfer-encoding: chunked\r\n\r\n";
        socket.write_all(head.as_bytes()).await.unwrap();
        for fragment in fragments {
            let frame = [
                format!("{:x}\r\n", fragment.len()).as_bytes(),
                fragment,
                b"\r\n",
            ]
            .concat();
            if socket.write_all(&frame).await.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let _ = socket.write_all(b"0\r\n\r\n").await;
    });
    format!("http://{addr}/events")
}

async fn first_event(url: String) -> Result<Option<SseEvent>, lazyflow::pull::PipeError> {
    sse::connect_with(SseConfig {
        url,
        reconnect: false,
        ..Default::default()
    })
    .first()
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_decodes_a_character_split_across_body_chunks() {
    let url = fragmented_sse(vec![b"data: \xc3", b"\xa9\n\n"]).await;
    let event = first_event(url).await.unwrap().unwrap();
    assert_eq!(event.data, "\u{e9}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_accepts_lines_ended_by_a_bare_carriage_return() {
    let url = fragmented_sse(vec![b"data: hello\r\r"]).await;
    let event = first_event(url).await.unwrap().unwrap();
    assert_eq!(event.data, "hello");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_treats_a_crlf_split_across_body_chunks_as_one_line_end() {
    let url = fragmented_sse(vec![b"data: a\r", b"\ndata: b\r", b"\n\r\n"]).await;
    let event = first_event(url).await.unwrap().unwrap();
    assert_eq!(event.data, "a\nb");
}
