use std::net::SocketAddr;
use std::time::Duration;

use lazyflow::pipeline::Pipe;
use tonic::{Request, Response, Status};

pub mod pipe_grpc_test {
    tonic::include_proto!("pipe_grpc_test");
}

use pipe_grpc_test::test_streaming_client::TestStreamingClient;
use pipe_grpc_test::test_streaming_server::{TestStreaming, TestStreamingServer};
use pipe_grpc_test::{StreamItem, StreamRequest};

#[derive(Default)]
struct PipeBackedService;

#[tonic::async_trait]
impl TestStreaming for PipeBackedService {
    type ServerStreamStream = lazyflow_grpc::serve::PipeResponse<StreamItem>;

    async fn server_stream(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::ServerStreamStream>, Status> {
        let count = request.into_inner().count;
        let pipe = Pipe::from_iter(0..count).map(move |i| StreamItem {
            value: i,
            label: format!("item-{i}"),
        });
        Ok(Response::new(lazyflow_grpc::serve::to_stream(pipe)))
    }
}

async fn start_server() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(TestStreamingServer::new(PipeBackedService))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipe_backed_server_streams_all_items() {
    let addr = start_server().await;
    let mut client = TestStreamingClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    let response = client
        .server_stream(Request::new(StreamRequest { count: 5 }))
        .await
        .unwrap();

    let items: Vec<StreamItem> = lazyflow_grpc::streaming::from_tonic(response.into_inner())
        .collect()
        .await
        .unwrap();

    assert_eq!(items.len(), 5);
    for (i, item) in items.iter().enumerate() {
        assert_eq!(item.value, i as i32);
        assert_eq!(item.label, format!("item-{i}"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipe_backed_server_handles_empty() {
    let addr = start_server().await;
    let mut client = TestStreamingClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    let response = client
        .server_stream(Request::new(StreamRequest { count: 0 }))
        .await
        .unwrap();

    let items: Vec<StreamItem> = lazyflow_grpc::streaming::from_tonic(response.into_inner())
        .collect()
        .await
        .unwrap();

    assert!(items.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipe_backed_server_with_filter() {
    let addr = start_server().await;
    let mut client = TestStreamingClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    let response = client
        .server_stream(Request::new(StreamRequest { count: 10 }))
        .await
        .unwrap();

    let evens: Vec<i32> = lazyflow_grpc::streaming::from_tonic(response.into_inner())
        .map(|item| item.value)
        .filter(|v| *v % 2 == 0)
        .collect()
        .await
        .unwrap();

    assert_eq!(evens, vec![0, 2, 4, 6, 8]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_take_cancels_server_pipe() {
    let addr = start_server().await;
    let mut client = TestStreamingClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    let response = client
        .server_stream(Request::new(StreamRequest { count: 10000 }))
        .await
        .unwrap();

    let items: Vec<StreamItem> = lazyflow_grpc::streaming::from_tonic(response.into_inner())
        .take(3)
        .collect()
        .await
        .unwrap();

    assert_eq!(items.len(), 3);
}

#[tokio::test]
async fn pipe_error_to_status_roundtrip() {
    let original = tonic::Status::permission_denied("forbidden");
    let pipe_err = lazyflow_grpc::streaming::status_to_pipe_error(original);
    let recovered = lazyflow_grpc::serve::pipe_error_to_status(pipe_err);
    assert_eq!(recovered.code(), tonic::Code::PermissionDenied);
    assert!(recovered.message().contains("forbidden"));
}

#[tokio::test]
async fn pipe_error_io_maps_to_internal() {
    let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken");
    let pipe_err = lazyflow::pull::PipeError::Io(io_err);
    let status = lazyflow_grpc::serve::pipe_error_to_status(pipe_err);
    assert_eq!(status.code(), tonic::Code::Internal);
    assert!(status.message().contains("broken"));
}

#[tokio::test]
async fn pipe_error_closed_maps_to_cancelled() {
    let status = lazyflow_grpc::serve::pipe_error_to_status(lazyflow::pull::PipeError::Closed);
    assert_eq!(status.code(), tonic::Code::Cancelled);
}

#[tokio::test]
async fn pipe_error_retry_exhausted_maps_to_unavailable() {
    let status =
        lazyflow_grpc::serve::pipe_error_to_status(lazyflow::pull::PipeError::RetryExhausted);
    assert_eq!(status.code(), tonic::Code::Unavailable);
}

// ADR-004 Phase 1: `serve::Server` builder.

/// `serve::Server::builder().build()` hands back a real
/// `tonic::transport::Server`, so `.serve_with_incoming()` (and every other
/// tonic transport method) works exactly as it does when built directly.
/// This is the core claim: the builder adds convenience, it does not
/// replace tonic's API.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn builder_server_round_trips_through_tonic_serve_with_incoming() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        lazyflow_grpc::serve::Server::builder()
            .build()
            .unwrap()
            .serve_with_incoming(
                TestStreamingServer::new(PipeBackedService),
                tokio_stream::wrappers::TcpListenerStream::new(listener),
            )
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = TestStreamingClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let response = client
        .server_stream(Request::new(StreamRequest { count: 5 }))
        .await
        .unwrap();
    let items: Vec<StreamItem> = lazyflow_grpc::streaming::from_tonic(response.into_inner())
        .collect()
        .await
        .unwrap();
    assert_eq!(items.len(), 5);
}

/// `.serve_with_incoming_shutdown` on the builder's `Server` stops
/// accepting once the signal future resolves, same as raw tonic. Proves
/// the builder doesn't interfere with graceful shutdown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn builder_server_stops_on_shutdown_signal() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let handle = tokio::spawn(async move {
        lazyflow_grpc::serve::Server::builder()
            .build()
            .unwrap()
            .serve_with_incoming_shutdown(
                TestStreamingServer::new(PipeBackedService),
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = shutdown_rx.await;
                },
            )
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The server is up before shutdown: one request succeeds.
    let mut client = TestStreamingClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    client
        .server_stream(Request::new(StreamRequest { count: 1 }))
        .await
        .unwrap();

    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("serve_with_incoming_shutdown must return once the signal fires")
        .unwrap();
}

/// `.interceptor()` runs on every request before it reaches the service --
/// the core mechanism ADR-004 asks for (auth, request logging, etc.). A
/// rejecting interceptor stops the request from ever reaching
/// `PipeBackedService`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn builder_server_interceptor_rejects_before_the_service_runs() {
    #[derive(Clone)]
    struct RejectAll;
    impl tonic::service::Interceptor for RejectAll {
        fn call(&mut self, _req: Request<()>) -> Result<Request<()>, Status> {
            Err(Status::permission_denied("rejected by interceptor"))
        }
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        lazyflow_grpc::serve::Server::builder()
            .interceptor(RejectAll)
            .build()
            .unwrap()
            .serve_with_incoming(
                TestStreamingServer::new(PipeBackedService),
                tokio_stream::wrappers::TcpListenerStream::new(listener),
            )
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = TestStreamingClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let err = client
        .server_stream(Request::new(StreamRequest { count: 5 }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert!(err.message().contains("rejected by interceptor"));
}

#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn builder_server_tls_handshake_round_trips() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        lazyflow_grpc::serve::Server::builder()
            .tls(cert_pem.clone(), key_pem.clone())
            .build()
            .unwrap()
            .serve_with_incoming(
                TestStreamingServer::new(PipeBackedService),
                tokio_stream::wrappers::TcpListenerStream::new(listener),
            )
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let ca = tonic::transport::Certificate::from_pem(cert.cert.pem());
    let tls_config = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(ca)
        .domain_name("localhost");
    let channel = tonic::transport::Channel::from_shared(format!("https://{addr}"))
        .unwrap()
        .tls_config(tls_config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = TestStreamingClient::new(channel);

    let response = client
        .server_stream(Request::new(StreamRequest { count: 3 }))
        .await
        .unwrap();
    let items: Vec<StreamItem> = lazyflow_grpc::streaming::from_tonic(response.into_inner())
        .collect()
        .await
        .unwrap();
    assert_eq!(items.len(), 3);
}
