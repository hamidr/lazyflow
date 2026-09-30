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
