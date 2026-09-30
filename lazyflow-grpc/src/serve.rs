//! Server-side gRPC streaming response.
//!
//! Converts a `Pipe<T>` into a stream that tonic server handlers can
//! return directly. Bridges pipe's `PipeError` to `tonic::Status`.
//!
//! ```ignore
//! use lazyflow_grpc::serve;
//!
//! #[tonic::async_trait]
//! impl MyService for MyServer {
//!     type StreamStream = serve::PipeResponse<MyItem>;
//!
//!     async fn stream(
//!         &self,
//!         request: Request<MyRequest>,
//!     ) -> Result<Response<Self::StreamStream>, Status> {
//!         let pipe = Pipe::from_iter(vec![item1, item2, item3])
//!             .filter(|i| i.is_valid())
//!             .map(|i| transform(i));
//!         Ok(Response::new(serve::to_stream(pipe)))
//!     }
//! }
//! ```

use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use lazyflow::pipeline::Pipe;
use lazyflow::pull::PipeError;

/// A tonic-compatible response stream backed by a `Pipe<T>`.
///
/// Implements `Stream<Item = Result<T, tonic::Status>>`, which is
/// the signature tonic expects from server-streaming RPC return types.
///
/// Created by [`to_stream`].
pub struct PipeResponse<T: Send + 'static> {
    inner: lazyflow::stream::PipeStream<T>,
}

impl<T: Send + 'static> Stream for PipeResponse<T> {
    type Item = Result<T, tonic::Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<T, tonic::Status>>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(pipe_error_to_status(e)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Convert a `Pipe<T>` into a tonic-compatible streaming response.
///
/// The pipe runs in a background task (via `Pipe::into_stream`).
/// Backpressure propagates through the bounded channel: a slow client
/// slows the pipe. Dropping the response stream cancels the pipe.
pub fn to_stream<T: Send + 'static>(pipe: Pipe<T>) -> PipeResponse<T> {
    PipeResponse {
        inner: pipe.into_stream(),
    }
}

/// Convert a `Pipe<T>` into a tonic-compatible streaming response
/// with a custom buffer size for backpressure tuning.
pub fn to_stream_buffered<T: Send + 'static>(pipe: Pipe<T>, buffer_size: usize) -> PipeResponse<T> {
    PipeResponse {
        inner: pipe.into_stream_buffered(buffer_size),
    }
}

/// Map a `PipeError` to `tonic::Status`.
///
/// - `PipeError::Io` -> `Status::internal`
/// - `PipeError::Closed` -> `Status::cancelled`
/// - `PipeError::RetryExhausted` -> `Status::unavailable`
/// - `PipeError::Custom` containing a `tonic::Status` -> unwrapped as-is
/// - `PipeError::Custom` (other) -> `Status::internal`
pub fn pipe_error_to_status(err: PipeError) -> tonic::Status {
    match err {
        PipeError::Io(e) => tonic::Status::internal(e.to_string()),
        PipeError::Closed => tonic::Status::cancelled("pipe closed"),
        PipeError::RetryExhausted => tonic::Status::unavailable("retry exhausted"),
        PipeError::Custom(e) => match e.downcast::<tonic::Status>() {
            Ok(status) => *status,
            Err(other) => tonic::Status::internal(other.to_string()),
        },
    }
}

/// Server builder (ADR-004 Phase 1): wraps `tonic::transport::Server`,
/// eliminating the TLS/mTLS/interceptor bootstrap boilerplate every
/// lazyflow-grpc service otherwise repeats. Deliberately thin -- `.build()`
/// hands back tonic's own `Server`, so `.serve()`, `.serve_with_shutdown()`,
/// `.add_service()`, and every other tonic transport API this wrapper does
/// not re-expose keep working exactly as they do today. lazyflow-grpc adds
/// convenience over tonic, it does not replace it.
///
/// ```ignore
/// use lazyflow_grpc::serve;
///
/// # #[cfg(feature = "tls")]
/// # async fn example(cert_pem: &[u8], key_pem: &[u8], svc: impl Clone) -> Result<(), Box<dyn std::error::Error>> {
/// let server = serve::Server::builder()
///     .tls(cert_pem, key_pem)
///     .build()?;
///
/// server.serve("0.0.0.0:50051".parse()?, svc).await?;
/// # Ok(())
/// # }
/// ```
pub struct Server<L = tower::layer::util::Identity> {
    inner: tonic::transport::Server<L>,
    #[cfg(feature = "tls")]
    tls: Option<tonic::transport::ServerTlsConfig>,
}

impl Server<tower::layer::util::Identity> {
    /// Start building a server. Mirrors `tonic::transport::Server::builder()`.
    pub fn builder() -> Self {
        Server {
            inner: tonic::transport::Server::builder(),
            #[cfg(feature = "tls")]
            tls: None,
        }
    }
}

impl<L> Server<L> {
    /// Set the server's TLS identity from PEM-encoded certificate and
    /// private key. Requires the `tls` feature.
    #[cfg(feature = "tls")]
    pub fn tls(mut self, cert_pem: impl AsRef<[u8]>, key_pem: impl AsRef<[u8]>) -> Self {
        let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);
        let cfg = self.tls.take().unwrap_or_default().identity(identity);
        self.tls = Some(cfg);
        self
    }

    /// Require and validate client certificates against a PEM-encoded CA
    /// (mutual TLS). Has no effect unless `.tls()` is also set. Requires
    /// the `tls` feature.
    #[cfg(feature = "tls")]
    pub fn client_ca(mut self, ca_pem: impl AsRef<[u8]>) -> Self {
        let cert = tonic::transport::Certificate::from_pem(ca_pem);
        let cfg = self.tls.take().unwrap_or_default().client_ca_root(cert);
        self.tls = Some(cfg);
        self
    }

    /// Make client certificate presentation optional rather than required.
    /// Has no effect unless `.client_ca()` is also set. Requires the `tls`
    /// feature.
    #[cfg(feature = "tls")]
    pub fn client_auth_optional(mut self, optional: bool) -> Self {
        let cfg = self
            .tls
            .take()
            .unwrap_or_default()
            .client_auth_optional(optional);
        self.tls = Some(cfg);
        self
    }

    /// Wrap every request through a tonic [`Interceptor`](tonic::service::Interceptor)
    /// (e.g. auth). At most one interceptor call is meaningful here; an
    /// interceptor that needs to run several checks composes them itself.
    pub fn interceptor<I>(
        self,
        interceptor: I,
    ) -> Server<tower::layer::util::Stack<tonic::service::InterceptorLayer<I>, L>>
    where
        I: tonic::service::Interceptor + Clone,
    {
        Server {
            inner: self
                .inner
                .layer(tonic::service::InterceptorLayer::new(interceptor)),
            #[cfg(feature = "tls")]
            tls: self.tls,
        }
    }

    /// Apply the configured TLS settings (if any) and hand back tonic's
    /// own `Server`, ready for `.serve()`, `.serve_with_shutdown()`,
    /// `.add_service()`, or any tonic transport configuration
    /// (custom codecs, additional Tower layers, health checks,
    /// reflection) this wrapper does not re-expose. Fails only if the
    /// TLS identity or CA is malformed.
    pub fn build(self) -> Result<tonic::transport::Server<L>, tonic::transport::Error> {
        #[cfg(feature = "tls")]
        {
            match self.tls {
                Some(tls) => self.inner.tls_config(tls),
                None => Ok(self.inner),
            }
        }
        #[cfg(not(feature = "tls"))]
        {
            Ok(self.inner)
        }
    }
}
