//! Panics in spawned producer tasks.
//!
//! A producer task feeds its consumer through a channel. If the task
//! panics, tokio catches the unwind and drops the task's sender, so the
//! channel closes exactly as it does at a normal end of stream: the
//! consumer sees a short, successful pipe. Every task that writes a
//! `Result` channel runs its body through [`report_panic`] so the panic
//! is sent down that channel as an error instead.

use crate::pull::PipeError;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Run `body`; if it panics, send the panic to the consumer on `tx`.
///
/// `tx` is the channel `body` itself writes, so the error arrives after
/// every item the task sent before it died.
pub(crate) async fn report_panic<T>(
    tx: &tokio::sync::mpsc::Sender<Result<T, PipeError>>,
    body: impl Future<Output = ()>,
) {
    if let Err(e) = catch_panic(body).await {
        let _ = tx.send(Err(e)).await;
    }
}

/// Run `body`, turning a panic in it into an error. For a task that
/// reports to more than one channel and so cannot use [`report_panic`].
pub(crate) async fn catch_panic<F: Future>(body: F) -> Result<F::Output, PipeError> {
    CatchPanic(Box::pin(body)).await
}

/// One upstream error, delivered to every branch of a fan-out.
/// `PipeError` is not `Clone`, so the branches share it; each surfaces
/// it as a `PipeError::Custom` whose `source()` is the original error.
#[derive(Debug, Clone)]
pub(crate) struct SharedError(std::sync::Arc<PipeError>);

impl SharedError {
    pub(crate) fn new(e: PipeError) -> Self {
        Self(std::sync::Arc::new(e))
    }

    pub(crate) fn into_pipe_error(self) -> PipeError {
        PipeError::Custom(Box::new(self))
    }
}

impl std::fmt::Display for SharedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for SharedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.0)
    }
}

/// The unwind is caught per poll, so the future is never polled again
/// after it panics: the task ends with the error.
struct CatchPanic<F>(Pin<Box<F>>);

impl<F: Future> Future for CatchPanic<F> {
    type Output = Result<F::Output, PipeError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let fut = self.0.as_mut();
        match catch_unwind(AssertUnwindSafe(|| fut.poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(v)) => Poll::Ready(Ok(v)),
            Err(payload) => Poll::Ready(Err(panic_error(payload))),
        }
    }
}

/// A worker's `JoinError` as the error its consumer sees: the panic
/// message when it panicked, otherwise the cancellation.
pub(crate) fn join_error(e: tokio::task::JoinError) -> PipeError {
    match e.try_into_panic() {
        Ok(payload) => panic_error(payload),
        Err(e) => PipeError::Custom(format!("worker task ended: {e}").into()),
    }
}

fn panic_error(payload: Box<dyn std::any::Any + Send>) -> PipeError {
    let msg = payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_string());
    PipeError::Custom(format!("producer task panicked: {msg}").into())
}
