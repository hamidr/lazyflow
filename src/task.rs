//! Background tasks whose lifetime follows their owner.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::task::{JoinError, JoinHandle};

/// A spawned task that is aborted when its handle is dropped.
///
/// Dropping a plain [`JoinHandle`] detaches the task, so work owned by a
/// cancelled pipe would keep running and performing side effects.
pub(crate) struct OwnedTask<T>(JoinHandle<T>);

impl<T: Send + 'static> OwnedTask<T> {
    pub(crate) fn spawn(future: impl Future<Output = T> + Send + 'static) -> Self {
        Self(tokio::spawn(future))
    }
}

impl<T> Drop for OwnedTask<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> Future for OwnedTask<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}
