//! Generic async pipeline engine -- composable, typed, pull-based data flows.
//!
//! `lazyflow` provides a lazy effectful list (`Pipe<B>`, analogous to
//! FS2's `Stream[F, O]`) and per-element operators. It can power any domain:
//! data processing, HTTP middleware, ML inference, event streaming.
//!
//! `B` is the **element type** -- chunking is an internal detail.
//!
//! # Example
//!
//! ```ignore
//! use lazyflow::prelude::*;
//!
//! let result = Pipe::from_iter(1..=10)
//!     .map(|x| x * 2)
//!     .filter(|x| *x > 10)
//!     .take(3)
//!     .collect().await?;
//! assert_eq!(result, vec![12, 14, 16]);
//! ```
//!
//! # Async operators
//!
//! For effectful per-element transforms, implement [`Operator`](crate::operator::Operator)
//! and use `.pipe(op)`. Operators capture their own state:
//!
//! ```ignore
//! #[derive(Debug)]
//! struct FetchScore { client: HttpClient }
//!
//! impl Operator<UserId, UserId> for FetchScore {
//!     fn execute<'a>(&'a self, id: UserId) -> PinFut<'a, UserId> {
//!         Box::pin(async move { /* async I/O via self.client */ Ok(id) })
//!     }
//! }
//! ```

pub mod cancel;
pub mod channel;
pub mod io;
pub mod meter;
pub mod operator;
pub mod pipeline;
pub mod pull;
pub mod signal;
pub mod sink;
pub mod stream;
pub mod topic;
pub mod transform;

pub use lazyflow_macros::{operator, pipe_fn, pull_operator};
pub use operator::PipeResult;

/// Construct a `Pipe` from literal elements.
///
/// ```ignore
/// let p = pipe![1, 2, 3];
/// assert_eq!(p.collect().await?, vec![1, 2, 3]);
/// ```
#[macro_export]
macro_rules! pipe {
    ($($elem:expr),* $(,)?) => {
        $crate::pipeline::Pipe::from_iter(vec![$($elem),*])
    };
}

/// Async generator shorthand for `Pipe::generate`.
///
/// Wraps the body in `async move { ... ; Ok(()) }` so the caller
/// does not need the trailing `Ok(())`.
///
/// ```ignore
/// let p = pipe_gen!(tx => {
///     tx.emit(1).await?;
///     tx.emit(2).await?;
/// });
/// ```
#[macro_export]
macro_rules! pipe_gen {
    ($tx:ident => $body:block) => {
        $crate::pipeline::Pipe::generate(|$tx| async move {
            $body
            #[allow(unreachable_code)]
            Ok(())
        })
    };
}

/// Like [`pipe_gen!`] but for single-use sources that capture owned resources.
///
/// Uses [`Pipe::generate_once`] under the hood -- the resulting pipe can be
/// cloned, but only one clone may be materialized.
///
/// ```ignore
/// let listener = TcpListener::bind("0.0.0.0:8080").await?;
/// let p = pipe_gen_once!(tx => {
///     loop {
///         let (stream, _) = listener.accept().await?;
///         tx.emit(stream).await?;
///     }
/// });
/// ```
#[macro_export]
macro_rules! pipe_gen_once {
    ($tx:ident => $body:block) => {
        $crate::pipeline::Pipe::generate_once(move |$tx| async move {
            $body
            #[allow(unreachable_code)]
            Ok(())
        })
    };
}

pub mod prelude {
    //! Re-exports for convenience.
    pub use crate::cancel::CancelToken;
    pub use crate::channel::{Receiver, Sender, bounded};
    pub use crate::operator::{Operator, PinFut};
    pub use crate::pipeline::{Emitter, Pipe};
    pub use crate::pull::{ChunkFut, PipeError, PullOperator};
    pub use crate::sink::Sink;
    pub use crate::transform::Transform;
    pub use crate::{operator, pipe_fn, pull_operator};
}
