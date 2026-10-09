//! Lightweight observability for pipe stages.
//!
//! [`Pipe::meter_with`](crate::pipeline::Pipe::meter_with) inserts a monitoring point that reports element
//! counts and errors to a callback, without adding external dependencies.
//!
//! ```ignore
//! let pipe = Pipe::from_iter(1..=1000)
//!     .filter(|x| x % 2 == 0)
//!     .meter(|stats| println!("{}: {} elements in {} chunks", stats.name, stats.elements, stats.chunks))
//!     .map(|x| x * 10);
//! ```

use crate::pull::{ChunkFut, PullOperator};

/// Statistics reported by a metered pipe stage.
#[derive(Debug, Clone)]
pub struct MeterStats {
    /// Name of the metered stage.
    pub name: String,
    /// Total elements seen so far.
    pub elements: u64,
    /// Total chunks pulled so far.
    pub chunks: u64,
    /// Whether the stream has completed.
    pub completed: bool,
    /// Whether the stream ended with an error.
    pub errored: bool,
}

pub(crate) struct PullMeter<B: Send + 'static> {
    pub(crate) child: Box<dyn PullOperator<B>>,
    pub(crate) on_chunk: Box<dyn Fn(&MeterStats) + Send>,
    pub(crate) stats: MeterStats,
}

impl<B: Send + 'static> PullOperator<B> for PullMeter<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            match self.child.next_chunk().await {
                Ok(Some(chunk)) => {
                    self.stats.elements += chunk.len() as u64;
                    self.stats.chunks += 1;
                    (self.on_chunk)(&self.stats);
                    Ok(Some(chunk))
                }
                Ok(None) => {
                    self.stats.completed = true;
                    (self.on_chunk)(&self.stats);
                    Ok(None)
                }
                Err(e) => {
                    self.stats.errored = true;
                    (self.on_chunk)(&self.stats);
                    Err(e)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::Pipe;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn meter_counts_elements() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let log2 = log.clone();

        let result = Pipe::from_iter(vec![1, 2, 3, 4, 5])
            .meter_with("test", move |stats| {
                log2.lock().unwrap().push(stats.clone());
            })
            .collect()
            .await
            .unwrap();

        assert_eq!(result, vec![1, 2, 3, 4, 5]);

        let snapshots = log.lock().unwrap();
        let last = snapshots.last().unwrap();
        assert_eq!(last.elements, 5);
        assert!(last.completed);
        assert!(!last.errored);
        assert_eq!(last.name, "test");
    }

    #[tokio::test]
    async fn meter_reports_errors() {
        use crate::pull::ChunkFut;

        struct Fail;
        impl PullOperator<i64> for Fail {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async { Err("boom".into()) })
            }
        }

        let errored = Arc::new(Mutex::new(false));
        let errored2 = errored.clone();

        let result = Pipe::from_pull_once(Fail)
            .meter_with("fail-stage", move |stats| {
                if stats.errored {
                    *errored2.lock().unwrap() = true;
                }
            })
            .collect()
            .await;

        assert!(result.is_err());
        assert!(*errored.lock().unwrap());
    }
}
