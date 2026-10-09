//! Semi-naive fixpoint iteration.
//!
//! A recursive definition (a transitive closure, a reachability set, a
//! recursive query) is a seed plus a step that derives more items from
//! the items found so far. Re-running the step over everything each
//! round repeats all earlier work; semi-naive iteration feeds each round
//! only the items first found in the round before (the delta), and lets
//! the step read the whole set found so far when it joins against it.

use std::collections::HashSet;
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;

use super::Pipe;
use crate::pull::{ChunkFut, PullOperator};

/// What one round of [`Pipe::fixpoint`] reads.
pub struct Round<B> {
    number: usize,
    /// One segment per earlier round, the last one being the delta.
    /// Segments are shared, never copied, so handing a round the whole
    /// set costs one pointer per round rather than one per item.
    segments: Arc<[Arc<[B]>]>,
}

impl<B> Round<B> {
    /// 1 for the round that reads the seed, then 2, 3, ...
    pub fn number(&self) -> usize {
        self.number
    }

    /// The items first found in the previous round (the seed, in round 1).
    pub fn delta(&self) -> &[B] {
        self.segments.last().map(|s| &s[..]).unwrap_or(&[])
    }

    /// Every item found so far, the delta included, each once.
    pub fn full(&self) -> impl Iterator<Item = &B> {
        self.segments.iter().flat_map(|s| s.iter())
    }
}

type Step<B> = Arc<dyn Fn(Round<B>) -> Pipe<B> + Send + Sync>;

impl<B: Clone + Eq + Hash + Send + Sync + 'static> Pipe<B> {
    /// Iterate `step` from this pipe's items until a round finds nothing
    /// new, emitting each distinct item once, as soon as it is found.
    ///
    /// Round 1 reads the seed (this pipe, deduplicated) as its delta;
    /// each later round reads the items first found by the round before.
    /// An item the step yields again is dropped, which is what makes a
    /// cyclic definition terminate. A step that keeps inventing new items
    /// never terminates: bound it with [`fixpoint_bounded`](Self::fixpoint_bounded)
    /// or a cancel token.
    ///
    /// ```ignore
    /// // Reachability from node 0.
    /// let reach = Pipe::once(0).fixpoint(|round| {
    ///     Pipe::from_iter(round.delta().iter().flat_map(|n| neighbours(*n)).collect::<Vec<_>>())
    /// });
    /// ```
    pub fn fixpoint(self, step: impl Fn(Round<B>) -> Pipe<B> + Send + Sync + 'static) -> Self {
        self.fixpoint_with_hasher::<std::collections::hash_map::RandomState>(None, step)
    }

    /// [`fixpoint`](Self::fixpoint) that stops after `max_rounds` rounds
    /// even if the last one found something new. The seed is not a round:
    /// `max_rounds == 0` emits the seed alone.
    pub fn fixpoint_bounded(
        self,
        max_rounds: usize,
        step: impl Fn(Round<B>) -> Pipe<B> + Send + Sync + 'static,
    ) -> Self {
        self.fixpoint_with_hasher::<std::collections::hash_map::RandomState>(Some(max_rounds), step)
    }

    /// [`fixpoint`](Self::fixpoint) deduplicating through `S`, stopping
    /// after `max_rounds` rounds when it is `Some`. Every item found is
    /// hashed once, so on large or cheap-to-compare items the hasher can
    /// dominate; a caller that does not need SipHash's resistance to
    /// adversarial keys can pass a faster one.
    pub fn fixpoint_with_hasher<S>(
        self,
        max_rounds: Option<usize>,
        step: impl Fn(Round<B>) -> Pipe<B> + Send + Sync + 'static,
    ) -> Self
    where
        S: BuildHasher + Default + Send + 'static,
    {
        let step: Step<B> = Arc::new(step);
        let seed = self.factory;
        Self::from_factory(move || {
            Box::new(PullFixpoint {
                current: Some(seed()),
                step: Arc::clone(&step),
                max_rounds,
                rounds_run: 0,
                seen: HashSet::<B, S>::default(),
                segments: Vec::new(),
                found: Vec::new(),
            })
        })
    }
}

struct PullFixpoint<B: Send + 'static, S> {
    /// The seed, then the running round's pipe; `None` once done.
    current: Option<Box<dyn PullOperator<B>>>,
    step: Step<B>,
    max_rounds: Option<usize>,
    rounds_run: usize,
    seen: HashSet<B, S>,
    segments: Vec<Arc<[B]>>,
    /// Items the running round found first, the next round's delta.
    found: Vec<B>,
}

impl<B: Clone + Eq + Hash + Send + Sync + 'static, S: BuildHasher> PullFixpoint<B, S> {
    /// Seal the finished round's items as the next delta and start the
    /// next round, or end when there is nothing new or no rounds left.
    fn next_round(&mut self) -> Option<Box<dyn PullOperator<B>>> {
        if self.found.is_empty() || self.max_rounds.is_some_and(|m| self.rounds_run >= m) {
            return None;
        }
        self.segments.push(std::mem::take(&mut self.found).into());
        self.rounds_run += 1;
        let round = Round {
            number: self.rounds_run,
            segments: self.segments.clone().into(),
        };
        Some((self.step)(round).into_pull())
    }
}

impl<B, S> PullOperator<B> for PullFixpoint<B, S>
where
    B: Clone + Eq + Hash + Send + Sync + 'static,
    S: BuildHasher + Send,
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            while let Some(current) = self.current.as_mut() {
                match current.next_chunk().await? {
                    Some(chunk) => {
                        let new: Vec<B> = chunk
                            .into_iter()
                            .filter(|item| self.seen.insert(item.clone()))
                            .collect();
                        if !new.is_empty() {
                            self.found.extend(new.iter().cloned());
                            return Ok(Some(new));
                        }
                    }
                    None => self.current = self.next_round(),
                }
            }
            Ok(None)
        })
    }
}
