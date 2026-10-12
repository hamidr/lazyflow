//! Test sources that deliver the same items under different chunkings,
//! so a test can check an operator's output does not depend on them.

use lazyflow::prelude::*;

/// `items` as source chunks whose sizes cycle through `sizes`.
pub fn in_chunks_of<T: Clone + Send + Sync + 'static>(items: Vec<T>, sizes: &[usize]) -> Pipe<T> {
    let mut rest = items.as_slice();
    let mut pipe = Pipe::empty();
    for &size in sizes.iter().cycle() {
        if rest.is_empty() {
            break;
        }
        let (piece, after) = rest.split_at(size.clamp(1, rest.len()));
        pipe = pipe.chain(Pipe::from_iter(piece.to_vec()));
        rest = after;
    }
    pipe
}

/// Chunkings worth trying: single items, odd sizes, and sizes around the
/// default source chunk of 256.
pub const CHUNKINGS: &[&[usize]] = &[&[1], &[3], &[2, 5], &[100], &[255], &[256], &[257]];
