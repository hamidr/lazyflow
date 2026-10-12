//! chunks(n) cuts batches by element count alone; how the source happens
//! to be chunked must not show through.

mod common;

use common::{CHUNKINGS, in_chunks_of};
use lazyflow::prelude::*;

/// Full batches of `size`, then one short batch with the rest.
fn expected_sizes(len: usize, size: usize) -> Vec<usize> {
    let mut sizes = vec![size; len / size];
    if len % size != 0 {
        sizes.push(len % size);
    }
    sizes
}

#[tokio::test]
async fn only_the_last_batch_is_short_whatever_the_source_chunking() {
    for len in [0, 1, 255, 256, 257, 512] {
        let items: Vec<usize> = (0..len).collect();
        for size in [1, 7, 100, 256, 300] {
            for chunking in CHUNKINGS {
                let batches = in_chunks_of(items.clone(), chunking)
                    .chunks(size)
                    .collect()
                    .await
                    .unwrap();
                let sizes: Vec<_> = batches.iter().map(Vec::len).collect();
                let context = format!("len {len}, size {size}, chunking {chunking:?}");
                assert_eq!(sizes, expected_sizes(len, size), "{context}");
                assert_eq!(batches.concat(), items, "{context}");
            }
        }
    }
}

#[tokio::test]
async fn the_default_source_chunk_does_not_cut_a_batch_short() {
    let batches = Pipe::from_iter(0..512).chunks(100).collect().await.unwrap();
    let sizes: Vec<_> = batches.iter().map(Vec::len).collect();
    assert_eq!(sizes, [100, 100, 100, 100, 100, 12]);
}
