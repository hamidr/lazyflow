//! lines_with_max_len(n) bounds every line, terminated or not, by its
//! payload: the bytes before `\n` or `\r\n`.

use lazyflow::prelude::*;

const MAX: usize = 4;

/// `wire` as byte buffers of at most `size` bytes.
fn in_buffers_of(wire: &[u8], size: usize) -> Pipe<Vec<u8>> {
    Pipe::from_iter(wire.chunks(size).map(<[u8]>::to_vec).collect::<Vec<_>>())
}

/// Splits `wire` at every buffer size and expects `ok` from each.
async fn expect_for_every_split(wire: &[u8], ok: bool) {
    for size in 1..=wire.len() {
        let result = in_buffers_of(wire, size)
            .lines_with_max_len(MAX)
            .collect()
            .await;
        assert_eq!(
            result.is_ok(),
            ok,
            "{wire:?} in buffers of {size}: {result:?}"
        );
    }
}

#[tokio::test]
async fn a_terminated_line_at_the_limit_is_accepted() {
    expect_for_every_split(b"1234\n", true).await;
    expect_for_every_split(b"1234\r\n", true).await;
}

#[tokio::test]
async fn a_terminated_line_over_the_limit_is_rejected() {
    expect_for_every_split(b"12345\n", false).await;
    expect_for_every_split(b"12345\r\n", false).await;
    expect_for_every_split(b"ok\n12345\nok\n", false).await;
}

#[tokio::test]
async fn an_unterminated_final_line_is_bounded_too() {
    expect_for_every_split(b"ok\n1234", true).await;
    expect_for_every_split(b"ok\n12345", false).await;
}

#[tokio::test]
async fn many_lines_within_the_limit_may_exceed_it_together() {
    let lines = in_buffers_of(b"1234\n1234\n1234\n", 15)
        .lines_with_max_len(MAX)
        .collect()
        .await
        .unwrap();
    assert_eq!(lines, ["1234"; 3]);
}

#[tokio::test]
async fn the_default_limit_is_one_mebibyte_per_line() {
    let at_limit = [vec![b'x'; 1024 * 1024], b"\n".to_vec()].concat();
    assert!(Pipe::once(at_limit).lines().first().await.is_ok());
    let over = [vec![b'x'; 1024 * 1024 + 1], b"\n".to_vec()].concat();
    assert!(Pipe::once(over).lines().first().await.is_err());
}
