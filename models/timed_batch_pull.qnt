// Constrains src/pipeline/time.rs: Pipe::chunks_timeout.
// A timer may flush buffered items, but must retain an in-flight pull.
// Three source items, one in-flight item. Counts abstract away payload/order,
// channel capacity, and whole-pipeline cancellation; this checks conservation
// across pull/timer interleavings, not liveness or the full implementation.
module timed_batch_pull {
  var consumed: int
  var pending: int
  var buffered: int
  var emitted: int

  action init = all {
    consumed' = 0,
    pending' = 0,
    buffered' = 0,
    emitted' = 0,
  }

  action beginPull = all {
    consumed < 3,
    pending == 0,
    consumed' = consumed + 1,
    pending' = 1,
    buffered' = buffered,
    emitted' = emitted,
  }

  action completePull = all {
    pending == 1,
    consumed' = consumed,
    pending' = 0,
    buffered' = buffered + pending,
    emitted' = emitted,
  }

  action timerFlush(keepPending) = all {
    consumed' = consumed,
    pending' = if (keepPending) pending else 0,
    buffered' = 0,
    emitted' = emitted + buffered,
  }

  action step = any { beginPull, completePull, timerFlush(true) }

  // Negative control: the old select! dropped its pending pull on timer wins.
  action buggyStep = any { beginPull, completePull, timerFlush(false) }

  val conserved = consumed == pending + buffered + emitted
}
