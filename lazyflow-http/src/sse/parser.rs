//! Incremental `text/event-stream` parser.
//!
//! HTTP body chunks need not align with characters or lines, so the parser
//! accepts arbitrary byte fragments and keeps the unfinished UTF-8
//! sequence, line, and event between them. It also keeps the stream's
//! resume point, the last event ID, across reconnections. It knows
//! nothing about connections or retry policy; the caller owns those.

use std::time::Duration;

use lazyflow::pull::PipeError;

use super::SseEvent;

/// Parser state for an event stream and its reconnections.
#[derive(Default)]
pub(super) struct EventStreamParser {
    /// Bound on the bytes the event under construction retains, the line
    /// in progress included, so a server cannot grow it without limit.
    max_event_size: usize,
    /// Set by `id` fields; committed to `last_event_id` at each blank line.
    id_buffer: String,
    /// The resume point sent as `Last-Event-ID`; empty means none.
    last_event_id: String,
    /// Trailing bytes of a UTF-8 sequence the last fragment cut short.
    utf8_tail: Vec<u8>,
    /// Whether text has been decoded yet; a leading BOM is skipped.
    started: bool,
    /// The line being read.
    line: String,
    /// The previous character was CR, so an immediate LF ends nothing.
    after_cr: bool,
    event: PendingEvent,
    /// Reconnection time last set by a `retry` field, not yet taken.
    retry: Option<Duration>,
}

/// Fields collected for the event under construction.
#[derive(Default)]
struct PendingEvent {
    event_type: Option<String>,
    data: String,
    id: Option<String>,
    retry: Option<Duration>,
}

impl EventStreamParser {
    /// A parser whose stream resumes from `last_event_id` and whose
    /// events hold at most `max_event_size` bytes.
    pub(super) fn new(last_event_id: Option<String>, max_event_size: usize) -> Self {
        let last_event_id = last_event_id.unwrap_or_default();
        Self {
            max_event_size,
            id_buffer: last_event_id.clone(),
            last_event_id,
            ..Self::default()
        }
    }

    /// Starts over for a new connection. Partial input is discarded,
    /// including an unfinished event's `id`; the resume point is kept.
    pub(super) fn restart(&mut self) {
        *self = Self::new(self.last_event_id(), self.max_event_size);
    }

    /// The ID to resume from on reconnection, if the server set one.
    pub(super) fn last_event_id(&self) -> Option<String> {
        Some(self.last_event_id.clone()).filter(|id| !id.is_empty())
    }

    /// Feeds one body fragment and returns the events it completes. Fails
    /// once the event being read exceeds the size limit; the parser must
    /// then be restarted.
    pub(super) fn feed(&mut self, bytes: &[u8]) -> Result<Vec<SseEvent>, PipeError> {
        let text = self.decode(bytes);
        let mut text = text.as_str();
        if !self.started && !text.is_empty() {
            self.started = true;
            text = text.strip_prefix('\u{FEFF}').unwrap_or(text);
        }

        let mut events = Vec::new();
        for c in text.chars() {
            if std::mem::take(&mut self.after_cr) && c == '\n' {
                continue;
            }
            match c {
                '\r' | '\n' => {
                    self.after_cr = c == '\r';
                    let line = std::mem::take(&mut self.line);
                    events.extend(self.process_line(&line));
                    self.check_size(0)?;
                }
                c => {
                    self.check_size(c.len_utf8())?;
                    self.line.push(c);
                }
            }
        }
        Ok(events)
    }

    /// Fails if retaining `extra` more bytes would exceed the limit.
    fn check_size(&self, extra: usize) -> Result<(), PipeError> {
        let event = &self.event;
        let retained = self.line.len()
            + event.data.len()
            + event.event_type.as_ref().map_or(0, String::len)
            + event.id.as_ref().map_or(0, String::len);
        if retained + extra > self.max_event_size {
            return Err(PipeError::Custom(
                format!("SSE event exceeded {} bytes", self.max_event_size).into(),
            ));
        }
        Ok(())
    }

    /// Takes the reconnection time the server last asked for, if any.
    pub(super) fn take_retry(&mut self) -> Option<Duration> {
        self.retry.take()
    }

    /// Decodes as UTF-8 across fragment boundaries. Invalid bytes become
    /// U+FFFD, as the SSE specification requires; a sequence cut short at
    /// the end of the fragment waits for the next one.
    fn decode(&mut self, bytes: &[u8]) -> String {
        let mut input = std::mem::take(&mut self.utf8_tail);
        input.extend_from_slice(bytes);
        let mut out = String::with_capacity(input.len());
        let mut rest = input.as_slice();
        loop {
            match std::str::from_utf8(rest) {
                Ok(valid) => {
                    out.push_str(valid);
                    return out;
                }
                Err(e) => {
                    let (valid, after) = rest.split_at(e.valid_up_to());
                    out.push_str(std::str::from_utf8(valid).expect("checked by valid_up_to"));
                    match e.error_len() {
                        Some(invalid) => {
                            out.push('\u{FFFD}');
                            rest = &after[invalid..];
                        }
                        None => {
                            self.utf8_tail = after.to_vec();
                            return out;
                        }
                    }
                }
            }
        }
    }

    fn process_line(&mut self, line: &str) -> Option<SseEvent> {
        if line.is_empty() {
            return self.dispatch();
        }
        let (field, value) = match parse_field(line)? {
            ("retry", value) => {
                if let Ok(ms) = value.parse::<u64>() {
                    let retry = Duration::from_millis(ms);
                    self.event.retry = Some(retry);
                    self.retry = Some(retry);
                }
                return None;
            }
            field_value => field_value,
        };
        match field {
            "event" => self.event.event_type = Some(value.to_owned()),
            "data" => {
                self.event.data.push_str(value);
                self.event.data.push('\n');
            }
            "id" if !value.contains('\0') => {
                self.event.id = Some(value.to_owned());
                self.id_buffer = value.to_owned();
            }
            _ => {}
        }
        None
    }

    /// Ends the event at a blank line. The resume point advances even
    /// when the event has no data and so is not dispatched.
    fn dispatch(&mut self) -> Option<SseEvent> {
        self.last_event_id.clone_from(&self.id_buffer);
        let mut event = std::mem::take(&mut self.event);
        if event.data.is_empty() {
            return None;
        }
        event.data.pop();
        Some(SseEvent {
            event: event.event_type,
            data: event.data,
            id: event.id,
            retry: event.retry,
        })
    }
}

/// Splits a line into field name and value; `None` for a comment.
fn parse_field(line: &str) -> Option<(&str, &str)> {
    if line.starts_with(':') {
        return None;
    }
    match line.split_once(':') {
        Some((field, value)) => Some((field, value.strip_prefix(' ').unwrap_or(value))),
        None => Some((line, "")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: usize = 1024;

    fn parser() -> EventStreamParser {
        EventStreamParser::new(None, LIMIT)
    }

    fn parse(text: &str) -> Vec<SseEvent> {
        parser().feed(text.as_bytes()).unwrap()
    }

    /// Feeds `wire` split into two fragments at every byte position and
    /// checks each split yields the same events as the unsplit input.
    fn assert_split_invariant(wire: &[u8]) {
        let whole = parser().feed(wire).unwrap();
        for at in 0..=wire.len() {
            let mut parser = parser();
            let mut events = parser.feed(&wire[..at]).unwrap();
            events.extend(parser.feed(&wire[at..]).unwrap());
            assert_eq!(events, whole, "split at byte {at}");
        }
    }

    #[test]
    fn parse_field_strips_one_leading_space_only() {
        assert_eq!(parse_field("data: hello"), Some(("data", "hello")));
        assert_eq!(parse_field("data:  two"), Some(("data", " two")));
        assert_eq!(parse_field("data:"), Some(("data", "")));
        assert_eq!(parse_field("data"), Some(("data", "")));
        assert_eq!(parse_field(": comment"), None);
    }

    #[test]
    fn a_complete_event_carries_its_type_data_and_id() {
        let events = parse("event: update\ndata: hello\nid: 1\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.as_deref(), Some("update"));
        assert_eq!(events[0].data, "hello");
        assert_eq!(events[0].id.as_deref(), Some("1"));
    }

    #[test]
    fn data_lines_join_with_newlines() {
        let events = parse("data: line1\ndata: line2\ndata: line3\n\n");
        assert_eq!(events[0].data, "line1\nline2\nline3");
    }

    #[test]
    fn blank_lines_separate_events() {
        let events = parse("data: first\n\ndata: second\n\n");
        let data: Vec<_> = events.iter().map(|e| e.data.as_str()).collect();
        assert_eq!(data, ["first", "second"]);
    }

    #[test]
    fn comments_and_unknown_fields_are_ignored() {
        let events = parse(": comment\ndata: hello\nfoo: bar\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "hello");
    }

    #[test]
    fn retry_sets_the_event_and_the_reconnection_time() {
        let mut parser = parser();
        let events = parser.feed(b"data: hello\nretry: 5000\n\n").unwrap();
        assert_eq!(events[0].retry, Some(Duration::from_millis(5000)));
        assert_eq!(parser.take_retry(), Some(Duration::from_millis(5000)));
        assert_eq!(parser.take_retry(), None);
    }

    #[test]
    fn an_id_containing_null_is_ignored() {
        assert_eq!(parse("data: hello\nid: bad\0id\n\n")[0].id, None);
    }

    #[test]
    fn an_event_without_data_is_not_dispatched() {
        assert!(parse("event: ping\n\n").is_empty());
    }

    #[test]
    fn a_data_field_without_colon_dispatches_empty_data() {
        assert_eq!(parse("data\n\n")[0].data, "");
    }

    #[test]
    fn cr_lf_and_crlf_all_end_lines() {
        for wire in [
            "data: a\rdata: b\r\r",
            "data: a\ndata: b\n\n",
            "data: a\r\ndata: b\r\n\r\n",
        ] {
            let events = parse(wire);
            assert_eq!(events.len(), 1, "{wire:?}");
            assert_eq!(events[0].data, "a\nb", "{wire:?}");
        }
    }

    #[test]
    fn a_leading_bom_is_skipped_once() {
        let events = parse("\u{FEFF}data: a\n\n");
        assert_eq!(events[0].data, "a");
        let mut parser = parser();
        parser.feed(b"data: a\n\n").unwrap();
        let later = parser.feed("\u{FEFF}data: b\n\n".as_bytes()).unwrap();
        assert!(later.is_empty(), "a later BOM is part of a field name");
    }

    #[test]
    fn invalid_utf8_becomes_replacement_characters() {
        let events = parser().feed(b"data: a\xffb\n\n").unwrap();
        assert_eq!(events[0].data, "a\u{FFFD}b");
    }

    #[test]
    fn an_unfinished_event_at_the_end_is_not_dispatched() {
        assert!(parse("data: partial\n").is_empty());
        assert!(parse("data: partial").is_empty());
    }

    #[test]
    fn events_do_not_depend_on_where_the_wire_is_split() {
        assert_split_invariant(
            "\u{FEFF}: hi\r\nevent: e\u{e9}\rdata: \u{1F600} one\ndata:two\r\n\r\nid: 7\ndata\n\n"
                .as_bytes(),
        );
    }

    fn resume_point_after(wire: &str) -> Option<String> {
        let mut parser = parser();
        parser.feed(wire.as_bytes()).unwrap();
        parser.last_event_id()
    }

    #[test]
    fn an_id_without_data_still_sets_the_resume_point() {
        assert_eq!(resume_point_after("id: 7\n\n").as_deref(), Some("7"));
    }

    #[test]
    fn the_resume_point_survives_events_without_an_id() {
        let wire = "id: 3\ndata: a\n\ndata: b\n\n";
        assert_eq!(resume_point_after(wire).as_deref(), Some("3"));
    }

    #[test]
    fn an_empty_id_clears_the_resume_point() {
        assert_eq!(resume_point_after("id: 3\n\nid\n\n"), None);
    }

    #[test]
    fn an_id_containing_null_leaves_the_resume_point() {
        assert_eq!(
            resume_point_after("id: 3\n\nid: x\0y\n\n").as_deref(),
            Some("3")
        );
    }

    #[test]
    fn an_id_in_an_unfinished_event_is_not_committed() {
        assert_eq!(
            resume_point_after("id: 3\n\nid: 4\ndata: x\n"),
            Some("3".into())
        );
        let mut parser = parser();
        parser.feed(b"id: 3\n\nid: 4\n").unwrap();
        parser.restart();
        parser.feed(b"data: y\n\n").unwrap();
        assert_eq!(parser.last_event_id().as_deref(), Some("3"));
    }

    #[test]
    fn a_restart_keeps_the_resume_point_and_drops_partial_input() {
        let mut parser = EventStreamParser::new(Some("1".into()), LIMIT);
        parser.feed(b"data: cut").unwrap();
        parser.restart();
        let events = parser.feed(b"data: whole\n\n").unwrap();
        assert_eq!(events[0].data, "whole");
        assert_eq!(parser.last_event_id().as_deref(), Some("1"));
    }

    #[test]
    fn an_event_at_the_size_limit_is_dispatched() {
        // Retained: the line in progress, then its value plus a newline.
        let value = "x".repeat(LIMIT - "data: ".len());
        let events = parse(&format!("data: {value}\n\n"));
        assert_eq!(events[0].data, value);
    }

    #[test]
    fn a_line_longer_than_the_limit_fails() {
        let value = "x".repeat(LIMIT - "data: ".len() + 1);
        assert!(parser().feed(format!("data: {value}").as_bytes()).is_err());
    }

    #[test]
    fn many_short_data_lines_cannot_exceed_the_limit() {
        let line = "data: 0123456789\n".repeat(LIMIT / 11 + 1);
        assert!(parser().feed(line.as_bytes()).is_err());
    }

    #[test]
    fn the_type_and_id_count_toward_the_limit() {
        let name = "e".repeat(LIMIT / 2);
        let wire = format!("event: {name}\nid: {name}\ndata: x\n");
        assert!(parser().feed(wire.as_bytes()).is_err());
    }

    #[test]
    fn the_limit_applies_across_fragments_and_resets_per_event() {
        let mut parser = parser();
        let half = format!("data: {}\n", "x".repeat(LIMIT / 2));
        parser.feed(half.as_bytes()).unwrap();
        parser.feed(b"\n").unwrap();
        parser.feed(half.as_bytes()).unwrap();
        assert!(parser.feed(half.as_bytes()).is_err());
    }
}
