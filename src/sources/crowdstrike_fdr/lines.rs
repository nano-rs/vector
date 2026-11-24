//! Newline framing that numbers every line, so each event carries a stable offset.

use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;

/// One line of an object.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Line {
    /// Zero-based line number within the decompressed object.
    pub(super) number: u64,
    /// The line without its terminator, or `None` when it exceeded the maximum length and was
    /// dropped. Empty lines are returned as empty.
    pub(super) bytes: Option<Bytes>,
}

/// Splits on `\n` (trimming a trailing `\r`), counting lines, and drops lines longer
/// than `max_length` bytes without buffering them whole.
pub(super) struct LineCodec {
    max_length: usize,
    /// Bytes of `buf` already searched for a newline.
    scanned: usize,
    /// Inside an oversized line; its bytes are being discarded.
    discarding: bool,
    next_number: u64,
}

impl LineCodec {
    pub(super) const fn new(max_length: usize) -> Self {
        Self {
            max_length,
            scanned: 0,
            discarding: false,
            next_number: 0,
        }
    }

    const fn take_number(&mut self) -> u64 {
        let number = self.next_number;
        self.next_number += 1;
        number
    }

    fn finish_line(&mut self, mut line: BytesMut) -> Line {
        let number = self.take_number();
        if std::mem::take(&mut self.discarding) {
            return Line {
                number,
                bytes: None,
            };
        }
        if line.last() == Some(&b'\r') {
            line.truncate(line.len() - 1);
        }
        if line.len() > self.max_length {
            return Line {
                number,
                bytes: None,
            };
        }
        Line {
            number,
            bytes: Some(line.freeze()),
        }
    }
}

impl Decoder for LineCodec {
    type Item = Line;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Line>, Self::Error> {
        if let Some(offset) = buf[self.scanned..].iter().position(|byte| *byte == b'\n') {
            let end = self.scanned + offset;
            let line = buf.split_to(end);
            buf.advance(1);
            self.scanned = 0;
            return Ok(Some(self.finish_line(line)));
        }
        // No terminator yet. Keep at most one byte over the limit (plus a possible '\r'), so a
        // line that never ends cannot grow the buffer without bound.
        if buf.len() > self.max_length + 1 {
            self.discarding = true;
            buf.clear();
            self.scanned = 0;
        } else {
            self.scanned = buf.len();
        }
        Ok(None)
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Line>, Self::Error> {
        if let Some(line) = self.decode(buf)? {
            return Ok(Some(line));
        }
        if buf.is_empty() && !self.discarding {
            return Ok(None);
        }
        let line = buf.split();
        self.scanned = 0;
        Ok(Some(self.finish_line(line)))
    }
}
