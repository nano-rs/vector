//! Internal events of the shutdown release of received, unacknowledged messages (nano).

use vector_lib::{NamedInternalEvent, internal_event::InternalEvent};

/// A pull source handed messages it had received but not delivered back to the broker on
/// shutdown, so that they are redelivered right away instead of after their ack deadline or
/// visibility timeout.
#[derive(Debug, NamedInternalEvent)]
pub struct SourceMessagesReleased {
    pub component_type: &'static str,
    /// Messages released for immediate redelivery.
    pub released: usize,
    /// Messages that could not be released; they are redelivered when their ack deadline or
    /// visibility timeout expires.
    pub not_released: usize,
    /// Whether the source stopped waiting for acknowledgements before all came back.
    pub timed_out: bool,
}

impl InternalEvent for SourceMessagesReleased {
    fn emit(self) {
        info!(
            message = "Released unacknowledged messages for immediate redelivery on shutdown.",
            component_type = self.component_type,
            released = self.released,
            not_released = self.not_released,
            ack_wait_timed_out = self.timed_out,
        );
    }
}
