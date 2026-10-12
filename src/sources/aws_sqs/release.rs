//! Hands SQS messages back to the queue when the source shuts down.
//!
//! A message the source received but did not delete stays invisible for the queue's visibility
//! timeout (often several minutes). When the source stops before those messages are delivered
//! downstream, it sets their visibility timeout to zero instead, so the next consumer receives
//! them right away. Nothing here deletes a message: deleting stays with the acknowledgement path.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::time::Instant;

use aws_sdk_sqs::{Client as SqsClient, types::ChangeMessageVisibilityBatchRequestEntry};

use crate::{internal_events::SourceMessagesReleased, shutdown::ShutdownSignal};

/// The most entries `ChangeMessageVisibilityBatch` takes.
const MAX_BATCH_SIZE: usize = 10;

/// The time a shutting-down source has in all before Vector stops it outright: nano's pull
/// container runs Vector with `--graceful-shutdown-limit-secs 15`
/// (`VECTOR_GRACEFUL_SHUTDOWN_LIMIT_SECS` in nanosiem's `pull-container.js`). The release must be
/// done within it, or the messages wait out their visibility timeout after all.
const GRACEFUL_SHUTDOWN_LIMIT: Duration = Duration::from_secs(15);

/// How long after shutdown the source waits for the acknowledgements of messages already sent
/// downstream before it releases what is left. A long poll in progress runs concurrently.
const SHUTDOWN_ACK_WAIT: Duration = Duration::from_secs(8);

/// How long releasing may take. Releasing is best effort: what is not released in time becomes
/// visible again when its visibility timeout expires.
const RELEASE_WAIT: Duration = Duration::from_secs(5);

/// The longest a receive call long-polls, in seconds; `poll_secs` above it is capped. A poll
/// open at shutdown must end before the release: one abandoned client-side can still be answered
/// server-side (stranding its messages for the visibility timeout), and one still open could
/// receive the messages just released (another receive per release, toward a dead-letter
/// queue's `maxReceiveCount`). So polls opened before shutdown are awaited, and they all end
/// before the acknowledgement wait does. The cost is an empty receive every 5 s per receiving
/// task while the queue is idle.
pub(crate) const MAX_POLL_WAIT_SECS: u32 = if cfg!(test) { 1 } else { POLL_WAIT_CAP_SECS };

const POLL_WAIT_CAP_SECS: u32 = 5;

const _: () = assert!((POLL_WAIT_CAP_SECS as u64) + 1 < SHUTDOWN_ACK_WAIT.as_secs());
// The acknowledgement wait and the release fit the graceful shutdown limit, with margin.
const _: () = assert!(
    SHUTDOWN_ACK_WAIT.as_secs() + RELEASE_WAIT.as_secs() + 2 <= GRACEFUL_SHUTDOWN_LIMIT.as_secs()
);
// The same ordering in tests: polls end well before the acknowledgement wait does.
const _: () = assert!((MAX_POLL_WAIT_SECS as u64) + 1 < SHUTDOWN_ACK_TIMEOUT.as_secs());

/// How long before the end of the release a poll answered late is given up, leaving time to
/// release what it returns.
const LATE_POLL_MARGIN: Duration = Duration::from_secs(1);

/// [`SHUTDOWN_ACK_WAIT`], shortened in tests.
const SHUTDOWN_ACK_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_secs(3)
} else {
    SHUTDOWN_ACK_WAIT
};

/// The deadlines of one shutdown, all measured from when the shutdown signal fired. Polls open at
/// shutdown end within [`MAX_POLL_WAIT_SECS`], before `acknowledgements`; the release starts by
/// `acknowledgements` and ends by `release` (13 s, under the 15 s graceful shutdown limit).
#[derive(Clone, Copy, Debug)]
pub(crate) struct ShutdownDeadlines {
    /// The end of the wait for acknowledgements of messages already sent downstream, and for the
    /// polls open at shutdown, which normally end well before it.
    pub(crate) acknowledgements: Instant,
    /// The end of the wait for a poll answered late (a slow network or service): until then it
    /// is still awaited rather than abandoned, and what it returns is released directly.
    pub(crate) late_poll: Instant,
    /// The end of the release.
    pub(crate) release: Instant,
}

impl ShutdownDeadlines {
    pub(crate) fn starting_at(start: Instant) -> Self {
        let release = start + SHUTDOWN_ACK_TIMEOUT + RELEASE_WAIT;
        Self {
            acknowledgements: start + SHUTDOWN_ACK_TIMEOUT,
            late_poll: release - LATE_POLL_MARGIN,
            release,
        }
    }

    /// A release deadline for a release that starts now, outside the shutdown sequence (always
    /// before the end of the acknowledgement wait, or right after it).
    pub(crate) fn release_from_now() -> Instant {
        Instant::now() + RELEASE_WAIT
    }

    /// The deadlines of the shutdown `shutdown` signals, once it fires.
    pub(crate) async fn of(shutdown: ShutdownSignal) -> Self {
        _ = shutdown.await;
        Self::starting_at(Instant::now())
    }
}

/// The receipt handles of messages that were received and not yet deleted (or handed to the
/// deletion path). Shared between the receiving tasks and the acknowledgement task.
#[derive(Clone, Debug, Default)]
pub(crate) struct InFlight(Arc<Mutex<InFlightState>>);

#[derive(Debug, Default)]
struct InFlightState {
    receipts: HashSet<String>,
    /// Taken for release: nothing is tracked any more.
    closed: bool,
}

impl InFlight {
    /// Tracks `receipts`. Returns false, tracking nothing, once the in-flight receipts were taken
    /// for release: the caller then releases these itself.
    #[must_use]
    pub(crate) fn insert<'a>(&self, receipts: impl IntoIterator<Item = &'a String>) -> bool {
        let mut state = self.0.lock().expect("in-flight lock poisoned");
        if state.closed {
            return false;
        }
        state.receipts.extend(receipts.into_iter().cloned());
        true
    }

    /// Removes receipts that are about to be deleted (or otherwise settled). This must happen
    /// before the deletion is requested, so that a message is never both deleted and released.
    pub(crate) fn remove<'a>(&self, receipts: impl IntoIterator<Item = &'a String>) {
        let mut state = self.0.lock().expect("in-flight lock poisoned");
        for receipt in receipts {
            state.receipts.remove(receipt);
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.0
            .lock()
            .expect("in-flight lock poisoned")
            .receipts
            .len()
    }

    /// Takes every receipt still in flight; they will not be deleted by this source any more, and
    /// nothing is tracked from now on.
    pub(crate) fn take(&self) -> Vec<String> {
        let mut state = self.0.lock().expect("in-flight lock poisoned");
        state.closed = true;
        state.receipts.drain().collect()
    }
}

/// Sets the visibility timeout of a batch of messages to zero. Returns how many failed.
#[async_trait::async_trait]
pub(crate) trait Release: Send + Sync {
    async fn release_batch(&self, receipts: &[String]) -> Result<usize, String>;
}

/// Releases through `ChangeMessageVisibilityBatch`.
pub(crate) struct SqsRelease<'a> {
    pub client: &'a SqsClient,
    pub queue_url: &'a str,
}

#[async_trait::async_trait]
impl Release for SqsRelease<'_> {
    async fn release_batch(&self, receipts: &[String]) -> Result<usize, String> {
        let entries = receipts
            .iter()
            .enumerate()
            .map(|(id, receipt)| {
                ChangeMessageVisibilityBatchRequestEntry::builder()
                    .id(id.to_string())
                    .receipt_handle(receipt)
                    .visibility_timeout(0)
                    .build()
                    .expect("all required builder parameters specified")
            })
            .collect();
        self.client
            .change_message_visibility_batch()
            .queue_url(self.queue_url)
            .set_entries(Some(entries))
            .send()
            .await
            .map(|output| output.failed.len())
            .map_err(|error| error.to_string())
    }
}

/// Releases `receipts` in batches, by `deadline`, and logs the outcome. Each receipt is sent at
/// most once. `ack_wait_timed_out` says whether the source gave up waiting for acknowledgements
/// (reported in the log).
pub(crate) async fn release(
    component_type: &'static str,
    releaser: &dyn Release,
    receipts: Vec<String>,
    ack_wait_timed_out: bool,
    deadline: Instant,
) {
    if receipts.is_empty() {
        return;
    }
    let total = receipts.len();
    let mut released = 0;
    let work = async {
        for batch in receipts.chunks(MAX_BATCH_SIZE) {
            match releaser.release_batch(batch).await {
                Ok(failed) => released += batch.len() - failed.min(batch.len()),
                Err(error) => debug!(message = "Failed to release SQS messages.", %error),
            }
        }
    };
    // A timeout here is reported as `not_released`.
    let _ = tokio::time::timeout_at(deadline, work).await;
    emit!(SourceMessagesReleased {
        component_type,
        released,
        not_released: total - released,
        timed_out: ack_wait_timed_out,
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use super::*;

    #[derive(Default)]
    struct MockRelease {
        calls: StdMutex<Vec<Vec<String>>>,
        fail_batch: Option<usize>,
    }

    #[async_trait::async_trait]
    impl Release for MockRelease {
        async fn release_batch(&self, receipts: &[String]) -> Result<usize, String> {
            let mut calls = self.calls.lock().unwrap();
            calls.push(receipts.to_vec());
            if self.fail_batch == Some(calls.len() - 1) {
                return Err("injected".into());
            }
            Ok(0)
        }
    }

    fn receipts(range: std::ops::Range<usize>) -> Vec<String> {
        range.map(|i| format!("r{i}")).collect()
    }

    /// The worst case (polls open at shutdown, acknowledgements that never come, a slow release)
    /// ends 13 s after shutdown, under the 15 s graceful shutdown limit
    /// (`VECTOR_GRACEFUL_SHUTDOWN_LIMIT_SECS` in nanosiem's `pull-container.js`): the polls end
    /// within 5 s, concurrently with the acknowledgement wait, and the release starts by 8 s.
    #[test]
    fn shutdown_fits_the_graceful_shutdown_limit() {
        assert_eq!(SHUTDOWN_ACK_WAIT, Duration::from_secs(8));
        let worst_case = SHUTDOWN_ACK_WAIT + RELEASE_WAIT;
        assert_eq!(worst_case, Duration::from_secs(13));
        assert!(worst_case + Duration::from_secs(2) <= GRACEFUL_SHUTDOWN_LIMIT);
        // Polls open at shutdown end before the acknowledgement wait does.
        assert!(Duration::from_secs(POLL_WAIT_CAP_SECS.into()) < SHUTDOWN_ACK_WAIT);
    }

    #[test]
    fn nothing_is_tracked_once_taken() {
        let in_flight = InFlight::default();
        assert!(in_flight.insert(&receipts(0..2)));
        assert_eq!(in_flight.take().len(), 2);
        assert!(!in_flight.insert(&receipts(2..4)));
        assert!(in_flight.take().is_empty());
    }

    #[test]
    fn in_flight_tracks_only_unsettled_receipts() {
        let in_flight = InFlight::default();
        assert!(in_flight.insert(&receipts(0..25)));
        // Acknowledged (deleted) receipts leave before the delete is sent.
        in_flight.remove(&receipts(0..10));
        assert_eq!(in_flight.len(), 15);
        let mut taken = in_flight.take();
        taken.sort();
        let mut expected = receipts(10..25);
        expected.sort();
        assert_eq!(taken, expected);
        assert!(in_flight.take().is_empty());
    }

    #[tokio::test]
    async fn releases_each_receipt_once_in_batches_of_ten() {
        let releaser = MockRelease::default();
        release(
            "aws_sqs",
            &releaser,
            receipts(0..23),
            false,
            Instant::now() + RELEASE_WAIT,
        )
        .await;
        let calls = releaser.calls.into_inner().unwrap();
        assert_eq!(
            calls.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![10, 10, 3]
        );
        let all: Vec<String> = calls.into_iter().flatten().collect();
        assert_eq!(all, receipts(0..23));
    }

    #[tokio::test]
    async fn a_failed_batch_does_not_stop_the_others() {
        let releaser = MockRelease {
            fail_batch: Some(0),
            ..Default::default()
        };
        release(
            "aws_sqs",
            &releaser,
            receipts(0..15),
            false,
            Instant::now() + RELEASE_WAIT,
        )
        .await;
        assert_eq!(releaser.calls.into_inner().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn nothing_to_release_sends_nothing() {
        let releaser = MockRelease::default();
        release(
            "aws_sqs",
            &releaser,
            Vec::new(),
            false,
            Instant::now() + RELEASE_WAIT,
        )
        .await;
        assert!(releaser.calls.into_inner().unwrap().is_empty());
    }
}
