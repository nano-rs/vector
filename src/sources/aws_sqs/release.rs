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

use aws_sdk_sqs::{Client as SqsClient, types::ChangeMessageVisibilityBatchRequestEntry};

use crate::internal_events::SourceMessagesReleased;

/// The most entries `ChangeMessageVisibilityBatch` takes.
const MAX_BATCH_SIZE: usize = 10;

/// How long a shutting-down source waits for the acknowledgements of messages already sent
/// downstream before it releases them.
pub(crate) const SHUTDOWN_ACK_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_secs(1)
} else {
    Duration::from_secs(15)
};

/// How long releasing may take in total. Releasing is best effort: what is not released in time
/// becomes visible again when its visibility timeout expires.
pub(crate) const RELEASE_TIMEOUT: Duration = Duration::from_secs(5);

/// The receipt handles of messages that were received and not yet deleted (or handed to the
/// deletion path). Shared between the receiving tasks and the acknowledgement task.
#[derive(Clone, Debug, Default)]
pub(crate) struct InFlight(Arc<Mutex<HashSet<String>>>);

impl InFlight {
    pub(crate) fn insert<'a>(&self, receipts: impl IntoIterator<Item = &'a String>) {
        let mut set = self.0.lock().expect("in-flight lock poisoned");
        set.extend(receipts.into_iter().cloned());
    }

    /// Removes receipts that are about to be deleted (or otherwise settled). This must happen
    /// before the deletion is requested, so that a message is never both deleted and released.
    pub(crate) fn remove<'a>(&self, receipts: impl IntoIterator<Item = &'a String>) {
        let mut set = self.0.lock().expect("in-flight lock poisoned");
        for receipt in receipts {
            set.remove(receipt);
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.0.lock().expect("in-flight lock poisoned").len()
    }

    /// Takes every receipt still in flight; they will not be deleted by this source any more.
    pub(crate) fn take(&self) -> Vec<String> {
        let mut set = self.0.lock().expect("in-flight lock poisoned");
        set.drain().collect()
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

/// Releases `receipts` in batches, within [`RELEASE_TIMEOUT`], and logs the outcome. Each
/// receipt is sent at most once. `ack_wait_timed_out` says whether the source gave up waiting for
/// acknowledgements (reported in the log).
pub(crate) async fn release(
    component_type: &'static str,
    releaser: &dyn Release,
    receipts: Vec<String>,
    ack_wait_timed_out: bool,
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
    let _ = tokio::time::timeout(RELEASE_TIMEOUT, work).await;
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

    #[test]
    fn in_flight_tracks_only_unsettled_receipts() {
        let in_flight = InFlight::default();
        in_flight.insert(&receipts(0..25));
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
        release("aws_sqs", &releaser, receipts(0..23), false).await;
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
        release("aws_sqs", &releaser, receipts(0..15), false).await;
        assert_eq!(releaser.calls.into_inner().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn nothing_to_release_sends_nothing() {
        let releaser = MockRelease::default();
        release("aws_sqs", &releaser, Vec::new(), false).await;
        assert!(releaser.calls.into_inner().unwrap().is_empty());
    }
}
