use std::{
    path::Path,
    sync::{Arc, Weak},
    time::Duration,
};

use tokio_test::{assert_pending, assert_ready, task::spawn};
use vector_common::finalization::{BatchNotifier, EventFinalizer, EventStatus, Finalizable};

use crate::{
    buffer_usage_data::BufferUsageHandle,
    test::{SizedRecord, with_temp_dir},
    variants::disk_v2::{
        DiskBufferConfigBuilder,
        ledger::Ledger,
        reader::ReaderError,
        tests::{FilesystemUnderTest, create_default_buffer_v2},
    },
};

pub(crate) async fn acknowledge(batch: BatchNotifier) {
    let finalizer = EventFinalizer::new(batch);
    finalizer.update_status(EventStatus::Delivered);
    drop(finalizer); // This sends the status update
    tokio::task::yield_now().await;
}

#[tokio::test]
async fn ack_updates_ledger_correctly() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            // Create a standalone ledger.
            let usage_handle = BufferUsageHandle::noop();
            let config = DiskBufferConfigBuilder::from_path(data_dir)
                .build()
                .expect("creating buffer should not fail");
            let ledger = Ledger::load_or_create(config, usage_handle)
                .await
                .expect("ledger should not fail to load/create");
            assert_eq!(ledger.consume_pending_acks(), 0);

            // Create our ledger, and make sure it's empty.
            let ledger = Arc::new(ledger);
            let finalizer = Arc::clone(&ledger).spawn_finalizer();
            assert_eq!(ledger.consume_pending_acks(), 0);

            // Now make sure it updates pending acks.
            let (batch, receiver) = BatchNotifier::new_with_receiver();
            finalizer.add(42, receiver);
            acknowledge(batch).await;
            assert_eq!(ledger.consume_pending_acks(), 42);
            assert_eq!(ledger.consume_pending_acks(), 0);
        }
    })
    .await;
}

#[tokio::test]
async fn ack_wakes_reader() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            // Create a standalone ledger.
            let usage_handle = BufferUsageHandle::noop();
            let config = DiskBufferConfigBuilder::from_path(data_dir)
                .build()
                .expect("creating buffer should not fail");
            let ledger = Ledger::load_or_create(config, usage_handle)
                .await
                .expect("ledger should not fail to load/create");

            // Create our ledger, as well as a future for awaiting
            // writer progress, and make sure it's not yet woken up.
            let ledger = Arc::new(ledger);
            let finalizer = Arc::clone(&ledger).spawn_finalizer();

            let mut wait_for_writer = spawn(ledger.wait_for_writer());
            assert_pending!(wait_for_writer.poll());
            assert!(!wait_for_writer.is_woken());

            // Now fire off an acknowledgement, and make sure our call woke up and can complete.
            let (batch, receiver) = BatchNotifier::new_with_receiver();
            finalizer.add(1, receiver);
            acknowledge(batch).await;

            assert!(wait_for_writer.is_woken());
            assert_ready!(wait_for_writer.poll());
        }
    })
    .await;
}

/// Finalizes a record read from the buffer with the given status.
async fn finalize(mut record: SizedRecord, status: EventStatus) {
    record.take_finalizers().update_status(status);
    drop(record);
    tokio::task::yield_now().await;
}

/// Waits until every handle to a dropped buffer's ledger is gone (the background finalizer task
/// holds one until its stream ends), so the ledger lock is free to reopen the buffer.
async fn wait_for_ledger_release(ledger: Weak<Ledger<FilesystemUnderTest>>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while ledger.strong_count() > 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("ledger should be released after the buffer is dropped");
}

/// Writes three single-event records (sizes 64, 65, 66), reads them, and finalizes them with
/// the given statuses. Returns the error of the next read and whether the ledger latched a hold.
async fn write_read_and_finalize(
    data_dir: &Path,
    statuses: [EventStatus; 3],
    hold_errored: bool,
) -> (Option<ReaderError<SizedRecord>>, bool) {
    let (mut writer, mut reader, ledger) =
        create_default_buffer_v2::<_, SizedRecord>(data_dir).await;
    if hold_errored {
        ledger.enable_errored_hold();
    }
    for size in [64, 65, 66] {
        writer
            .write_record(SizedRecord::new(size))
            .await
            .expect("write should not fail");
    }
    writer.flush().await.expect("flush should not fail");

    let mut records = Vec::new();
    for _ in 0..3 {
        records.push(
            reader
                .next()
                .await
                .expect("read should not fail")
                .expect("record should be present"),
        );
    }
    for (record, status) in records.into_iter().zip(statuses) {
        finalize(record, status).await;
    }

    // The next read applies the pending acknowledgements. With nothing left to read and the
    // writer still open it would wait forever, so close the writer first.
    writer.close();
    let next = tokio::time::timeout(Duration::from_secs(5), reader.next())
        .await
        .expect("read should not hang");
    let held = ledger.is_ack_held();
    let error = next.err();

    ledger.flush().expect("ledger flush should not fail");
    let weak = Arc::downgrade(&ledger);
    drop(reader);
    drop(writer);
    drop(ledger);
    wait_for_ledger_release(weak).await;

    (error, held)
}

/// NAN-5567: a record finalized as `Errored` must not be acknowledged. The durable read
/// position stays before it, the reader stops, and the record is the first one read after the
/// buffer is reopened.
#[tokio::test]
async fn errored_record_survives_reader_restart() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let (error, held) = write_read_and_finalize(
                &data_dir,
                [
                    EventStatus::Delivered,
                    EventStatus::Errored,
                    EventStatus::Delivered,
                ],
                true,
            )
            .await;
            assert!(held, "an errored record must latch the acknowledgement hold");
            assert_eq!(error, Some(ReaderError::UndeliveredRecord));
            assert!(
                ReaderError::<SizedRecord>::UndeliveredRecord
                    .as_recoverable_error()
                    .is_none(),
                "an undelivered record must stop the sink, not be skipped"
            );

            let (_writer, mut reader, ledger) =
                create_default_buffer_v2::<_, SizedRecord>(&data_dir).await;
            assert!(!ledger.is_ack_held(), "the hold is runtime-only");
            let first = tokio::time::timeout(Duration::from_secs(5), reader.next())
                .await
                .expect("read should not hang")
                .expect("read should not fail")
                .expect("the errored record should be replayed");
            assert_eq!(first, SizedRecord::new(65));
            let second = tokio::time::timeout(Duration::from_secs(5), reader.next())
                .await
                .expect("read should not hang")
                .expect("read should not fail")
                .expect("records after the errored one should be replayed");
            assert_eq!(second, SizedRecord::new(66));
        }
    })
    .await;
}

/// NAN-5567: a `Rejected` record is a permanent failure the sink has already handled (written
/// to its dead-letter store); replaying it would fail forever, so it is acknowledged.
#[tokio::test]
async fn rejected_record_is_acknowledged() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let (error, held) = write_read_and_finalize(
                &data_dir,
                [
                    EventStatus::Delivered,
                    EventStatus::Rejected,
                    EventStatus::Delivered,
                ],
                true,
            )
            .await;
            assert!(!held);
            assert_eq!(error, None);

            let (mut writer, mut reader, _ledger) =
                create_default_buffer_v2::<_, SizedRecord>(&data_dir).await;
            writer.close();
            let next = tokio::time::timeout(Duration::from_secs(5), reader.next())
                .await
                .expect("read should not hang")
                .expect("read should not fail");
            assert_eq!(next, None, "every record was acknowledged");
        }
    })
    .await;
}

#[tokio::test]
async fn errored_status_holds_later_acknowledgements() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let config = DiskBufferConfigBuilder::from_path(data_dir)
                .build()
                .expect("creating buffer should not fail");
            let ledger = Ledger::load_or_create(config, BufferUsageHandle::noop())
                .await
                .expect("ledger should not fail to load/create");
            let ledger = Arc::new(ledger);
            ledger.enable_errored_hold();
            let finalizer = Arc::clone(&ledger).spawn_finalizer();

            let mut batches = Vec::new();
            for amount in [1, 2, 4] {
                let (batch, receiver) = BatchNotifier::new_with_receiver();
                finalizer.add(amount, receiver);
                batches.push(batch);
            }
            let statuses = [
                EventStatus::Delivered,
                EventStatus::Errored,
                EventStatus::Delivered,
            ];
            for (batch, status) in batches.into_iter().zip(statuses) {
                let finalizer = EventFinalizer::new(batch);
                finalizer.update_status(status);
                drop(finalizer);
                tokio::task::yield_now().await;
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                while !ledger.is_ack_held() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the errored batch should latch the hold");
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            assert_eq!(
                ledger.consume_pending_acks(),
                1,
                "only the record before the errored one may be acknowledged"
            );
        }
    })
    .await;
}

/// NAN-5567: without the sink's opt-in, `Errored` is acknowledged as upstream does, so a sink that
/// reports `Errored` for deterministic failures cannot crash-loop on replay.
#[tokio::test]
async fn errored_record_is_acknowledged_without_opt_in() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let (error, held) = write_read_and_finalize(
                &data_dir,
                [
                    EventStatus::Delivered,
                    EventStatus::Errored,
                    EventStatus::Delivered,
                ],
                false,
            )
            .await;
            assert!(!held);
            assert_eq!(error, None);
        }
    })
    .await;
}

/// Copies a buffer's data directory as it is on disk right now: what a restarted process finds
/// after the old one was killed (`SIGKILL`) without finalizing anything.
fn snapshot_data_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("snapshot dir");
    for entry in std::fs::read_dir(from).expect("read buffer dir") {
        let entry = entry.expect("dir entry");
        if entry.file_type().expect("file type").is_file() {
            std::fs::copy(entry.path(), to.join(entry.file_name())).expect("copy buffer file");
        }
    }
}

/// Drives the reader (every record is already read, so `next` only applies pending
/// acknowledgements and then waits for the writer) until the durable read position reaches
/// `expected`, then a few more rounds so a position that would overshoot it gets the chance to.
async fn settle_acknowledgements(
    reader: &mut crate::variants::disk_v2::BufferReader<SizedRecord, FilesystemUnderTest>,
    ledger: &Ledger<FilesystemUnderTest>,
    expected: u64,
) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while ledger.state().get_last_reader_record_id() < expected {
        assert!(
            tokio::time::Instant::now() < deadline,
            "acknowledgements did not reach record {expected} (at {})",
            ledger.state().get_last_reader_record_id()
        );
        let next = tokio::time::timeout(Duration::from_millis(20), reader.next()).await;
        assert!(next.is_err(), "every record was already read; next must only wait");
    }
    for _ in 0..5 {
        let next = tokio::time::timeout(Duration::from_millis(20), reader.next()).await;
        assert!(next.is_err(), "every record was already read; next must only wait");
    }
    ledger.state().get_last_reader_record_id()
}

/// NAN-5885: records the sink has read but not finalized (a batch in flight, retrying against a
/// ClickHouse that is down) when Vector is killed must be the first records read after restart.
#[tokio::test]
async fn in_flight_records_are_replayed_after_a_kill() {
    with_temp_dir(|dir| {
        let data_dir = dir.join("live");
        let snapshot_dir = dir.join("after-kill");

        async move {
            let (mut writer, mut reader, ledger) =
                create_default_buffer_v2::<_, SizedRecord>(&data_dir).await;
            for size in [64, 65, 66, 67] {
                writer
                    .write_record(SizedRecord::new(size))
                    .await
                    .expect("write should not fail");
            }
            writer.flush().await.expect("flush should not fail");

            let mut read = Vec::new();
            for _ in 0..4 {
                read.push(
                    reader
                        .next()
                        .await
                        .expect("read should not fail")
                        .expect("record should be present"),
                );
            }
            let in_flight = read.split_off(1);
            // The first record was delivered; the rest are in flight.
            finalize(read.pop().unwrap(), EventStatus::Delivered).await;
            assert_eq!(settle_acknowledgements(&mut reader, &ledger, 1).await, 1);
            ledger.flush().expect("ledger flush should not fail");

            // SIGKILL: nothing in flight is ever finalized.
            snapshot_data_dir(&data_dir, &snapshot_dir);
            std::mem::forget(in_flight);

            let (_writer, mut reader, ledger) =
                create_default_buffer_v2::<_, SizedRecord>(&snapshot_dir).await;
            assert_eq!(ledger.state().get_last_reader_record_id(), 1);
            for size in [65, 66, 67] {
                let next = tokio::time::timeout(Duration::from_secs(5), reader.next())
                    .await
                    .expect("read should not hang")
                    .expect("read should not fail")
                    .expect("an in-flight record must be replayed");
                assert_eq!(next, SizedRecord::new(size));
            }
        }
    })
    .await;
}

/// NAN-5885: after a restart that replays in-flight records, acknowledging some of the replayed
/// records must advance the durable read position exactly to the last acknowledged record, never
/// past records that are still in flight. Before the fix, every record the startup seek re-read
/// left a spare acknowledgement behind (here 5), which acknowledged in-flight records.
#[tokio::test]
async fn acknowledgements_after_a_replay_never_pass_in_flight_records() {
    with_temp_dir(|dir| {
        let data_dir = dir.join("live");
        let snapshot_dir = dir.join("after-kill");

        async move {
            let (mut writer, mut reader, ledger) =
                create_default_buffer_v2::<_, SizedRecord>(&data_dir).await;
            for size in 64..74 {
                writer.write_record(SizedRecord::new(size)).await.expect("write");
            }
            writer.flush().await.expect("flush");
            let mut read = Vec::new();
            for _ in 0..10 {
                read.push(reader.next().await.expect("read").expect("record"));
            }
            // Records 1..=5 delivered, 6..=10 in flight.
            let in_flight = read.split_off(5);
            for record in read {
                finalize(record, EventStatus::Delivered).await;
            }
            assert_eq!(settle_acknowledgements(&mut reader, &ledger, 5).await, 5);
            ledger.flush().expect("ledger flush");
            snapshot_data_dir(&data_dir, &snapshot_dir);
            std::mem::forget(in_flight);

            // Restart: records 6..=10 (sizes 69..=73) are replayed, then four new ones.
            let (mut writer, mut reader, ledger) =
                create_default_buffer_v2::<_, SizedRecord>(&snapshot_dir).await;
            let before = ledger.state().get_last_reader_record_id();
            assert_eq!(before, 5);
            for size in 74..78 {
                writer.write_record(SizedRecord::new(size)).await.expect("write");
            }
            writer.flush().await.expect("flush");
            let mut replayed = Vec::new();
            for _ in 0..9 {
                replayed.push(
                    tokio::time::timeout(Duration::from_secs(5), reader.next())
                        .await
                        .expect("read should not hang")
                        .expect("read")
                        .expect("record"),
                );
            }
            assert_eq!(replayed[0], SizedRecord::new(69));
            // Deliver the first two replayed records only; the rest stay in flight.
            let rest = replayed.split_off(2);
            for record in replayed {
                finalize(record, EventStatus::Delivered).await;
            }
            let after = settle_acknowledgements(&mut reader, &ledger, before + 2).await;
            assert_eq!(after, before + 2, "only the two delivered records may be acknowledged");
            std::mem::forget(rest);
        }
    })
    .await;
}
