//! NAN-5208: a written record's finalizers may resolve only once its bytes are in the data file.
//!
//! Sources treat a `Delivered` batch status as the durable handoff: `splunk_hec` flips the
//! indexer ackId to true, `http_server` answers 2xx. The writer used to resolve finalizers as
//! soon as the record was appended to its in-process write buffer, before any `write(2)` reached
//! the data file. A process kill (SIGKILL, OOM, node loss) between that point and the next flush
//! lost a record the sender had already been told was durable.

use std::time::Duration;

use vector_common::finalization::{AddBatchNotifier, BatchNotifier, BatchStatus};

use super::create_default_buffer_v2;
use crate::test::{SizedRecord, with_temp_dir};

fn data_file_len(path: &std::path::Path) -> u64 {
    std::fs::metadata(path).map_or(0, |m| m.len())
}

#[tokio::test]
async fn finalizers_resolve_only_after_the_record_reaches_the_data_file() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let (mut writer, _reader, ledger) =
                create_default_buffer_v2::<_, SizedRecord>(data_dir).await;
            let data_file = ledger.get_current_writer_data_file_path();

            for index in 0..8u32 {
                let (batch, mut receiver) = BatchNotifier::new_with_receiver();
                // Small records sit entirely inside the writer's internal buffer.
                let mut record = SizedRecord::new(64 + index);
                record.add_batch_notifier(batch);
                let before = data_file_len(&data_file);

                writer
                    .write_record(record)
                    .await
                    .expect("write should not fail");

                // Let the notifier task run so a resolved status is observable.
                tokio::time::sleep(Duration::from_millis(5)).await;
                if let Ok(status) = receiver.try_recv() {
                    // Sync std read of the file: whatever is here survives a process kill.
                    assert!(
                        data_file_len(&data_file) > before,
                        "record {index} was finalized as {status:?} while its bytes were not yet \
                         in the data file ({} bytes on disk, {before} before the write)",
                        data_file_len(&data_file),
                    );
                    continue;
                }

                // The caller's flush (the topology fanout flushes after every send) writes the
                // record out, and only then may it be acknowledged.
                writer.flush().await.expect("flush should not fail");
                tokio::time::sleep(Duration::from_millis(5)).await;
                let status = receiver
                    .try_recv()
                    .unwrap_or_else(|_| panic!("record {index} was flushed but never finalized"));
                assert_eq!(status, BatchStatus::Delivered);
                assert!(
                    data_file_len(&data_file) > before,
                    "record {index} was finalized after flush but is not in the data file"
                );
            }
        }
    })
    .await;
}

#[tokio::test]
async fn a_record_never_flushed_is_not_acknowledged_as_delivered() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let (mut writer, _reader, _ledger) =
                create_default_buffer_v2::<_, SizedRecord>(data_dir).await;
            let (batch, receiver) = BatchNotifier::new_with_receiver();
            let mut record = SizedRecord::new(64);
            record.add_batch_notifier(batch);
            writer
                .write_record(record)
                .await
                .expect("write should not fail");

            // The process stops before the record left the write buffer: the source must not
            // tell its sender the record was durably accepted.
            drop(writer);
            let status = tokio::time::timeout(Duration::from_secs(5), receiver)
                .await
                .expect("dropping the writer should resolve the pending finalizers");
            assert_ne!(status, BatchStatus::Delivered);
        }
    })
    .await;
}
