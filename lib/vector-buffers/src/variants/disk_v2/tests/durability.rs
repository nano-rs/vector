//! NAN-5208: a written record's finalizers may resolve only once its bytes are in the data file.
//!
//! Sources treat a `Delivered` batch status as the durable handoff: `splunk_hec` flips the
//! indexer ackId to true, `http_server` answers 2xx. The writer used to resolve finalizers as
//! soon as the record was appended to its in-process write buffer, before any `write(2)` reached
//! the data file, so a kill of the Vector process (SIGKILL, OOM kill, container kill) between that
//! point and the next flush lost a record the sender had already been told was durable.
//!
//! The guarantee these tests pin is process-death durability: an acknowledged record has been
//! written into the data file (the kernel page cache). It is not a host-crash guarantee: the
//! data file is still `fsync`ed only every `flush_interval` (500 ms by default), so a power loss
//! or kernel crash can lose up to that window of acknowledged records.

use std::{
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use vector_common::finalization::{
    AddBatchNotifier, BatchNotifier, BatchStatus, BatchStatusReceiver,
};

use super::create_default_buffer_v2;
use crate::{
    buffer_usage_data::BufferUsageHandle,
    test::{SizedRecord, with_temp_dir},
    variants::disk_v2::{
        Buffer, DiskBufferConfigBuilder,
        io::{AsyncFile, Filesystem, Metadata, ProductionFilesystem},
    },
};

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

                // A finalizer resolves synchronously when it is dropped, so no yield is needed to
                // observe it. No sleep here or below: a sleep would let a write-behind file
                // complete its background write and hide a missing flush await.
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

/// A production filesystem whose data-file writes fail once when armed. Like
/// `tokio::fs::File`, a failed write is reported once and the following `flush` succeeds.
#[derive(Clone, Debug, Default)]
struct FailOnce {
    fail_next_write: Arc<AtomicBool>,
}

#[derive(Debug)]
struct FailOnceFile {
    inner: tokio::fs::File,
    fs: FailOnce,
}

impl AsyncRead for FailOnceFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for FailOnceFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.fs.fail_next_write.swap(false, Ordering::AcqRel) {
            return Poll::Ready(Err(io::Error::other("injected write failure")));
        }
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl AsyncFile for FailOnceFile {
    async fn metadata(&self) -> io::Result<Metadata> {
        AsyncFile::metadata(&self.inner).await
    }

    async fn truncate(&self, size: u64) -> io::Result<()> {
        AsyncFile::truncate(&self.inner, size).await
    }

    async fn sync_all(&self) -> io::Result<()> {
        AsyncFile::sync_all(&self.inner).await
    }
}

impl Filesystem for FailOnce {
    type File = FailOnceFile;
    type MemoryMap = <ProductionFilesystem as Filesystem>::MemoryMap;
    type MutableMemoryMap = <ProductionFilesystem as Filesystem>::MutableMemoryMap;

    async fn open_file_writable(&self, path: &Path) -> io::Result<FailOnceFile> {
        Ok(FailOnceFile {
            inner: ProductionFilesystem.open_file_writable(path).await?,
            fs: self.clone(),
        })
    }

    async fn open_file_writable_atomic(&self, path: &Path) -> io::Result<FailOnceFile> {
        Ok(FailOnceFile {
            inner: ProductionFilesystem.open_file_writable_atomic(path).await?,
            fs: self.clone(),
        })
    }

    async fn open_file_readable(&self, path: &Path) -> io::Result<FailOnceFile> {
        Ok(FailOnceFile {
            inner: ProductionFilesystem.open_file_readable(path).await?,
            fs: self.clone(),
        })
    }

    async fn open_mmap_readable(&self, path: &Path) -> io::Result<Self::MemoryMap> {
        ProductionFilesystem.open_mmap_readable(path).await
    }

    async fn open_mmap_writable(&self, path: &Path) -> io::Result<Self::MutableMemoryMap> {
        ProductionFilesystem.open_mmap_writable(path).await
    }

    async fn truncate_file(&self, path: &Path, size: u64) -> io::Result<()> {
        ProductionFilesystem.truncate_file(path, size).await
    }

    async fn delete_file(&self, path: &Path) -> io::Result<()> {
        ProductionFilesystem.delete_file(path).await
    }

    async fn list_files(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        ProductionFilesystem.list_files(path).await
    }
}

fn notified(size: u32) -> (SizedRecord, BatchStatusReceiver) {
    let (batch, receiver) = BatchNotifier::new_with_receiver();
    let mut record = SizedRecord::new(size);
    record.add_batch_notifier(batch);
    (record, receiver)
}

#[tokio::test]
async fn a_failed_implicit_flush_never_acknowledges_the_records_it_discarded() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let fs = FailOnce::default();
            let config = DiskBufferConfigBuilder::from_path(data_dir)
                .filesystem(fs.clone())
                .write_buffer_size(4096)
                .build()
                .expect("creating buffer should not fail");
            let (mut writer, _reader, _ledger) =
                Buffer::from_config_inner(config, BufferUsageHandle::noop())
                    .await
                    .expect("should not fail to create buffer");

            // A small record waits in the write buffer, unacknowledged.
            let (buffered, mut buffered_status) = notified(64);
            writer
                .write_record(buffered)
                .await
                .expect("write should not fail");
            assert!(buffered_status.try_recv().is_err());

            // The next record overflows the write buffer, so writing it first flushes the buffered
            // one; that write fails and the buffered bytes are gone.
            fs.fail_next_write.store(true, Ordering::Release);
            let (overflow, mut overflow_status) = notified(5000);
            assert!(writer.write_record(overflow).await.is_err());
            assert_eq!(overflow_status.try_recv(), Ok(BatchStatus::Errored));
            assert_eq!(
                buffered_status.try_recv(),
                Ok(BatchStatus::Errored),
                "a record discarded by a failed flush must be resolved Errored, never Delivered"
            );

            // The file reports the failure only once (as tokio's File does), but the writer stays
            // poisoned: a later flush or write must not succeed and acknowledge anything.
            assert!(writer.flush().await.is_err());
            let (later, mut later_status) = notified(64);
            assert!(writer.write_record(later).await.is_err());
            assert_eq!(later_status.try_recv(), Ok(BatchStatus::Errored));
        }
    })
    .await;
}
