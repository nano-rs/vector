//! NAN-5285: an acknowledged record survives a host crash, not just a process kill.
//!
//! NAN-5208 made the writer acknowledge a record only once its bytes reached the data file, i.e.
//! the kernel page cache. That survives a process kill, but a host crash (power loss, kernel panic,
//! hard reset) discards whatever was not yet `fsync`ed, and the data file was synced only every
//! `flush_interval` (500 ms). The v2.0.13 HA drill lost exactly such a record: an OTLP request got
//! its 2xx and the ingest host was hard-reset about 370 ms later.
//!
//! These tests model the page cache: the filesystem remembers how many bytes of each data file
//! were present at its last `sync_all`, and a "crash" truncates every data file to that length.

use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use vector_common::finalization::{AddBatchNotifier, BatchNotifier, BatchStatus};

use crate::{
    AcknowledgementDurability,
    buffer_usage_data::BufferUsageHandle,
    test::{SizedRecord, with_temp_dir},
    variants::disk_v2::{
        Buffer, DiskBufferConfigBuilder,
        io::{
            AsyncFile, DataSync, Filesystem, Metadata, ProductionFilesystem, ReadableMemoryMap,
            WritableMemoryMap,
        },
    },
};

/// Bytes of each file that a `sync_all` has made durable.
#[derive(Clone, Debug, Default)]
struct PageCacheFs {
    synced: Arc<Mutex<HashMap<PathBuf, u64>>>,
    syncs: Arc<Mutex<u64>>,
    /// Offer the group syncer an off-task sync handle (the production path); `false` exercises
    /// the inline fallback for files without one.
    off_task_sync: bool,
    /// Make every off-task sync fail.
    fail_syncs: Arc<std::sync::atomic::AtomicBool>,
    /// The ledger's bytes as of its last `msync` (or its open).
    mmap_synced: Arc<Mutex<HashMap<PathBuf, Vec<u8>>>>,
    /// How many times the ledger was msynced.
    ledger_flushes: Arc<Mutex<u64>>,
}

/// The ledger's writable mapping, remembering what each `flush` (msync) made durable.
struct PageCacheMmap {
    inner: memmap2::MmapMut,
    path: PathBuf,
    fs: PageCacheFs,
}

impl AsRef<[u8]> for PageCacheMmap {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

impl AsMut<[u8]> for PageCacheMmap {
    fn as_mut(&mut self) -> &mut [u8] {
        self.inner.as_mut()
    }
}

impl ReadableMemoryMap for PageCacheMmap {}

impl WritableMemoryMap for PageCacheMmap {
    fn flush(&self) -> io::Result<()> {
        self.inner.flush()?;
        self.fs
            .mmap_synced
            .lock()
            .unwrap()
            .insert(self.path.clone(), self.inner.as_ref().to_vec());
        *self.fs.ledger_flushes.lock().unwrap() += 1;
        Ok(())
    }
}

impl PageCacheFs {
    fn group_synced() -> Self {
        Self {
            off_task_sync: true,
            ..Self::default()
        }
    }

    fn record_sync(&self, path: &Path) {
        let len = std::fs::metadata(path).map_or(0, |m| m.len());
        self.synced.lock().unwrap().insert(path.to_path_buf(), len);
        *self.syncs.lock().unwrap() += 1;
    }

    fn sync_count(&self) -> u64 {
        *self.syncs.lock().unwrap()
    }

    /// Simulates a host crash: every data file loses the bytes written since its last sync.
    fn crash(&self, data_dir: &Path) {
        let synced = self.synced.lock().unwrap();
        for entry in std::fs::read_dir(data_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) != Some("dat") {
                continue;
            }
            let durable = synced.get(&path).copied().unwrap_or(0);
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            file.set_len(durable).unwrap();
        }
        // The ledger loses every update since its last msync.
        for (path, bytes) in self.mmap_synced.lock().unwrap().iter() {
            std::fs::write(path, bytes).unwrap();
        }
    }
}

#[derive(Debug)]
struct PageCacheFile {
    inner: tokio::fs::File,
    path: PathBuf,
    fs: PageCacheFs,
}

impl AsyncRead for PageCacheFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for PageCacheFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl AsyncFile for PageCacheFile {
    async fn metadata(&self) -> io::Result<Metadata> {
        AsyncFile::metadata(&self.inner).await
    }

    async fn truncate(&self, size: u64) -> io::Result<()> {
        AsyncFile::truncate(&self.inner, size).await
    }

    async fn sync_all(&self) -> io::Result<()> {
        AsyncFile::sync_all(&self.inner).await?;
        self.fs.record_sync(&self.path);
        Ok(())
    }

    /// The group syncer's off-task sync records what it made durable, like `sync_all`.
    async fn data_sync(&self) -> io::Result<Option<DataSync>> {
        if !self.fs.off_task_sync {
            return Ok(None);
        }
        let file = self.inner.try_clone().await?.into_std().await;
        let (fs, path) = (self.fs.clone(), self.path.clone());
        Ok(Some(DataSync::new(move || {
            if fs.fail_syncs.load(std::sync::atomic::Ordering::Acquire) {
                return Err(io::Error::other("injected sync failure"));
            }
            file.sync_data()?;
            fs.record_sync(&path);
            Ok(())
        })))
    }
}

impl PageCacheFs {
    fn wrap(&self, inner: tokio::fs::File, path: &Path) -> PageCacheFile {
        PageCacheFile {
            inner,
            path: path.to_path_buf(),
            fs: self.clone(),
        }
    }
}

impl Filesystem for PageCacheFs {
    type File = PageCacheFile;
    type MemoryMap = <ProductionFilesystem as Filesystem>::MemoryMap;
    type MutableMemoryMap = PageCacheMmap;

    async fn open_file_writable(&self, path: &Path) -> io::Result<PageCacheFile> {
        let inner = ProductionFilesystem.open_file_writable(path).await?;
        Ok(self.wrap(inner, path))
    }

    async fn open_file_writable_atomic(&self, path: &Path) -> io::Result<PageCacheFile> {
        let inner = ProductionFilesystem.open_file_writable_atomic(path).await?;
        Ok(self.wrap(inner, path))
    }

    async fn open_file_readable(&self, path: &Path) -> io::Result<PageCacheFile> {
        let inner = ProductionFilesystem.open_file_readable(path).await?;
        Ok(self.wrap(inner, path))
    }

    async fn open_mmap_readable(&self, path: &Path) -> io::Result<Self::MemoryMap> {
        ProductionFilesystem.open_mmap_readable(path).await
    }

    async fn open_mmap_writable(&self, path: &Path) -> io::Result<Self::MutableMemoryMap> {
        let inner = ProductionFilesystem.open_mmap_writable(path).await?;
        self.mmap_synced
            .lock()
            .unwrap()
            .entry(path.to_path_buf())
            .or_insert_with(|| inner.as_ref().to_vec());
        Ok(PageCacheMmap {
            inner,
            path: path.to_path_buf(),
            fs: self.clone(),
        })
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

async fn acknowledged_status(status: vector_common::finalization::BatchStatusReceiver) -> BatchStatus {
    tokio::time::timeout(std::time::Duration::from_secs(5), status)
        .await
        .expect("the flushed record should be acknowledged")
}

#[tokio::test]
async fn an_acknowledged_record_survives_a_host_crash() {
    for fs in [PageCacheFs::group_synced(), PageCacheFs::default()] {
        acknowledged_record_survives_a_host_crash(fs).await;
    }
}

async fn acknowledged_record_survives_a_host_crash(fs: PageCacheFs) {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let config = DiskBufferConfigBuilder::from_path(data_dir.clone())
                .filesystem(fs.clone())
                .build()
                .expect("creating buffer should not fail");
            let (mut writer, reader, ledger) =
                Buffer::from_config_inner(config, BufferUsageHandle::noop())
                    .await
                    .expect("should not fail to create buffer");

            let (batch, status) = BatchNotifier::new_with_receiver();
            let mut record = SizedRecord::new(64);
            record.add_batch_notifier(batch);
            writer
                .write_record(record)
                .await
                .expect("write should not fail");
            writer.flush().await.expect("flush should not fail");
            assert_eq!(acknowledged_status(status).await, BatchStatus::Delivered);

            // The sender now holds a 2xx. Pull the plug.
            drop(writer);
            drop(reader);
            drop(ledger);
            fs.crash(&data_dir);

            let config = DiskBufferConfigBuilder::from_path(data_dir)
                .build()
                .expect("creating buffer should not fail");
            let (_writer, mut reader, _ledger) =
                Buffer::<SizedRecord>::from_config_inner(config, BufferUsageHandle::noop())
                    .await
                    .expect("the buffer should reopen after the crash");
            let recovered = tokio::time::timeout(std::time::Duration::from_secs(5), reader.next())
                .await
                .expect("the acknowledged record was lost by the host crash (the recovered buffer is empty)")
                .expect("reading should not fail");
            assert_eq!(
                recovered,
                Some(SizedRecord::new(64)),
                "a record acknowledged as Delivered was lost by a host crash"
            );
        }
    })
    .await;
}

#[tokio::test]
async fn records_no_source_waits_on_keep_the_periodic_sync() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let fs = PageCacheFs::group_synced();
            let config = DiskBufferConfigBuilder::from_path(data_dir)
                .filesystem(fs.clone())
                .build()
                .expect("creating buffer should not fail");
            let (mut writer, _reader, _ledger) =
                Buffer::from_config_inner(config, BufferUsageHandle::noop())
                    .await
                    .expect("should not fail to create buffer");

            // Opening the first data file syncs it once.
            writer
                .write_record(SizedRecord::new(64))
                .await
                .expect("write should not fail");
            writer.flush().await.expect("flush should not fail");
            let after_open = fs.sync_count();

            for _ in 0..16 {
                writer
                    .write_record(SizedRecord::new(64))
                    .await
                    .expect("write should not fail");
                writer.flush().await.expect("flush should not fail");
            }
            assert_eq!(
                fs.sync_count(),
                after_open,
                "records without finalizers must not pay an fsync per flush"
            );

            // One acknowledged record among many costs exactly one sync: group commit.
            let (batch, status) = BatchNotifier::new_with_receiver();
            let mut tracked = SizedRecord::new(64);
            tracked.add_batch_notifier(batch);
            for _ in 0..4 {
                writer
                    .write_record(SizedRecord::new(64))
                    .await
                    .expect("write should not fail");
            }
            writer
                .write_record(tracked)
                .await
                .expect("write should not fail");
            writer.flush().await.expect("flush should not fail");
            assert_eq!(acknowledged_status(status).await, BatchStatus::Delivered);
            assert_eq!(fs.sync_count(), after_open + 1);
        }
    })
    .await;
}

/// Group commit across flushes: while one sync runs, later flushes queue their records and the
/// writer keeps going, so many flushes share few syncs instead of one sync each.
#[tokio::test]
async fn concurrent_flushes_share_syncs() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let fs = PageCacheFs::group_synced();
            let config = DiskBufferConfigBuilder::from_path(data_dir)
                .filesystem(fs.clone())
                .build()
                .expect("creating buffer should not fail");
            let (mut writer, _reader, _ledger) =
                Buffer::from_config_inner(config, BufferUsageHandle::noop())
                    .await
                    .expect("should not fail to create buffer");
            let before = fs.sync_count();
            let mut statuses = Vec::new();
            for _ in 0..200 {
                let (batch, status) = BatchNotifier::new_with_receiver();
                let mut record = SizedRecord::new(64);
                record.add_batch_notifier(batch);
                writer
                    .write_record(record)
                    .await
                    .expect("write should not fail");
                // The flush returns without waiting for the sync.
                writer.flush().await.expect("flush should not fail");
                statuses.push(status);
            }
            for status in statuses {
                assert_eq!(acknowledged_status(status).await, BatchStatus::Delivered);
            }
            let syncs = fs.sync_count() - before;
            assert!(
                syncs < 200,
                "200 acknowledged flushes took {syncs} syncs: no group commit"
            );
        }
    })
    .await;
}

/// A failed sync never acknowledges, and the writer refuses to carry on as if it had succeeded.
#[tokio::test]
async fn a_failed_group_sync_never_acknowledges() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let fs = PageCacheFs::group_synced();
            let config = DiskBufferConfigBuilder::from_path(data_dir)
                .filesystem(fs.clone())
                .build()
                .expect("creating buffer should not fail");
            let (mut writer, _reader, _ledger) =
                Buffer::from_config_inner(config, BufferUsageHandle::noop())
                    .await
                    .expect("should not fail to create buffer");
            writer
                .write_record(SizedRecord::new(64))
                .await
                .expect("write should not fail");
            writer.flush().await.expect("flush should not fail");
            fs.fail_syncs.store(true, std::sync::atomic::Ordering::Release);

            let (batch, status) = BatchNotifier::new_with_receiver();
            let mut record = SizedRecord::new(64);
            record.add_batch_notifier(batch);
            writer
                .write_record(record)
                .await
                .expect("write should not fail");
            writer.flush().await.expect("flush should not fail");
            assert_eq!(acknowledged_status(status).await, BatchStatus::Errored);
            assert!(
                writer.flush().await.is_err(),
                "after a failed sync the writer must not report success"
            );
        }
    })
    .await;
}

/// Acknowledged records that span a move to a new data file survive a crash too: the ledger
/// (which names the writer's current file) is synced when the writer moves on.
#[tokio::test]
async fn acknowledged_records_across_data_files_survive_a_host_crash() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let fs = PageCacheFs::group_synced();
            let config = DiskBufferConfigBuilder::from_path(data_dir.clone())
                .filesystem(fs.clone())
                .max_data_file_size(256)
                .max_record_size(192)
                .build()
                .expect("creating buffer should not fail");
            let (mut writer, reader, ledger) =
                Buffer::from_config_inner(config, BufferUsageHandle::noop())
                    .await
                    .expect("should not fail to create buffer");
            let sizes = [96u32, 97, 98, 99, 100];
            for size in sizes {
                let (batch, status) = BatchNotifier::new_with_receiver();
                let mut record = SizedRecord::new(size);
                record.add_batch_notifier(batch);
                writer
                    .write_record(record)
                    .await
                    .expect("write should not fail");
                writer.flush().await.expect("flush should not fail");
                assert_eq!(acknowledged_status(status).await, BatchStatus::Delivered);
            }
            let data_files = std::fs::read_dir(&data_dir)
                .unwrap()
                .filter(|e| {
                    e.as_ref().unwrap().path().extension().and_then(|x| x.to_str()) == Some("dat")
                })
                .count();
            assert!(data_files > 1, "the records should span data files ({data_files})");

            drop(writer);
            drop(reader);
            drop(ledger);
            fs.crash(&data_dir);

            let config = DiskBufferConfigBuilder::from_path(data_dir)
                .max_data_file_size(256)
                .max_record_size(192)
                .build()
                .expect("creating buffer should not fail");
            let (_writer, mut reader, _ledger) =
                Buffer::<SizedRecord>::from_config_inner(config, BufferUsageHandle::noop())
                    .await
                    .expect("the buffer should reopen after the crash");
            for size in sizes {
                let recovered =
                    tokio::time::timeout(std::time::Duration::from_secs(5), reader.next())
                        .await
                        .unwrap_or_else(|_| panic!("record of size {size} lost by the crash"))
                        .expect("reading should not fail");
                assert_eq!(recovered, Some(SizedRecord::new(size)));
            }
        }
    })
    .await;
}

/// `acknowledgement_durability = "page_cache"` (the operator opt-out) acknowledges once the record
/// is in the data file, with no sync per flush: the pre-NAN-5285 behavior.
#[tokio::test]
async fn page_cache_durability_acknowledges_without_a_sync() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let fs = PageCacheFs::group_synced();
            let config = DiskBufferConfigBuilder::from_path(data_dir)
                .filesystem(fs.clone())
                .acknowledgement_durability(AcknowledgementDurability::PageCache)
                .build()
                .expect("creating buffer should not fail");
            let (mut writer, _reader, _ledger) =
                Buffer::from_config_inner(config, BufferUsageHandle::noop())
                    .await
                    .expect("should not fail to create buffer");
            writer
                .write_record(SizedRecord::new(64))
                .await
                .expect("write should not fail");
            writer.flush().await.expect("flush should not fail");
            let after_open = fs.sync_count();

            for _ in 0..16 {
                let (batch, mut status) = BatchNotifier::new_with_receiver();
                let mut record = SizedRecord::new(64);
                record.add_batch_notifier(batch);
                writer
                    .write_record(record)
                    .await
                    .expect("write should not fail");
                writer.flush().await.expect("flush should not fail");
                // Acknowledged by the flush itself, not by a later sync.
                assert_eq!(status.try_recv(), Ok(BatchStatus::Delivered));
            }
            assert_eq!(
                fs.sync_count(),
                after_open,
                "page_cache durability must not sync per acknowledged flush"
            );
        }
    })
    .await;
}

/// A steady stream of acknowledged records (every flush handed to the group syncer) must not
/// starve the ledger of its periodic `flush_interval` msync, which persists reader progress.
#[tokio::test]
async fn acknowledged_traffic_keeps_the_periodic_ledger_sync() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let fs = PageCacheFs::group_synced();
            let config = DiskBufferConfigBuilder::from_path(data_dir)
                .filesystem(fs.clone())
                .flush_interval(std::time::Duration::ZERO)
                .build()
                .expect("creating buffer should not fail");
            let (mut writer, _reader, _ledger) =
                Buffer::from_config_inner(config, BufferUsageHandle::noop())
                    .await
                    .expect("should not fail to create buffer");
            writer
                .write_record(SizedRecord::new(64))
                .await
                .expect("write should not fail");
            writer.flush().await.expect("flush should not fail");
            let before = *fs.ledger_flushes.lock().unwrap();

            let mut statuses = Vec::new();
            for _ in 0..8 {
                let (batch, status) = BatchNotifier::new_with_receiver();
                let mut record = SizedRecord::new(64);
                record.add_batch_notifier(batch);
                writer
                    .write_record(record)
                    .await
                    .expect("write should not fail");
                writer.flush().await.expect("flush should not fail");
                statuses.push(status);
            }
            for status in statuses {
                assert_eq!(acknowledged_status(status).await, BatchStatus::Delivered);
            }
            let ledger_flushes = *fs.ledger_flushes.lock().unwrap() - before;
            assert!(
                ledger_flushes >= 8,
                "the periodic ledger msync was skipped on acknowledged flushes ({ledger_flushes} of 8)"
            );
        }
    })
    .await;
}
