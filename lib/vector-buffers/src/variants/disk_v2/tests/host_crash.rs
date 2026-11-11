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
    buffer_usage_data::BufferUsageHandle,
    test::{SizedRecord, with_temp_dir},
    variants::disk_v2::{
        Buffer, DiskBufferConfigBuilder,
        io::{AsyncFile, Filesystem, Metadata, ProductionFilesystem},
    },
};

/// Bytes of each file that a `sync_all` has made durable.
#[derive(Clone, Debug, Default)]
struct PageCacheFs {
    synced: Arc<Mutex<HashMap<PathBuf, u64>>>,
    syncs: Arc<Mutex<u64>>,
}

impl PageCacheFs {
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
        let len = std::fs::metadata(&self.path).map_or(0, |m| m.len());
        self.fs.synced.lock().unwrap().insert(self.path.clone(), len);
        *self.fs.syncs.lock().unwrap() += 1;
        Ok(())
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
    type MutableMemoryMap = <ProductionFilesystem as Filesystem>::MutableMemoryMap;

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

#[tokio::test]
async fn an_acknowledged_record_survives_a_host_crash() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let fs = PageCacheFs::default();
            let config = DiskBufferConfigBuilder::from_path(data_dir.clone())
                .filesystem(fs.clone())
                .build()
                .expect("creating buffer should not fail");
            let (mut writer, reader, ledger) =
                Buffer::from_config_inner(config, BufferUsageHandle::noop())
                    .await
                    .expect("should not fail to create buffer");

            let (batch, mut status) = BatchNotifier::new_with_receiver();
            let mut record = SizedRecord::new(64);
            record.add_batch_notifier(batch);
            writer
                .write_record(record)
                .await
                .expect("write should not fail");
            writer.flush().await.expect("flush should not fail");
            assert_eq!(
                status.try_recv(),
                Ok(BatchStatus::Delivered),
                "the flushed record should be acknowledged"
            );

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
            let fs = PageCacheFs::default();
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
            let (batch, mut status) = BatchNotifier::new_with_receiver();
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
            assert_eq!(status.try_recv(), Ok(BatchStatus::Delivered));
            assert_eq!(fs.sync_count(), after_open + 1);
        }
    })
    .await;
}
