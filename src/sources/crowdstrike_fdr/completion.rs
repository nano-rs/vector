//! Which files of a batch are already delivered, so a re-delivered batch skips them.
//!
//! A file is recorded only once every one of its events is acknowledged by the sinks (or, with
//! acknowledgements off, once its events are handed to the pipeline). The record is kept in
//! memory and, when a data directory is available, in one append-only file per batch, so it
//! also survives a restart. It is removed once the batch's message is deleted. Records SQS can
//! no longer re-deliver (older than the 14-day maximum retention) are swept.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, SystemTime},
};

use sha2::Digest as _;
use tokio::io::AsyncWriteExt as _;

use super::manifest::Manifest;

/// The directory under the source's data dir that holds the records.
pub(super) const DIR_NAME: &str = "crowdstrike_fdr_batches";
const EXTENSION: &str = "done";
/// SQS keeps a message at most 14 days; a record older than that can never be needed again.
const MAX_AGE: Duration = Duration::from_hours(14 * 24);
const SWEEP_EVERY_SECS: i64 = 3600;

/// Delivered files per batch.
#[derive(Debug)]
pub(super) struct Completions {
    dir: Option<PathBuf>,
    memory: Mutex<HashMap<String, HashSet<String>>>,
    last_sweep_secs: AtomicI64,
}

/// A batch's identity: the same manifest re-delivered (or re-sent) maps to the same key.
pub(super) fn batch_key(manifest: &Manifest) -> String {
    let mut hasher = sha2::Sha256::new();
    for part in [
        manifest.bucket.as_str(),
        manifest.cid.as_str(),
        &manifest.timestamp_ms.to_string(),
    ] {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    for file in &manifest.files {
        hasher.update(file.path.as_bytes());
        hasher.update([0]);
    }
    hex::encode(hasher.finalize())
}

impl Completions {
    /// Records kept in this process only.
    pub(super) fn in_memory() -> Self {
        Self {
            dir: None,
            memory: Mutex::default(),
            last_sweep_secs: AtomicI64::new(0),
        }
    }

    /// Records persisted under `data_dir`; sweeps expired ones.
    pub(super) fn open(data_dir: &Path) -> std::io::Result<Self> {
        let dir = data_dir.join(DIR_NAME);
        std::fs::create_dir_all(&dir)?;
        let completions = Self {
            dir: Some(dir),
            memory: Mutex::default(),
            last_sweep_secs: AtomicI64::new(0),
        };
        completions.sweep(MAX_AGE);
        Ok(completions)
    }

    fn path(&self, key: &str) -> Option<PathBuf> {
        self.dir
            .as_ref()
            .map(|dir| dir.join(format!("{key}.{EXTENSION}")))
    }

    /// The delivered files of the batch `key`.
    pub(super) async fn load(&self, key: &str) -> HashSet<String> {
        let mut done = self
            .memory
            .lock()
            .expect("completions lock")
            .get(key)
            .cloned()
            .unwrap_or_default();
        if let Some(path) = self.path(key) {
            match tokio::fs::read(&path).await {
                Ok(bytes) => {
                    // One JSON string per line; a torn last line (crash mid-append) is ignored.
                    done.extend(
                        bytes
                            .split(|byte| *byte == b'\n')
                            .filter_map(|line| serde_json::from_slice::<String>(line).ok()),
                    );
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => warn!(
                    message = "Failed to read FDR batch progress; files of this batch may be delivered again.",
                    path = %path.display(),
                    %error,
                ),
            }
        }
        done
    }

    /// Records that `file` of the batch `key` is delivered.
    pub(super) async fn mark(&self, key: &str, file: &str) -> std::io::Result<()> {
        self.memory
            .lock()
            .expect("completions lock")
            .entry(key.to_owned())
            .or_default()
            .insert(file.to_owned());
        let Some(path) = self.path(key) else {
            return Ok(());
        };
        let mut line = serde_json::to_vec(file).map_err(std::io::Error::other)?;
        line.push(b'\n');
        let mut out = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        out.write_all(&line).await?;
        out.sync_data().await
    }

    /// Drops the record of a batch whose message is gone.
    pub(super) async fn forget(&self, key: &str) {
        self.memory.lock().expect("completions lock").remove(key);
        if let Some(path) = self.path(key) {
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => warn!(
                    message = "Failed to remove FDR batch progress.",
                    path = %path.display(),
                    %error,
                ),
            }
        }
        let now = chrono::Utc::now().timestamp();
        let last = self.last_sweep_secs.load(Ordering::Relaxed);
        if now - last >= SWEEP_EVERY_SECS
            && self
                .last_sweep_secs
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            self.sweep(MAX_AGE);
        }
    }

    /// Removes records not written to for `max_age` (their batches can no longer re-deliver).
    pub(super) fn sweep(&self, max_age: Duration) {
        let Some(dir) = &self.dir else {
            return;
        };
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        let now = SystemTime::now();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some(EXTENSION) {
                continue;
            }
            let expired = entry
                .metadata()
                .and_then(|meta| meta.modified())
                .ok()
                .and_then(|modified| now.duration_since(modified).ok())
                .is_some_and(|age| age >= max_age);
            if expired {
                drop(std::fs::remove_file(&path));
            }
        }
    }
}
