//! Which files of a batch are already delivered, so a re-delivered batch skips them.
//!
//! A file is recorded only once every one of its events is acknowledged by the sinks (or, with
//! acknowledgements off, once its events are handed to the pipeline). The record is kept in
//! memory and, when a data directory is available, in one append-only file per batch, so it
//! also survives a restart. It is removed once the batch's message is deleted and no other copy
//! of the same batch is still being processed. Records SQS can no longer re-deliver (older than
//! the 14-day maximum retention) are swept.
//!
//! The scope is one instance with a persistent data directory: another instance, or this one
//! after losing its data directory, knows nothing of what was delivered and emits the whole
//! batch again.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, SystemTime},
};

use sha2::Digest as _;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};

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
    /// Batches being processed, by key: how many holders, and whether the record is to be
    /// removed once the last one is done.
    active: Mutex<HashMap<String, Active>>,
    last_sweep_secs: AtomicI64,
}

#[derive(Debug, Default)]
struct Active {
    holders: usize,
    forget: bool,
}

/// Keeps a batch's record while a copy of the batch is processed or its acknowledgements are
/// still tracked. A `forget` while any guard is alive takes effect when the last one drops.
#[derive(Debug)]
pub(super) struct BatchGuard {
    completions: Arc<Completions>,
    key: String,
}

impl Drop for BatchGuard {
    fn drop(&mut self) {
        let remove = {
            let mut active = self.completions.active.lock().expect("completions lock");
            let Some(entry) = active.get_mut(&self.key) else {
                return;
            };
            entry.holders -= 1;
            if entry.holders > 0 {
                return;
            }
            let forget = entry.forget;
            active.remove(&self.key);
            forget
        };
        if remove {
            self.completions.remove(&self.key);
        }
    }
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
            active: Mutex::default(),
            last_sweep_secs: AtomicI64::new(0),
        }
    }

    /// How many batch records the data directory holds (batches a previous run left in
    /// progress, or this one is processing).
    pub(super) fn records_on_disk(&self) -> usize {
        self.dir
            .as_ref()
            .and_then(|dir| std::fs::read_dir(dir).ok())
            .map_or(0, |entries| {
                entries
                    .flatten()
                    .filter(|entry| {
                        entry.path().extension().and_then(|ext| ext.to_str()) == Some(EXTENSION)
                    })
                    .count()
            })
    }

    /// Registers one more holder of the batch `key` (see [`BatchGuard`]).
    pub(super) fn begin(self: &Arc<Self>, key: &str) -> BatchGuard {
        self.active
            .lock()
            .expect("completions lock")
            .entry(key.to_owned())
            .or_default()
            .holders += 1;
        BatchGuard {
            completions: Arc::clone(self),
            key: key.to_owned(),
        }
    }

    /// Records persisted under `data_dir`; sweeps expired ones.
    pub(super) fn open(data_dir: &Path) -> std::io::Result<Self> {
        let dir = data_dir.join(DIR_NAME);
        std::fs::create_dir_all(&dir)?;
        let completions = Self {
            dir: Some(dir),
            memory: Mutex::default(),
            active: Mutex::default(),
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
            .read(true)
            .append(true)
            .open(&path)
            .await?;
        let len = out.metadata().await?.len();
        if len > 0 {
            // A crash mid-append can leave a line without its newline; start a fresh line so
            // this record is not glued onto the torn one.
            let mut last = [0u8; 1];
            out.seek(std::io::SeekFrom::Start(len - 1)).await?;
            out.read_exact(&mut last).await?;
            if last[0] != b'\n' {
                line.insert(0, b'\n');
            }
        }
        out.write_all(&line).await?;
        out.sync_data().await?;
        if len == 0 {
            // A new file: make its directory entry durable too.
            sync_dir(path.parent().expect("record has a directory")).await?;
        }
        Ok(())
    }

    /// Drops the record of a batch whose message is gone, once no copy of the batch is still
    /// being processed (immediately when none is).
    pub(super) fn forget(&self, key: &str) {
        {
            let mut active = self.active.lock().expect("completions lock");
            if let Some(entry) = active.get_mut(key) {
                entry.forget = true;
                return;
            }
        }
        self.remove(key);
    }

    fn remove(&self, key: &str) {
        self.memory.lock().expect("completions lock").remove(key);
        if let Some(path) = self.path(key) {
            match std::fs::remove_file(&path) {
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

    /// Whether the batch `key` has a holder (test helper).
    #[cfg(test)]
    pub(super) fn is_active(&self, key: &str) -> bool {
        self.active
            .lock()
            .expect("completions lock")
            .contains_key(key)
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

/// Flushes a directory's entries (new files) to disk. Not supported on every platform; a
/// failure there is not an error.
async fn sync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        tokio::fs::File::open(dir).await?.sync_all().await
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}
