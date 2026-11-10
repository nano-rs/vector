//! `start_from = cursor`: the newest fully delivered batch time, persisted in the data dir.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

const FILE_NAME: &str = "crowdstrike_fdr_cursor.json";
const START_FILE_NAME: &str = "crowdstrike_fdr_start.json";

#[derive(Debug, Deserialize, Serialize)]
struct StartFile {
    /// When the source first started with `start_from = now`, epoch milliseconds.
    started_at_ms: i64,
}

/// `start_from = now`: the time of the first start, persisted in `dir` so a restart does not
/// move the cutoff forward past batches that arrived while the source was down.
pub(super) fn pinned_start(dir: &Path, now_ms: i64) -> std::io::Result<i64> {
    let path = dir.join(START_FILE_NAME);
    match std::fs::read(&path) {
        Ok(bytes) => {
            return serde_json::from_slice::<StartFile>(&bytes)
                .map(|file| file.started_at_ms)
                .map_err(std::io::Error::other);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let body = serde_json::to_vec(&StartFile {
        started_at_ms: now_ms,
    })
    .map_err(std::io::Error::other)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, body)?;
    std::fs::rename(&tmp, &path)?;
    Ok(now_ms)
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct CursorFile {
    /// Batch time of the newest acknowledged manifest, epoch milliseconds.
    batch_timestamp_ms: i64,
}

/// Persists the newest acknowledged batch time. Advances never move it backwards.
#[derive(Debug)]
pub(super) struct Cursor {
    path: PathBuf,
    current: Mutex<Option<i64>>,
}

impl Cursor {
    /// Opens the cursor in `dir`, reading any value a previous run left.
    pub(super) fn open(dir: &Path) -> std::io::Result<Self> {
        let path = dir.join(FILE_NAME);
        let current = match std::fs::read(&path) {
            Ok(bytes) => Some(
                serde_json::from_slice::<CursorFile>(&bytes)
                    .map_err(std::io::Error::other)?
                    .batch_timestamp_ms,
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        Ok(Self {
            path,
            current: Mutex::new(current),
        })
    }

    /// The persisted value at open time (or after advances).
    pub(super) async fn get(&self) -> Option<i64> {
        *self.current.lock().await
    }

    /// Records `batch_timestamp_ms` if it is newer than the stored value.
    pub(super) async fn advance(&self, batch_timestamp_ms: i64) -> std::io::Result<()> {
        let mut current = self.current.lock().await;
        if current.is_some_and(|stored| stored >= batch_timestamp_ms) {
            return Ok(());
        }
        let body = serde_json::to_vec(&CursorFile { batch_timestamp_ms })
            .map_err(std::io::Error::other)?;
        let tmp = self.path.with_extension("json.tmp");
        tokio::fs::write(&tmp, body).await?;
        tokio::fs::rename(&tmp, &self.path).await?;
        *current = Some(batch_timestamp_ms);
        Ok(())
    }
}
