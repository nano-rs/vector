//! nano (NAN-5567): a bounded, local dead-letter store for batches ClickHouse permanently rejects.
//!
//! Upstream drops a batch ClickHouse refuses for good (HTTP 4xx, or a 500 carrying a
//! non-retryable code such as `117` incorrect data or `53` type mismatch) and acknowledges it,
//! so the disk buffer forgets it. Here the request body is written, exactly as it would have been
//! sent, to `<data_dir>/dead_letter/<sink_id>/` before the batch is finalized as `Rejected`, so
//! nothing is dropped silently and the batch can be inspected and replayed.
//!
//! Each batch is two files sharing a stem: the body (`<stem>.ndjson.gz` for the default
//! `JSONEachRow` + gzip) and `<stem>.meta.json`, which records the target table, format,
//! compression, event count, the rejection reason and the `INSERT` query that replays it.
//!
//! The store never blocks or fails delivery: it is capped at `max_bytes`, and when a new batch
//! does not fit, the oldest batches are deleted first. Every discarded event (evicted, too large
//! for the cap, or a failed write) is counted in `component_dead_letter_discarded_events_total`.
//! Stored events are counted in `component_dead_lettered_events_total`, and the bytes currently
//! held are reported in the `component_dead_letter_bytes` gauge.

use std::{
    collections::VecDeque,
    fs, io,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use bytes::Bytes;
use http_1::StatusCode;
use vector_lib::{
    NamedInternalEvent, configurable::configurable_component, counter, gauge,
    internal_event::{CounterName, GaugeName, InternalEvent},
};

use super::{
    config::Format,
    service::{ClickhouseRetryLogic, is_permanent_rejection},
    sink::PartitionKey,
};
use crate::sinks::{
    prelude::*,
    util::{http::HttpRequest, http_v1::HttpResponse},
};

/// Default cap on the bytes kept per sink: 256 MiB.
const DEFAULT_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// Unique suffix for dead-letter file names written by this process.
static SEQUENCE: AtomicU64 = AtomicU64::new(0);

const fn default_enabled() -> bool {
    true
}

const fn default_max_bytes() -> u64 {
    DEFAULT_MAX_BYTES
}

/// Dead-letter store for batches ClickHouse permanently rejects (nano).
#[configurable_component]
#[derive(Clone, Copy, Debug)]
#[serde(deny_unknown_fields)]
pub struct DeadLetterConfig {
    /// Write permanently rejected batches to `<data_dir>/dead_letter/<sink_id>/` instead of
    /// dropping them.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// The most bytes kept for this sink. When a new batch does not fit, the oldest batches are
    /// deleted (and counted as discarded).
    #[serde(default = "default_max_bytes")]
    #[configurable(metadata(docs::type_unit = "bytes"))]
    pub max_bytes: u64,
}

impl Default for DeadLetterConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            max_bytes: default_max_bytes(),
        }
    }
}

#[derive(Debug, NamedInternalEvent)]
struct DeadLettered {
    count: usize,
    held_bytes: u64,
}

impl InternalEvent for DeadLettered {
    fn emit(self) {
        counter!(CounterName::ComponentDeadLetteredEventsTotal).increment(self.count as u64);
        gauge!(GaugeName::ComponentDeadLetterBytes).set(self.held_bytes as f64);
    }
}

#[derive(Debug, NamedInternalEvent)]
struct DeadLetterDiscarded {
    count: usize,
    reason: &'static str,
}

impl InternalEvent for DeadLetterDiscarded {
    fn emit(self) {
        if self.count > 0 {
            counter!(
                CounterName::ComponentDeadLetterDiscardedEventsTotal,
                "reason" => self.reason,
            )
            .increment(self.count as u64);
        }
    }
}

/// One stored batch: its files, their combined size, and its event count.
#[derive(Debug)]
struct Entry {
    files: Vec<PathBuf>,
    bytes: u64,
    events: usize,
}

#[derive(Debug, Default)]
struct State {
    entries: VecDeque<Entry>,
    held_bytes: u64,
}

/// What one `store` call did, reported as internal events by the caller.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct StoreOutcome {
    pub(super) stored_events: usize,
    pub(super) evicted_events: usize,
    pub(super) oversize_events: usize,
    pub(super) failed_events: usize,
    pub(super) held_bytes: u64,
}

impl StoreOutcome {
    fn emit(&self) {
        if self.stored_events > 0 {
            emit!(DeadLettered {
                count: self.stored_events,
                held_bytes: self.held_bytes,
            });
        }
        emit!(DeadLetterDiscarded {
            count: self.evicted_events,
            reason: "evicted",
        });
        emit!(DeadLetterDiscarded {
            count: self.oversize_events,
            reason: "oversize",
        });
        emit!(DeadLetterDiscarded {
            count: self.failed_events,
            reason: "write_failed",
        });
    }
}

/// A rejected batch, as it would have been sent.
#[derive(Debug)]
pub(super) struct RejectedBatch {
    pub(super) key: PartitionKey,
    pub(super) body: Bytes,
    pub(super) events: usize,
    pub(super) reason: String,
    pub(super) http_status: Option<u16>,
}

/// The bounded dead-letter directory of one sink.
#[derive(Debug)]
pub(super) struct DeadLetterStore {
    dir: PathBuf,
    sink_id: String,
    compression: Compression,
    max_bytes: u64,
    state: Mutex<State>,
}

impl DeadLetterStore {
    /// Opens (without creating) the store at `dir`, accounting for batches left by earlier runs.
    pub(super) fn open(
        dir: PathBuf,
        sink_id: String,
        compression: Compression,
        max_bytes: u64,
    ) -> Self {
        let state = scan(&dir);
        if !state.entries.is_empty() {
            warn!(
                message = "Dead-letter store holds batches ClickHouse rejected earlier; inspect and replay them.",
                path = %dir.display(),
                batches = state.entries.len(),
                bytes = state.held_bytes,
            );
        }
        gauge!(GaugeName::ComponentDeadLetterBytes).set(state.held_bytes as f64);
        Self {
            dir,
            sink_id,
            compression,
            max_bytes,
            state: Mutex::new(state),
        }
    }

    /// Writes one rejected batch, evicting the oldest batches if needed. Never fails: problems
    /// are logged and reported as discarded events.
    pub(super) fn store(&self, batch: &RejectedBatch) -> StoreOutcome {
        let mut outcome = StoreOutcome::default();
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);

        let stem = format!(
            "{:013}-{:010}-{}",
            chrono::Utc::now().timestamp_millis(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed),
            std::process::id()
        );
        let body_name = format!(
            "{stem}.{}{}",
            body_extension(batch.key.format),
            compression_suffix(self.compression)
        );
        let meta = self.meta_json(batch, &body_name);
        let needed = batch.body.len() as u64 + meta.len() as u64;

        if needed > self.max_bytes {
            error!(
                message = "Rejected batch is larger than the dead-letter cap; discarding it.",
                events = batch.events,
                bytes = needed,
                max_bytes = self.max_bytes,
                reason = %batch.reason,
            );
            outcome.oversize_events = batch.events;
            outcome.held_bytes = state.held_bytes;
            return outcome;
        }

        while state.held_bytes + needed > self.max_bytes {
            let Some(oldest) = state.entries.pop_front() else {
                break;
            };
            for file in &oldest.files {
                if let Err(error) = fs::remove_file(file)
                    && error.kind() != io::ErrorKind::NotFound
                {
                    warn!(message = "Failed to delete an evicted dead-letter file.", path = %file.display(), %error);
                }
            }
            state.held_bytes = state.held_bytes.saturating_sub(oldest.bytes);
            outcome.evicted_events += oldest.events;
        }
        if outcome.evicted_events > 0 {
            warn!(
                message = "Dead-letter store is full; deleted the oldest rejected batches.",
                path = %self.dir.display(),
                evicted_events = outcome.evicted_events,
                max_bytes = self.max_bytes,
            );
        }

        let body_path = self.dir.join(&body_name);
        let meta_path = self.dir.join(format!("{stem}.meta.json"));
        let written = fs::create_dir_all(&self.dir)
            .and_then(|()| write_atomic(&body_path, &batch.body))
            .and_then(|()| write_atomic(&meta_path, meta.as_bytes()));
        match written {
            Ok(()) => {
                state.entries.push_back(Entry {
                    files: vec![body_path.clone(), meta_path],
                    bytes: needed,
                    events: batch.events,
                });
                state.held_bytes += needed;
                outcome.stored_events = batch.events;
                error!(
                    message = "ClickHouse permanently rejected a batch; wrote it to the dead-letter store.",
                    path = %body_path.display(),
                    events = batch.events,
                    database = %batch.key.database,
                    table = %batch.key.table,
                    reason = %batch.reason,
                );
            }
            Err(error) => {
                _ = fs::remove_file(&body_path);
                _ = fs::remove_file(&meta_path);
                error!(
                    message = "Failed to write a rejected batch to the dead-letter store; discarding it.",
                    path = %self.dir.display(),
                    events = batch.events,
                    %error,
                    reason = %batch.reason,
                );
                outcome.failed_events = batch.events;
            }
        }
        outcome.held_bytes = state.held_bytes;
        outcome
    }

    fn meta_json(&self, batch: &RejectedBatch, body_file: &str) -> String {
        let insert = format!(
            "INSERT INTO {}.{} FORMAT {}",
            quote_identifier(&batch.key.database),
            quote_identifier(&batch.key.table),
            batch.key.format
        );
        let meta = serde_json::json!({
            "sink": self.sink_id,
            "body_file": body_file,
            "database": batch.key.database,
            "table": batch.key.table,
            "format": batch.key.format.to_string(),
            "content_encoding": self.compression.content_encoding(),
            "events": batch.events,
            "body_bytes": batch.body.len(),
            "http_status": batch.http_status,
            "reason": batch.reason,
            "rejected_at": chrono::Utc::now().to_rfc3339(),
            "replay_query": insert,
        });
        let mut text = meta.to_string();
        text.push('\n');
        text
    }
}

/// Writes `bytes` to a temporary sibling, syncs it, then renames it into place, so a reader never
/// sees a partial file.
fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write as _;

    let tmp = path.with_extension("tmp");
    let mut file = fs::File::create(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, path)
}

/// Rebuilds the size accounting from files left in `dir` by earlier runs, oldest first.
fn scan(dir: &Path) -> State {
    let mut state = State::default();
    let Ok(read_dir) = fs::read_dir(dir) else {
        return state;
    };
    let mut files: Vec<(String, PathBuf, u64)> = read_dir
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let meta = entry.metadata().ok()?;
            meta.is_file().then(|| {
                (
                    entry.file_name().to_string_lossy().into_owned(),
                    entry.path(),
                    meta.len(),
                )
            })
        })
        .collect();
    files.sort();

    // Group files by stem (the part before the first '.').
    let mut current: Option<(String, Entry)> = None;
    for (name, path, len) in files {
        if name.ends_with(".tmp") {
            // An interrupted write: never a complete batch.
            _ = fs::remove_file(&path);
            continue;
        }
        let stem = name.split('.').next().unwrap_or_default().to_owned();
        let events = if name.ends_with(".meta.json") {
            fs::read(&path)
                .ok()
                .and_then(|raw| serde_json::from_slice::<serde_json::Value>(&raw).ok())
                .and_then(|value| value.get("events").and_then(serde_json::Value::as_u64))
                .unwrap_or(0) as usize
        } else {
            0
        };
        match &mut current {
            Some((current_stem, entry)) if *current_stem == stem => {
                entry.files.push(path);
                entry.bytes += len;
                entry.events += events;
            }
            _ => {
                if let Some((_, entry)) = current.take() {
                    state.held_bytes += entry.bytes;
                    state.entries.push_back(entry);
                }
                current = Some((
                    stem,
                    Entry {
                        files: vec![path],
                        bytes: len,
                        events,
                    },
                ));
            }
        }
    }
    if let Some((_, entry)) = current {
        state.held_bytes += entry.bytes;
        state.entries.push_back(entry);
    }
    state
}

const fn body_extension(format: Format) -> &'static str {
    match format {
        Format::JsonEachRow | Format::JsonAsObject | Format::JsonAsString => "ndjson",
        Format::ArrowStream => "arrows",
    }
}

const fn compression_suffix(compression: Compression) -> &'static str {
    match compression {
        Compression::None => "",
        Compression::Gzip(_) => ".gz",
        Compression::Zlib(_) => ".zz",
        Compression::Zstd(_) => ".zst",
        Compression::Snappy => ".snappy",
    }
}

fn quote_identifier(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

/// Resolves the store directory for a sink, or `None` when the store is disabled or the sink has
/// no id or data directory (for example while `vector validate` builds the sink).
pub(super) fn resolve_store(
    config: DeadLetterConfig,
    cx: &SinkContext,
    compression: Compression,
) -> Option<Arc<DeadLetterStore>> {
    if !config.enabled {
        return None;
    }
    let key = cx.key.as_ref()?;
    let data_dir = match cx.globals.resolve_and_validate_data_dir(None) {
        Ok(dir) => dir,
        Err(error) => {
            warn!(
                message = "No usable data_dir; rejected ClickHouse batches will be dropped, not dead-lettered.",
                %error,
            );
            return None;
        }
    };
    let dir = data_dir.join("dead_letter").join(sanitize(key.id()));
    Some(Arc::new(DeadLetterStore::open(
        dir,
        key.id().to_owned(),
        compression,
        config.max_bytes,
    )))
}

fn sanitize(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// The response of [`DeadLetterService`]: the HTTP response, with the batch status decided by
/// the ClickHouse retry logic rather than by the HTTP status class alone.
#[derive(Debug)]
pub(super) struct ClickhouseResponse {
    inner: HttpResponse,
    status: EventStatus,
}

impl DriverResponse for ClickhouseResponse {
    fn event_status(&self) -> EventStatus {
        self.status
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        self.inner.events_sent()
    }

    fn bytes_sent(&self) -> Option<usize> {
        self.inner.bytes_sent()
    }
}

/// Wraps the retrying ClickHouse service (outside the retry layer, so it sees only the final
/// outcome of a batch).
///
/// - A success passes through as `Delivered`.
/// - A final non-success response that the retry logic would not retry (4xx, or a 500 carrying a
///   non-retryable ClickHouse code) is a permanent rejection: the body is written to the
///   dead-letter store and the batch is reported `Rejected`, so the disk buffer acknowledges it.
///   Upstream reported those 500s as `Errored`, which the disk buffer now holds for replay; a
///   permanently bad batch would then be replayed forever.
/// - A final response the retry logic would still retry (retries exhausted on a transient error,
///   including an access refusal, NAN-5390) is `Errored`, so the disk buffer keeps the batch for
///   replay after a restart. Upstream reported a retryable 4xx such as 403 or 429 as `Rejected`,
///   which acknowledged it without a copy.
/// - An error (a non-retryable transport or request-building error, reported `Rejected` by the
///   driver) is dead-lettered too, so it is not dropped silently.
#[derive(Clone)]
pub(super) struct DeadLetterService<S> {
    inner: S,
    store: Option<Arc<DeadLetterStore>>,
}

impl<S> DeadLetterService<S> {
    pub(super) const fn new(inner: S, store: Option<Arc<DeadLetterStore>>) -> Self {
        Self { inner, store }
    }
}

async fn dead_letter(store: Option<Arc<DeadLetterStore>>, batch: RejectedBatch) {
    let Some(store) = store else {
        return;
    };
    // File I/O off the async runtime; the internal events are emitted back here, inside the
    // component's span, so they carry its tags.
    match tokio::task::spawn_blocking(move || store.store(&batch)).await {
        Ok(outcome) => outcome.emit(),
        Err(error) => error!(message = "Dead-letter write task failed.", %error),
    }
}

impl<S> Service<HttpRequest<PartitionKey>> for DeadLetterService<S>
where
    S: Service<HttpRequest<PartitionKey>, Response = HttpResponse> + Send,
    S::Future: Send + 'static,
    S::Error: std::fmt::Display + Send + 'static,
{
    type Response = ClickhouseResponse;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: HttpRequest<PartitionKey>) -> Self::Future {
        let store = self.store.clone();
        let key = request.get_additional_metadata().clone();
        let body = request.get_payload().clone();
        let events = request.get_metadata().event_count();
        let future = self.inner.call(request);

        Box::pin(async move {
            match future.await {
                Ok(response) => {
                    let status = response.response.status();
                    let event_status = if status.is_success() {
                        EventStatus::Delivered
                    } else if is_permanent_rejection(&ClickhouseRetryLogic, &response) {
                        EventStatus::Rejected
                    } else {
                        EventStatus::Errored
                    };
                    if event_status == EventStatus::Rejected {
                        let reason = rejection_reason(status, response.response.body());
                        dead_letter(
                            store,
                            RejectedBatch {
                                key,
                                body,
                                events,
                                reason,
                                http_status: Some(status.as_u16()),
                            },
                        )
                        .await;
                    }
                    Ok(ClickhouseResponse {
                        inner: response,
                        status: event_status,
                    })
                }
                Err(error) => {
                    dead_letter(
                        store,
                        RejectedBatch {
                            key,
                            body,
                            events,
                            reason: error.to_string(),
                            http_status: None,
                        },
                    )
                    .await;
                    Err(error)
                }
            }
        })
    }
}

fn rejection_reason(status: StatusCode, body: &[u8]) -> String {
    let detail = String::from_utf8_lossy(&body[..body.len().min(1024)]);
    format!("HTTP {status}: {}", detail.trim_end())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(events: usize, body: &'static [u8]) -> RejectedBatch {
        RejectedBatch {
            key: PartitionKey {
                database: "nanosiem".to_owned(),
                table: "logs_ingest".to_owned(),
                format: Format::JsonEachRow,
            },
            body: Bytes::from_static(body),
            events,
            reason: "HTTP 500: Code: 117. DB::Exception: incorrect data".to_owned(),
            http_status: Some(500),
        }
    }

    fn body_files(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".ndjson.gz"))
            .collect();
        names.sort();
        names
    }

    #[test]
    fn rejected_batch_is_written_with_replay_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("dead_letter").join("clickhouse_logs");
        let store = DeadLetterStore::open(
            dir.clone(),
            "clickhouse_logs".to_owned(),
            Compression::gzip_default(),
            1024 * 1024,
        );
        let outcome = store.store(&batch(2, b"{\"a\":1}\n{\"a\":2}\n"));
        assert_eq!(outcome.stored_events, 2);
        assert_eq!(outcome.evicted_events + outcome.failed_events, 0);

        let bodies = body_files(&dir);
        assert_eq!(bodies.len(), 1);
        assert_eq!(
            fs::read(dir.join(&bodies[0])).unwrap(),
            b"{\"a\":1}\n{\"a\":2}\n"
        );
        let stem = bodies[0].split('.').next().unwrap();
        let meta: serde_json::Value =
            serde_json::from_slice(&fs::read(dir.join(format!("{stem}.meta.json"))).unwrap())
                .unwrap();
        assert_eq!(meta["events"], 2);
        assert_eq!(meta["table"], "logs_ingest");
        assert_eq!(meta["content_encoding"], "gzip");
        assert_eq!(meta["http_status"], 500);
        assert_eq!(
            meta["replay_query"],
            "INSERT INTO `nanosiem`.`logs_ingest` FORMAT JSONEachRow"
        );
        assert_eq!(outcome.held_bytes, store.state.lock().unwrap().held_bytes);
    }

    #[test]
    fn full_store_evicts_the_oldest_batches_and_counts_them() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let probe = DeadLetterStore::open(
            tmp.path().join("probe"),
            "s".to_owned(),
            Compression::gzip_default(),
            u64::MAX,
        );
        let one = probe.store(&batch(3, b"0123456789")).held_bytes;

        // Room for two batches, not three.
        let store = DeadLetterStore::open(
            dir.clone(),
            "s".to_owned(),
            Compression::gzip_default(),
            one * 2 + one / 2,
        );
        assert_eq!(store.store(&batch(3, b"0123456789")).evicted_events, 0);
        assert_eq!(store.store(&batch(3, b"0123456789")).evicted_events, 0);
        let first_two = body_files(&dir);
        let third = store.store(&batch(3, b"0123456789"));
        assert_eq!(third.stored_events, 3);
        assert_eq!(third.evicted_events, 3, "the oldest batch is evicted");
        let now = body_files(&dir);
        assert_eq!(now.len(), 2);
        assert!(!now.contains(&first_two[0]));
        assert!(now.contains(&first_two[1]));
        assert!(third.held_bytes <= one * 2 + one / 2);

        // A batch larger than the cap is discarded, and nothing is evicted for it.
        let small = DeadLetterStore::open(
            tmp.path().join("small"),
            "s".to_owned(),
            Compression::gzip_default(),
            8,
        );
        let oversize = small.store(&batch(5, b"0123456789"));
        assert_eq!(oversize.oversize_events, 5);
        assert_eq!(oversize.stored_events, 0);
    }

    fn request(body: &'static [u8], events: usize) -> HttpRequest<PartitionKey> {
        HttpRequest::new(
            Bytes::from_static(body),
            EventFinalizers::default(),
            RequestMetadata::new(
                events,
                body.len(),
                body.len(),
                body.len(),
                GroupedCountByteSize::new_untagged(),
            ),
            batch(events, body).key,
        )
    }

    async fn call_with(
        store: Option<Arc<DeadLetterStore>>,
        status: u16,
        body: &'static str,
    ) -> EventStatus {
        let inner = tower::service_fn(move |_request: HttpRequest<PartitionKey>| async move {
            Ok::<_, crate::Error>(HttpResponse {
                response: http_1::Response::builder()
                    .status(status)
                    .body(Bytes::from_static(body.as_bytes()))
                    .unwrap(),
                events_byte_size: GroupedCountByteSize::new_untagged(),
                raw_byte_size: 0,
            })
        });
        let mut service = DeadLetterService::new(inner, store);
        let response = service
            .call(request(b"{\"a\":1}\n", 1))
            .await
            .expect("the inner service answered");
        response.event_status()
    }

    /// NAN-5567: a batch ClickHouse permanently rejects is written to the dead-letter store and
    /// reported `Rejected` (the disk buffer then acknowledges it), while a transient failure that
    /// outlived its retries stays `Errored` (the disk buffer keeps it for replay) and is not
    /// dead-lettered.
    #[tokio::test]
    async fn rejected_batch_goes_to_dead_letter_and_transient_stays_errored() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let store = Arc::new(DeadLetterStore::open(
            dir.clone(),
            "clickhouse_logs".to_owned(),
            Compression::gzip_default(),
            u64::MAX,
        ));

        // Upstream reports this 500 as `Errored`; replaying it would fail forever.
        let status = call_with(Some(Arc::clone(&store)), 500, "Code: 117. DB::Exception").await;
        assert_eq!(status, EventStatus::Rejected);
        assert_eq!(body_files(&dir).len(), 1);

        let status = call_with(Some(Arc::clone(&store)), 400, "Code: 27. Cannot parse").await;
        assert_eq!(status, EventStatus::Rejected);
        assert_eq!(body_files(&dir).len(), 2);

        let status = call_with(Some(Arc::clone(&store)), 503, "Code: 202. Too many").await;
        assert_eq!(status, EventStatus::Errored);
        assert_eq!(body_files(&dir).len(), 2, "a transient failure is not dead-lettered");

        // An access refusal is retried by the retry logic (NAN-5390), never dead-lettered.
        let status = call_with(Some(Arc::clone(&store)), 403, "Code: 497. ACCESS_DENIED").await;
        assert_eq!(status, EventStatus::Errored);
        assert_eq!(body_files(&dir).len(), 2);

        let status = call_with(None, 200, "").await;
        assert_eq!(status, EventStatus::Delivered);
    }

    #[test]
    fn reopening_accounts_for_batches_from_earlier_runs() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let store = DeadLetterStore::open(
            dir.clone(),
            "s".to_owned(),
            Compression::gzip_default(),
            u64::MAX,
        );
        store.store(&batch(4, b"abc"));
        let held = store.store(&batch(6, b"defg")).held_bytes;
        fs::write(dir.join("0000000000000-0000000000-1.ndjson.gz.tmp"), b"partial").unwrap();
        drop(store);

        let reopened = DeadLetterStore::open(
            dir.clone(),
            "s".to_owned(),
            Compression::gzip_default(),
            u64::MAX,
        );
        let state = reopened.state.lock().unwrap();
        assert_eq!(state.held_bytes, held);
        assert_eq!(state.entries.len(), 2);
        assert_eq!(state.entries[0].events, 4);
        assert_eq!(state.entries[1].events, 6);
        assert!(
            !dir.join("0000000000000-0000000000-1.ndjson.gz.tmp").exists(),
            "an interrupted write is removed"
        );
    }
}
