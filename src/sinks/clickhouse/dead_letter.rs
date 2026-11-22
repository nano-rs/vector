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
use http::StatusCode;
use vector_lib::{
    NamedInternalEvent, configurable::configurable_component, counter, gauge,
    internal_event::{CounterName, GaugeName, InternalEvent, emit},
};

use super::{
    config::Format,
    service::{ClickhouseRetryLogic, ClickhouseServiceRequestBuilder, is_permanent_rejection},
    sink::PartitionKey,
};
use crate::sinks::{
    prelude::*,
    util::http::{HttpRequest, HttpResponse},
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
        if self.count > 0 {
            counter!(CounterName::ComponentDeadLetteredEventsTotal).increment(self.count as u64);
        }
        // Set on every outcome, so evictions and failed writes are reflected too.
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
    /// Whether the batch is durably in the store.
    const fn stored(&self) -> bool {
        self.stored_events > 0
    }

    /// `kept` (nano, NAN-5808): the caller keeps a batch the store could not take (it is reported
    /// `Errored` into a disk buffer that holds it), so it is not counted as discarded.
    fn emit(&self, kept: bool) {
        emit(DeadLettered {
            count: self.stored_events,
            held_bytes: self.held_bytes,
        });
        emit(DeadLetterDiscarded {
            count: self.evicted_events,
            reason: "evicted",
        });
        if !kept {
            emit(DeadLetterDiscarded {
                count: self.oversize_events,
                reason: "oversize",
            });
            emit(DeadLetterDiscarded {
                count: self.failed_events,
                reason: "write_failed",
            });
        }
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
    /// The URL query string the batch was sent with (`INSERT` plus settings), if known.
    pub(super) url_query: Option<String>,
    /// How the batch is reported: `Rejected` (permanent, acknowledged once this copy is stored) or
    /// `Errored` (transient, or a permanent rejection this store could not take).
    pub(super) event_status: EventStatus,
    /// The batch is also kept in the sink's disk buffer and replayed after a restart (an
    /// `Errored` batch in a buffer that holds `Errored` records, NAN-5567). Replaying this copy as
    /// well would duplicate it.
    pub(super) held_for_replay: bool,
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
                message = "Rejected batch is larger than the dead-letter cap; not stored.",
                events = batch.events,
                bytes = needed,
                max_bytes = self.max_bytes,
                reason = %batch.reason,
            );
            outcome.oversize_events = batch.events;
            outcome.held_bytes = state.held_bytes;
            return outcome;
        }

        // Write both files under temporary names first, so a failed write never costs the
        // batches an eviction would have deleted to make room for it.
        let body_path = self.dir.join(&body_name);
        let meta_path = self.dir.join(format!("{stem}.meta.json"));
        let body_tmp = tmp_path(&body_path);
        let meta_tmp = tmp_path(&meta_path);
        let staged = create_private_dir(&self.dir)
            .and_then(|()| write_synced(&body_tmp, &batch.body))
            .and_then(|()| write_synced(&meta_tmp, meta.as_bytes()));
        if let Err(error) = staged {
            _ = fs::remove_file(&body_tmp);
            _ = fs::remove_file(&meta_tmp);
            error!(
                message = "Failed to write a rejected batch to the dead-letter store; not stored.",
                path = %self.dir.display(),
                events = batch.events,
                %error,
                reason = %batch.reason,
            );
            outcome.failed_events = batch.events;
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

        // The body is published before its meta: the meta file marks a complete batch.
        let published = fs::rename(&body_tmp, &body_path)
            .and_then(|()| fs::rename(&meta_tmp, &meta_path))
            .and_then(|()| sync_dir(&self.dir));
        match published {
            Ok(()) => {
                state.entries.push_back(Entry {
                    files: vec![body_path.clone(), meta_path],
                    bytes: needed,
                    events: batch.events,
                });
                state.held_bytes += needed;
                outcome.stored_events = batch.events;
                error!(
                    message = "ClickHouse did not accept a batch; wrote it to the dead-letter store.",
                    path = %body_path.display(),
                    events = batch.events,
                    database = %batch.key.database,
                    table = %batch.key.table,
                    held_for_replay = batch.held_for_replay,
                    reason = %batch.reason,
                );
            }
            Err(error) => {
                for file in [&body_tmp, &meta_tmp, &body_path, &meta_path] {
                    _ = fs::remove_file(file);
                }
                error!(
                    message = "Failed to write a rejected batch to the dead-letter store; not stored.",
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
            // The exact URL query the sink sent (INSERT plus input/async-insert settings).
            "replay_url_query": batch.url_query,
            "event_status": match batch.event_status {
                EventStatus::Rejected => "rejected",
                _ => "errored",
            },
            "held_for_replay": batch.held_for_replay,
        });
        let mut text = meta.to_string();
        text.push('\n');
        text
    }
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

/// Creates the store directory readable by the Vector user only (it holds raw log bodies).
fn create_private_dir(dir: &Path) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    builder.create(dir)
}

/// Writes `bytes` to a new file readable by the Vector user only, and syncs it.
fn write_synced(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write as _;

    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// Syncs the directory, so the renames survive a host crash.
fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Whether `name` is one of this store's files: `<13 digits>-<10 digits>-<pid>.<ext>`.
fn is_store_file(name: &str) -> bool {
    let stem = name.split('.').next().unwrap_or_default();
    let mut parts = stem.split('-');
    let (Some(ms), Some(seq), Some(pid), None) = (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    let digits = |s: &str, len: Option<usize>| {
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) && len.is_none_or(|l| s.len() == l)
    };
    name.len() > stem.len() && digits(ms, Some(13)) && digits(seq, Some(10)) && digits(pid, None)
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
        // Only this store's files are counted (and so may be evicted). Temporary files are left
        // alone: one may belong to a write in progress by the sink instance a reload replaces.
        if name.ends_with(".tmp") || !is_store_file(&name) {
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
    /// `None` when the request failed without a response (nano, NAN-5808).
    inner: Option<HttpResponse>,
    status: EventStatus,
    no_events: GroupedCountByteSize,
}

impl ClickhouseResponse {
    fn new(inner: Option<HttpResponse>, status: EventStatus) -> Self {
        Self {
            inner,
            status,
            no_events: GroupedCountByteSize::new_untagged(),
        }
    }
}

impl DriverResponse for ClickhouseResponse {
    fn event_status(&self) -> EventStatus {
        self.status
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        self.inner
            .as_ref()
            .map_or(&self.no_events, HttpResponse::events_sent)
    }

    fn bytes_sent(&self) -> Option<usize> {
        self.inner.as_ref().and_then(HttpResponse::bytes_sent)
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
/// - A request that failed without a response:
///   - transient (a retriable transport error or a timeout, retries exhausted) is `Errored`, with a
///     copy, like a transient response;
///   - permanent (for example a request that could not be built) is dead-lettered and returned as
///     the error, which the driver reports `Rejected`.
///
/// **No acknowledgement without a durable write (NAN-5808).** A batch is reported `Rejected`
/// (which the disk buffer acknowledges) only once its copy is durably in the dead-letter store.
/// When the store cannot take it (disabled, no usable `data_dir`, larger than the cap, or a failed
/// write), it is reported `Errored` instead, so a disk buffer that holds `Errored` records keeps
/// it: Vector stops and replays it after a restart rather than losing it.
#[derive(Clone)]
pub(super) struct DeadLetterService<S> {
    inner: S,
    store: Option<Arc<DeadLetterStore>>,
    replay: Option<Arc<ClickhouseServiceRequestBuilder>>,
    /// The sink's disk buffer holds `Errored` records for replay (`SinkContext::buffer_holds_errored`).
    buffer_holds_errored: bool,
}

impl<S> DeadLetterService<S> {
    pub(super) const fn new(
        inner: S,
        store: Option<Arc<DeadLetterStore>>,
        replay: Option<Arc<ClickhouseServiceRequestBuilder>>,
        buffer_holds_errored: bool,
    ) -> Self {
        Self {
            inner,
            store,
            replay,
            buffer_holds_errored,
        }
    }
}

/// Writes `batch` to the store. Returns whether it is durably stored.
async fn dead_letter(
    store: Option<Arc<DeadLetterStore>>,
    batch: RejectedBatch,
    buffer_holds_errored: bool,
) -> bool {
    let Some(store) = store else {
        return false;
    };
    // A batch this store cannot take is reported `Errored`; it is kept only by a buffer that
    // holds `Errored` records.
    let kept = buffer_holds_errored;
    // File I/O off the async runtime; the internal events are emitted back here, inside the
    // component's span, so they carry its tags.
    let span = tracing::Span::current();
    match tokio::task::spawn_blocking(move || span.in_scope(|| store.store(&batch))).await {
        Ok(outcome) => {
            outcome.emit(kept);
            outcome.stored()
        }
        Err(error) => {
            error!(message = "Dead-letter write task failed.", %error);
            false
        }
    }
}

/// Whether a request error is transient: a retriable transport error or a timeout, which the
/// retry layer gave up on (nano, NAN-5808). Anything else (a request that could not be built, an
/// unknown error) is permanent.
fn is_transient_error(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut next = Some(error);
    while let Some(error) = next {
        if let Some(http) = error.downcast_ref::<vector_http::HttpError>() {
            return http.is_retriable();
        }
        if error.downcast_ref::<tower::timeout::error::Elapsed>().is_some() {
            return true;
        }
        next = error.source();
    }
    false
}

/// The status for a batch that was not delivered, given whether its copy is durably stored:
/// `Rejected` only for a permanent failure that is stored; otherwise `Errored`.
const fn undelivered_status(permanent: bool, stored: bool) -> EventStatus {
    if permanent && stored {
        EventStatus::Rejected
    } else {
        EventStatus::Errored
    }
}

impl<S> Service<HttpRequest<PartitionKey>> for DeadLetterService<S>
where
    S: Service<HttpRequest<PartitionKey>, Response = HttpResponse> + Send,
    S::Future: Send + 'static,
    S::Error: AsRef<dyn std::error::Error + Send + Sync + 'static> + Send + 'static,
{
    type Response = ClickhouseResponse;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: HttpRequest<PartitionKey>) -> Self::Future {
        let store = self.store.clone();
        let holds = self.buffer_holds_errored;
        let key = request.get_additional_metadata().clone();
        let url_query = self
            .replay
            .as_ref()
            .and_then(|replay| replay.replay_url_query(&key));
        let body = request.get_payload().clone();
        let events = request.get_metadata().event_count();
        let future = self.inner.call(request);

        Box::pin(async move {
            // Every undelivered batch is copied: a permanent one so it can be acknowledged, an
            // `Errored` one because the buffer holds it only when it holds `Errored` records.
            let copy = |reason: String, http_status: Option<u16>, permanent: bool| RejectedBatch {
                key,
                body,
                events,
                reason,
                http_status,
                url_query,
                event_status: if permanent {
                    EventStatus::Rejected
                } else {
                    EventStatus::Errored
                },
                held_for_replay: !permanent && holds,
            };
            match future.await {
                Ok(response) => {
                    let status = response.response.status();
                    if status.is_success() {
                        return Ok(ClickhouseResponse::new(Some(response), EventStatus::Delivered));
                    }
                    let permanent = is_permanent_rejection(&ClickhouseRetryLogic, &response);
                    let reason = rejection_reason(status, response.response.body());
                    let stored =
                        dead_letter(store, copy(reason, Some(status.as_u16()), permanent), holds)
                            .await;
                    let event_status = undelivered_status(permanent, stored);
                    if permanent && !stored {
                        not_stored(events, holds);
                    }
                    Ok(ClickhouseResponse::new(Some(response), event_status))
                }
                Err(error) => {
                    let permanent = !is_transient_error(error.as_ref());
                    let stored =
                        dead_letter(store, copy(error.as_ref().to_string(), None, permanent), holds)
                            .await;
                    if undelivered_status(permanent, stored) == EventStatus::Rejected {
                        // Stored: the driver reports the error as `Rejected`.
                        return Err(error);
                    }
                    if permanent {
                        not_stored(events, holds);
                    }
                    error!(
                        message = "ClickHouse request failed; reporting the batch as errored.",
                        events,
                        error = %error.as_ref(),
                        held_for_replay = holds,
                    );
                    Ok(ClickhouseResponse::new(None, EventStatus::Errored))
                }
            }
        })
    }
}

/// Logs a permanent rejection that could not be dead-lettered (nano, NAN-5808).
fn not_stored(events: usize, holds: bool) {
    error!(
        message = "A batch ClickHouse rejected could not be written to the dead-letter store; reporting it errored instead of acknowledging it.",
        events,
        held_for_replay = holds,
    );
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
            url_query: Some("query=INSERT&date_time_input_format=best_effort".to_owned()),
            event_status: EventStatus::Rejected,
            held_for_replay: false,
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
        assert_eq!(
            meta["replay_url_query"],
            "query=INSERT&date_time_input_format=best_effort"
        );
        assert_eq!(meta["held_for_replay"], false);
        assert_eq!(meta["event_status"], "rejected");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = fs::metadata(dir.join(&bodies[0])).unwrap().permissions().mode();
            assert_eq!(mode & 0o077, 0, "dead-letter files are private: {mode:o}");
        }
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
        call_with_hold(store, status, body, true).await
    }

    async fn call_with_hold(
        store: Option<Arc<DeadLetterStore>>,
        status: u16,
        body: &'static str,
        holds: bool,
    ) -> EventStatus {
        let inner = tower::service_fn(move |_request: HttpRequest<PartitionKey>| async move {
            Ok::<_, crate::Error>(HttpResponse {
                response: http::Response::builder()
                    .status(status)
                    .body(Bytes::from_static(body.as_bytes()))
                    .unwrap(),
                events_byte_size: GroupedCountByteSize::new_untagged(),
                raw_byte_size: 0,
            })
        });
        let mut service = DeadLetterService::new(inner, store, None, holds);
        let response = service
            .call(request(b"{\"a\":1}\n", 1))
            .await
            .expect("the inner service answered");
        response.event_status()
    }

    /// Calls the service with an inner service that fails without a response. `Ok` is the status
    /// the service reported; `Err` means it returned the error (the driver reports `Rejected`).
    async fn call_failing(
        store: Option<Arc<DeadLetterStore>>,
        error: fn() -> crate::Error,
    ) -> Result<EventStatus, ()> {
        let inner = tower::service_fn(move |_request: HttpRequest<PartitionKey>| async move {
            Err::<HttpResponse, crate::Error>(error())
        });
        let mut service = DeadLetterService::new(inner, store, None, true);
        service
            .call(request(b"{\"a\":1}\n", 1))
            .await
            .map(|response| response.event_status())
            .map_err(drop)
    }

    fn open_store(dir: &Path, max_bytes: u64) -> Arc<DeadLetterStore> {
        Arc::new(DeadLetterStore::open(
            dir.to_path_buf(),
            "clickhouse_logs".to_owned(),
            Compression::gzip_default(),
            max_bytes,
        ))
    }

    fn metas(dir: &Path) -> Vec<serde_json::Value> {
        fs::read_dir(dir)
            .map(|entries| {
                entries
                    .map(|e| e.unwrap().path())
                    .filter(|p| p.to_string_lossy().ends_with(".meta.json"))
                    .map(|p| serde_json::from_slice(&fs::read(p).unwrap()).unwrap())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// NAN-5808: no acknowledgement without a durable write. A permanent rejection is reported
    /// `Rejected` (acknowledged) only when its copy is stored; otherwise it is `Errored`, which a
    /// disk buffer that holds `Errored` records keeps.
    #[tokio::test]
    async fn permanent_rejection_that_cannot_be_stored_is_errored() {
        // No store (disabled, or no usable data_dir).
        assert_eq!(call_with(None, 400, "Code: 27. Cannot parse").await, EventStatus::Errored);

        // Larger than the cap.
        let tmp = tempfile::tempdir().unwrap();
        let tiny = open_store(tmp.path(), 8);
        assert_eq!(
            call_with(Some(tiny), 400, "Code: 27. Cannot parse").await,
            EventStatus::Errored
        );
        assert!(metas(tmp.path()).is_empty());

        // The write fails (a directory Vector cannot write to).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let tmp = tempfile::tempdir().unwrap();
            let dir = tmp.path().join("dead_letter");
            fs::create_dir(&dir).unwrap();
            fs::set_permissions(&dir, fs::Permissions::from_mode(0o500)).unwrap();
            // Root ignores the mode; only assert where the directory really is read-only.
            if fs::write(dir.join("probe"), b"x").is_err() {
                let store = open_store(&dir, u64::MAX);
                assert_eq!(
                    call_with(Some(store), 500, "Code: 117. DB::Exception").await,
                    EventStatus::Errored
                );
            }
            fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
        }
    }

    /// NAN-5808: a request that failed without a response. Transient (retriable transport error,
    /// timeout) is `Errored` with a copy; permanent is returned as the error (`Rejected`) only
    /// once its copy is stored, otherwise it is `Errored` too.
    #[tokio::test]
    async fn failed_request_is_errored_unless_permanent_and_stored() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let store = open_store(&dir, u64::MAX);

        let transient = || -> crate::Error { Box::new(vector_http::HttpError::new("connection reset")) };
        assert_eq!(call_failing(Some(Arc::clone(&store)), transient).await, Ok(EventStatus::Errored));
        let timeout = || -> crate::Error { Box::new(tower::timeout::error::Elapsed::new()) };
        assert_eq!(call_failing(Some(Arc::clone(&store)), timeout).await, Ok(EventStatus::Errored));
        let copies = metas(&dir);
        assert_eq!(copies.len(), 2);
        assert!(copies.iter().all(|m| m["event_status"] == "errored" && m["held_for_replay"] == true));

        let permanent = || -> crate::Error {
            Box::new(vector_http::HttpError::from(crate::Error::from("bad request template")))
        };
        assert_eq!(call_failing(Some(Arc::clone(&store)), permanent).await, Err(()));
        assert_eq!(metas(&dir).len(), 3);
        // Not stored: reported `Errored`, never returned as the error the driver would reject.
        assert_eq!(call_failing(None, permanent).await, Ok(EventStatus::Errored));
        // An unknown error is permanent.
        let unknown = || -> crate::Error { crate::Error::from("something else") };
        assert_eq!(call_failing(None, unknown).await, Ok(EventStatus::Errored));
    }

    /// `held_for_replay` is true only when the sink's disk buffer really holds `Errored` records.
    #[tokio::test]
    async fn held_for_replay_follows_the_buffer() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let store = open_store(&dir, u64::MAX);
        assert_eq!(
            call_with_hold(Some(Arc::clone(&store)), 503, "Code: 202. Too many", false).await,
            EventStatus::Errored
        );
        let copies = metas(&dir);
        assert_eq!(copies.len(), 1);
        assert_eq!(copies[0]["held_for_replay"], false);
        assert_eq!(copies[0]["event_status"], "errored");
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

        // A transient failure that outlived its retries stays `Errored` (held for replay), with
        // a copy marked `held_for_replay`.
        let status = call_with(Some(Arc::clone(&store)), 503, "Code: 202. Too many").await;
        assert_eq!(status, EventStatus::Errored);
        assert_eq!(body_files(&dir).len(), 3);

        // An access refusal is retried by the retry logic (NAN-5390), so it is transient too.
        let status = call_with(Some(Arc::clone(&store)), 403, "Code: 497. ACCESS_DENIED").await;
        assert_eq!(status, EventStatus::Errored);
        assert_eq!(body_files(&dir).len(), 4);
        let held = fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.to_string_lossy().ends_with(".meta.json"))
            .map(|p| serde_json::from_slice::<serde_json::Value>(&fs::read(p).unwrap()).unwrap())
            .filter(|meta| meta["held_for_replay"] == true)
            .count();
        assert_eq!(held, 2);

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
        fs::write(dir.join("README.txt"), b"operator notes").unwrap();
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
            dir.join("0000000000000-0000000000-1.ndjson.gz.tmp").exists(),
            "a temporary file may be a write in progress; it is neither counted nor deleted"
        );
        assert!(is_store_file("1794700000000-0000000001-42.ndjson.gz"));
        assert!(!is_store_file("README.txt"));
        assert!(!is_store_file("1794700000000-0000000001-42"));
    }
}
