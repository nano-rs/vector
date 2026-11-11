//! The FDR consumer: receive manifests, fetch their objects, hold each SQS message until
//! everything it names is delivered, then delete it.
//!
//! The engine is generic over [`Queue`] and [`ObjectStore`] so the hold / release / heartbeat
//! behaviour is unit-tested without AWS; `aws.rs` provides the SQS and S3 implementations.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    future::Future,
    panic,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{FutureExt, StreamExt, stream::BoxStream, stream::FuturesUnordered};
use snafu::Snafu;
use tokio::{
    io::AsyncWriteExt as _,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinSet,
    time::{Instant, sleep, sleep_until, timeout},
};
use tokio_util::codec::FramedRead;
use tracing::Instrument;
use vector_common::compression::gzip_multiple_decoder;
use vector_lib::{
    config::{LegacyKey, LogNamespace},
    event::MaybeAsLogMut,
    internal_event::{
        ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Protocol, Registered,
        error_stage, error_type,
    },
    lookup::path,
    source_sender::SendError,
};

use super::{
    CrowdstrikeFdrConfig, LOOKUPS_OUTPUT, SkipAction,
    completion::{BatchGuard, Completions, batch_key},
    cursor::Cursor,
    lines::{Line, LineCodec},
    manifest::{
        ChecksumMode, Expected, Hasher, Manifest, ManifestFile, ObjectKind, parse_checksum,
        parse_manifest,
    },
};
use crate::{
    SourceSender,
    codecs::Decoder,
    common::backoff::ExponentialBackoff,
    event::{
        BatchNotifier, BatchStatus, BatchStatusReceiver, EstimatedJsonEncodedSizeOf, Event,
        LogEvent,
    },
    internal_events::{
        CrowdstrikeFdrBatchLag, CrowdstrikeFdrChecksumWarning, CrowdstrikeFdrError,
        CrowdstrikeFdrFileFetched, CrowdstrikeFdrFilesSkipped, CrowdstrikeFdrHoldExpired,
        CrowdstrikeFdrInflight, CrowdstrikeFdrManifest, CrowdstrikeFdrMetrics,
        CrowdstrikeFdrQueueDepth, EventsReceived, FdrManifestStatus, StreamClosedError,
    },
    shutdown::ShutdownSignal,
};

pub(super) type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub(super) type ByteChunks = BoxStream<'static, Result<Bytes, BoxError>>;

/// A received SQS message.
#[derive(Clone, Debug)]
pub(super) struct QueueMessage {
    pub(super) id: String,
    pub(super) receipt: String,
    pub(super) body: String,
    /// `ApproximateReceiveCount`: 1 on first delivery, 0 when SQS did not report it.
    pub(super) receive_count: u32,
}

/// Queue depth from `GetQueueAttributes`.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct QueueDepth {
    pub(super) visible: Option<u64>,
    pub(super) not_visible: Option<u64>,
}

#[derive(Debug)]
pub(super) enum DepthError {
    /// The credentials may not call `GetQueueAttributes`; stop asking.
    Denied,
    Other(BoxError),
}

#[async_trait]
pub(super) trait Queue: Send + Sync + 'static {
    async fn receive(&self, max: i32, visibility_secs: i32) -> Result<Vec<QueueMessage>, BoxError>;
    async fn delete(&self, receipt: &str) -> Result<(), BoxError>;
    async fn change_visibility(&self, receipt: &str, secs: i32) -> Result<(), BoxError>;
    async fn depth(&self) -> Result<QueueDepth, DepthError>;
}

#[async_trait]
pub(super) trait ObjectStore: Send + Sync + 'static {
    /// Starts a GET; the returned stream yields the object body.
    async fn get(&self, bucket: &str, key: &str) -> Result<ByteChunks, BoxError>;
}

/// Resolved runtime settings (see the config docs for each field).
#[derive(Clone, Debug)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "mirrors independent config options"
)]
pub(super) struct Settings {
    pub(super) max_number_of_messages: i32,
    pub(super) visibility_timeout_secs: i32,
    pub(super) heartbeat_interval: Duration,
    pub(super) max_hold: Duration,
    pub(super) file_concurrency: usize,
    pub(super) max_inflight_manifests: usize,
    /// In KiB, so one acquire fits a `u32`.
    pub(super) max_inflight_kib: Option<u32>,
    pub(super) max_tries: u32,
    pub(super) retry_delay_secs: i32,
    /// Tries per object (GET, read, checksum) before the batch is released.
    pub(super) fetch_max_tries: u32,
    /// Wait after the first failed try of an object; doubles per try, at most 30 s.
    pub(super) fetch_retry_backoff: Duration,
    /// Objects up to this size are held in memory; larger ones are spooled to `spool_dir`.
    pub(super) spool_threshold_bytes: usize,
    /// Total memory for objects held between download and emission; past it, objects spool.
    pub(super) max_memory_bytes: usize,
    pub(super) spool_dir: PathBuf,
    /// Whether the startup probe could write to `spool_dir`. When it could not, objects are
    /// only ever held in memory, and a manifest waits for memory instead of spooling.
    pub(super) spool_usable: bool,
    /// What `start_from` does with an older batch.
    pub(super) skip_action: SkipAction,
    pub(super) checksum: ChecksumMode,
    pub(super) cutoff: Option<DateTime<Utc>>,
    pub(super) route_lookups: bool,
    pub(super) max_line_bytes: usize,
    pub(super) max_object_bytes: usize,
    pub(super) delete_message: bool,
    pub(super) delete_failed_message: bool,
    /// Bound on one AWS call (GET until headers, delete, visibility change).
    pub(super) call_timeout: Duration,
    /// Bound on the gap between two body chunks of a GET.
    pub(super) stream_idle_timeout: Duration,
    pub(super) queue_depth_interval: Option<Duration>,
    pub(super) acknowledgements: bool,
    pub(super) log_namespace: LogNamespace,
}

pub(super) struct Engine<Q, O> {
    queue: Arc<Q>,
    store: Arc<O>,
    settings: Arc<Settings>,
    decoder: Decoder,
    cursor: Option<Arc<Cursor>>,
    completions: Arc<Completions>,
}

/// Shared by every manifest task.
struct Shared<Q, O> {
    queue: Arc<Q>,
    store: Arc<O>,
    settings: Arc<Settings>,
    decoder: Decoder,
    cursor: Option<Arc<Cursor>>,
    completions: Arc<Completions>,
    /// Objects being downloaded or waiting to be emitted, across all manifests.
    file_slots: Arc<Semaphore>,
    /// `max_memory_bytes`, in KiB.
    memory_budget: Arc<Semaphore>,
    /// Keeps the source's metric series registered and from expiring.
    _metrics: CrowdstrikeFdrMetrics,
    byte_budget: Option<Arc<Semaphore>>,
    inflight_manifests: AtomicUsize,
    inflight_bytes: AtomicU64,
    bytes_received: Registered<BytesReceived>,
    events_received: Registered<EventsReceived>,
    pins: Mutex<Pins>,
}

/// Batches the cursor must not pass: held (in flight in this process) and parked ones.
#[derive(Default)]
struct Pins {
    /// Batch time to number of held manifests with that time.
    held: BTreeMap<i64, usize>,
    /// Message id to batch time, for messages left alone after `max_tries`. Kept until restart.
    parked: HashMap<String, i64>,
}

impl Pins {
    fn oldest(&self) -> Option<i64> {
        let held = self.held.keys().next().copied();
        let parked = self.parked.values().min().copied();
        held.into_iter().chain(parked).min()
    }
}

/// Unpins a held batch however its task ends.
struct HeldPin<'a> {
    pins: &'a Mutex<Pins>,
    timestamp_ms: i64,
}

impl<'a> HeldPin<'a> {
    fn new(pins: &'a Mutex<Pins>, timestamp_ms: i64) -> Self {
        *pins
            .lock()
            .expect("pins lock")
            .held
            .entry(timestamp_ms)
            .or_default() += 1;
        Self { pins, timestamp_ms }
    }
}

impl Drop for HeldPin<'_> {
    fn drop(&mut self) {
        let mut pins = self.pins.lock().expect("pins lock");
        if let Some(count) = pins.held.get_mut(&self.timestamp_ms) {
            *count -= 1;
            if *count == 0 {
                pins.held.remove(&self.timestamp_ms);
            }
        }
    }
}

impl<Q: Queue, O: ObjectStore> Engine<Q, O> {
    pub(super) fn new(
        queue: Arc<Q>,
        store: Arc<O>,
        settings: Settings,
        decoder: Decoder,
        cursor: Option<Arc<Cursor>>,
        completions: Arc<Completions>,
    ) -> Self {
        Self {
            queue,
            store,
            settings: Arc::new(settings),
            decoder,
            cursor,
            completions,
        }
    }

    pub(super) async fn run(self, out: SourceSender, shutdown: ShutdownSignal) -> Result<(), ()> {
        let settings = Arc::clone(&self.settings);
        let queue = Arc::clone(&self.queue);
        let shared = Arc::new(self.into_shared());
        let manifest_slots = Arc::new(Semaphore::new(settings.max_inflight_manifests.max(1)));

        let depth_task = settings.queue_depth_interval.map(|interval| {
            tokio::spawn(
                poll_queue_depth(
                    Arc::clone(&queue),
                    interval,
                    settings.call_timeout,
                    shutdown.clone(),
                )
                .in_current_span(),
            )
        });

        let mut tasks = JoinSet::new();
        let mut backoff = ExponentialBackoff::default().max_delay(Duration::from_secs(30));
        let mut stop = shutdown.clone();
        loop {
            // Reap finished manifests (resuming any panic so Vector shuts down properly).
            while let Some(result) = tasks.try_join_next() {
                resume_panic(result);
            }

            let first = tokio::select! {
                _ = &mut stop => break,
                permit = Arc::clone(&manifest_slots).acquire_owned() => {
                    permit.expect("semaphore is never closed")
                }
            };
            let mut permits = more_slots(first, &manifest_slots, settings.max_number_of_messages);

            #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
            let max = permits.len() as i32;
            let received = tokio::select! {
                _ = &mut stop => break,
                received = bounded(
                    settings.call_timeout,
                    queue.receive(max, settings.visibility_timeout_secs),
                ) => received,
            };
            let messages = match received {
                Ok(messages) => {
                    backoff.reset();
                    messages
                }
                Err(error) => {
                    emit!(CrowdstrikeFdrError {
                        error_code: "failed_receiving_messages",
                        error_type: error_type::REQUEST_FAILED,
                        stage: error_stage::RECEIVING,
                        message_id: "",
                        error,
                    });
                    drop(permits);
                    let delay = backoff.next().expect("backoff never ends");
                    tokio::select! {
                        _ = &mut stop => break,
                        () = sleep(delay) => continue,
                    }
                }
            };

            for message in messages {
                let Some(permit) = permits.pop() else {
                    // SQS never returns more than asked; if it does, the extra message simply
                    // re-delivers after its visibility timeout.
                    break;
                };
                tasks.spawn(
                    handle_message(
                        Arc::clone(&shared),
                        message,
                        permit,
                        out.clone(),
                        shutdown.clone(),
                    )
                    .in_current_span(),
                );
            }
        }

        // Stop receiving; let in-flight manifests finish (they stop starting new downloads and
        // keep waiting for acknowledgements until Vector's shutdown deadline).
        while let Some(result) = tasks.join_next().await {
            resume_panic(result);
        }
        if let Some(task) = depth_task {
            task.abort();
        }
        Ok(())
    }
}

impl<Q: Queue, O: ObjectStore> Engine<Q, O> {
    /// The state every manifest task shares. Registers the source's metrics, so call it inside
    /// the component's span.
    fn into_shared(self) -> Shared<Q, O> {
        let settings = self.settings;
        Shared {
            queue: self.queue,
            store: self.store,
            decoder: self.decoder,
            cursor: self.cursor,
            completions: self.completions,
            file_slots: Arc::new(Semaphore::new(settings.file_concurrency.max(1))),
            memory_budget: Arc::new(Semaphore::new(
                (settings.max_memory_bytes / 1024).min(Semaphore::MAX_PERMITS),
            )),
            _metrics: CrowdstrikeFdrMetrics::register(),
            byte_budget: settings
                .max_inflight_kib
                .map(|kib| Arc::new(Semaphore::new(kib as usize))),
            inflight_manifests: AtomicUsize::new(0),
            inflight_bytes: AtomicU64::new(0),
            bytes_received: register!(BytesReceived::from(Protocol::HTTPS)),
            events_received: register!(EventsReceived),
            pins: Mutex::new(Pins::default()),
            settings,
        }
    }
}

/// Adds whatever free slots there are to `first`, up to one receive's worth of messages.
fn more_slots(
    first: OwnedSemaphorePermit,
    slots: &Arc<Semaphore>,
    max_messages: i32,
) -> Vec<OwnedSemaphorePermit> {
    let wanted = usize::try_from(max_messages).unwrap_or(1);
    let mut permits = vec![first];
    while permits.len() < wanted {
        match Arc::clone(slots).try_acquire_owned() {
            Ok(permit) => permits.push(permit),
            Err(_) => break,
        }
    }
    permits
}

fn resume_panic(result: Result<(), tokio::task::JoinError>) {
    if let Err(error) = result
        && error.is_panic()
    {
        panic::resume_unwind(error.into_panic());
    }
}

/// Renders an error with its sources (AWS SDK errors keep the useful part in the chain).
pub(super) fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(inner) = source {
        let inner_text = inner.to_string();
        if !text.contains(&inner_text) {
            text.push_str(": ");
            text.push_str(&inner_text);
        }
        source = inner.source();
    }
    text
}

/// How processing one manifest ended.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// Every event delivered (or dispatched, without acknowledgements).
    Delivered,
    /// A sink rejected events permanently.
    Rejected,
    /// A fetch, decode or delivery failed (the error code says which); re-deliver.
    Failed(&'static str),
    /// `max_hold_secs` elapsed first.
    HoldExpired,
    /// Vector is shutting down; the manifest was not finished.
    Shutdown,
}

#[derive(Debug, Snafu)]
enum FileError {
    #[snafu(display("GET s3://{bucket}/{key} failed: {error}"))]
    Get {
        bucket: String,
        key: String,
        error: String,
    },
    #[snafu(display("GET s3://{bucket}/{key} timed out after {secs}s"))]
    GetTimeout {
        bucket: String,
        key: String,
        secs: u64,
    },
    #[snafu(display("reading s3://{bucket}/{key} failed: {error}"))]
    Read {
        bucket: String,
        key: String,
        error: String,
    },
    #[snafu(display("s3://{bucket}/{key} is larger than max_object_bytes ({max})"))]
    TooLarge {
        bucket: String,
        key: String,
        max: usize,
    },
    #[snafu(display("s3://{bucket}/{key} stalled: no data for {secs}s"))]
    Stalled {
        bucket: String,
        key: String,
        secs: u64,
    },
    #[snafu(display("s3://{bucket}/{key}: {detail}"))]
    Checksum {
        bucket: String,
        key: String,
        detail: String,
    },
    #[snafu(display("buffering s3://{bucket}/{key} on disk failed: {error}"))]
    Spool {
        bucket: String,
        key: String,
        error: String,
    },
    #[snafu(display("decompressing s3://{bucket}/{key} failed: {error}"))]
    Decode {
        bucket: String,
        key: String,
        error: String,
    },
    #[snafu(display("pipeline closed"))]
    Closed,
    #[snafu(display("timed out handing events to the pipeline"))]
    SendTimeout,
    #[snafu(display("shutting down"))]
    ShuttingDown,
}

impl FileError {
    const fn code(&self) -> (&'static str, &'static str) {
        match self {
            Self::Get { .. } => ("failed_fetching_object", error_type::REQUEST_FAILED),
            Self::GetTimeout { .. } => ("failed_fetching_object", error_type::TIMED_OUT),
            Self::Read { .. } => ("failed_reading_object", error_type::READER_FAILED),
            Self::Stalled { .. } => ("object_stream_stalled", error_type::TIMED_OUT),
            Self::TooLarge { .. } => ("object_too_large", error_type::CONDITION_FAILED),
            Self::Checksum { .. } => ("checksum_mismatch", error_type::CONDITION_FAILED),
            Self::Spool { .. } => ("failed_buffering_object", error_type::WRITER_FAILED),
            Self::Decode { .. } => ("failed_decompressing_object", error_type::PARSER_FAILED),
            Self::SendTimeout => ("send_timed_out", error_type::TIMED_OUT),
            Self::Closed | Self::ShuttingDown => ("pipeline_closed", error_type::WRITER_FAILED),
        }
    }

    /// Whether another try of the same object may succeed (a cut or stalled download, a
    /// corrupted transfer). An object too large, or a full disk, fails the same way again.
    const fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Get { .. }
                | Self::GetTimeout { .. }
                | Self::Read { .. }
                | Self::Stalled { .. }
                | Self::Checksum { .. }
        )
    }
}

/// Keeps the in-flight gauges right however a task ends.
struct InflightGuard<'a, Q, O> {
    shared: &'a Shared<Q, O>,
    bytes: u64,
}

impl<'a, Q, O> InflightGuard<'a, Q, O> {
    fn new(shared: &'a Shared<Q, O>) -> Self {
        shared.inflight_manifests.fetch_add(1, Ordering::Relaxed);
        let guard = Self { shared, bytes: 0 };
        guard.publish();
        guard
    }

    fn add_bytes(&mut self, bytes: u64) {
        self.bytes += bytes;
        self.shared
            .inflight_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        self.publish();
    }

    fn publish(&self) {
        emit!(CrowdstrikeFdrInflight {
            manifests: self.shared.inflight_manifests.load(Ordering::Relaxed),
            bytes: self.shared.inflight_bytes.load(Ordering::Relaxed),
        });
    }
}

impl<Q, O> Drop for InflightGuard<'_, Q, O> {
    fn drop(&mut self) {
        self.shared
            .inflight_manifests
            .fetch_sub(1, Ordering::Relaxed);
        self.shared
            .inflight_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
        self.publish();
    }
}

async fn handle_message<Q: Queue, O: ObjectStore>(
    shared: Arc<Shared<Q, O>>,
    message: QueueMessage,
    _slot: OwnedSemaphorePermit,
    out: SourceSender,
    shutdown: ShutdownSignal,
) {
    let settings = &shared.settings;
    let received_at = Instant::now();
    emit!(CrowdstrikeFdrManifest {
        status: FdrManifestStatus::Received,
        message_id: &message.id,
    });

    let parsed = parse_manifest(&message.body);
    if let Ok(manifest) = &parsed {
        // Not for a parked delivery: it re-arrives every visibility timeout until the queue
        // dead-letters it, and would skew the lag of batches actually being read.
        if message.receive_count <= settings.max_tries
            && let Some(batch_time) = manifest.batch_time()
            && let Ok(lag) = (Utc::now() - batch_time).to_std()
        {
            emit!(CrowdstrikeFdrBatchLag { lag });
        }
        // Before the `max_tries` check: a batch `start_from` skips (released, with
        // `skip_action = release`, on every delivery) is skipped, not parked.
        if skip_old_batch(&shared, &message, manifest, &batch_key(manifest)).await {
            return;
        }
    }

    if message.receive_count > settings.max_tries {
        // A parked batch pins the cursor (until restart): it was never delivered.
        if let Ok(manifest) = &parsed {
            shared
                .pins
                .lock()
                .expect("pins lock")
                .parked
                .insert(message.id.clone(), manifest.timestamp_ms);
        }
        emit!(CrowdstrikeFdrManifest {
            status: FdrManifestStatus::Parked,
            message_id: &message.id,
        });
        return;
    }

    let manifest = match parsed {
        Ok(manifest) => manifest,
        Err(error) => {
            emit!(CrowdstrikeFdrError {
                error_code: "invalid_manifest",
                error_type: error_type::PARSER_FAILED,
                stage: error_stage::PROCESSING,
                message_id: &message.id,
                error: error.to_string(),
            });
            release(
                &shared,
                &message,
                settings.retry_delay_secs,
                "invalid_manifest",
            )
            .await;
            return;
        }
    };

    let key = batch_key(&manifest);
    info!(
        message = "FDR batch received.",
        message_id = %message.id,
        files = manifest.files.len(),
        bytes = manifest.total_size,
        batch_timestamp = %batch_timestamp(&manifest),
        receive_count = message.receive_count,
    );

    // Held until this copy of the batch is deleted or released, so a `forget` by another copy
    // of the same batch does not remove the record under it.
    let _batch = shared.completions.begin(&key);
    let pin = HeldPin::new(&shared.pins, manifest.timestamp_ms);
    let mut inflight = InflightGuard::new(&shared);
    let deadline = received_at + settings.max_hold;
    let outcome = tokio::select! {
        outcome = process(&shared, &message.id, &manifest, &key, &mut inflight, out, shutdown) => outcome,
        () = heartbeat(&shared, &message) => unreachable!("heartbeat never returns"),
        () = sleep_until(deadline) => Outcome::HoldExpired,
    };
    drop(inflight);
    drop(pin);
    conclude(&shared, &message, &manifest, &key, received_at, outcome).await;
}

/// Deletes, releases or leaves the message, per how processing ended.
async fn conclude<Q: Queue, O: ObjectStore>(
    shared: &Shared<Q, O>,
    message: &QueueMessage,
    manifest: &Manifest,
    key: &str,
    received_at: Instant,
    outcome: Outcome,
) {
    let settings = &shared.settings;
    match outcome {
        Outcome::Delivered => {
            finish(shared, message, manifest, key, received_at).await;
        }
        Outcome::Rejected if settings.delete_failed_message => {
            warn!(
                message = "Sinks rejected events of an FDR manifest; deleting it (delete_failed_message = true).",
                message_id = %message.id,
            );
            finish(shared, message, manifest, key, received_at).await;
        }
        Outcome::Rejected => {
            release(shared, message, settings.retry_delay_secs, "sink_rejected").await;
        }
        Outcome::Failed(reason) => {
            release(shared, message, settings.retry_delay_secs, reason).await;
        }
        Outcome::HoldExpired => {
            emit!(CrowdstrikeFdrHoldExpired {
                message_id: &message.id,
                held: received_at.elapsed(),
            });
            // The visibility timeout lapses on its own; nothing to call.
            emit!(CrowdstrikeFdrManifest {
                status: FdrManifestStatus::Released,
                message_id: &message.id,
            });
        }
        Outcome::Shutdown => {
            // Hand it straight back so another instance (or the next start) picks it up.
            release(shared, message, 0, "shutdown").await;
        }
    }
}

fn older_than_cutoff(settings: &Settings, manifest: &Manifest) -> bool {
    match (settings.cutoff, manifest.batch_time()) {
        (Some(cutoff), Some(batch_time)) => batch_time < cutoff,
        _ => false,
    }
}

/// Applies `start_from`: returns `true` when the batch is older than the cutoff and was
/// skipped (deleted or released per `skip_action`) without downloading.
async fn skip_old_batch<Q: Queue, O: ObjectStore>(
    shared: &Shared<Q, O>,
    message: &QueueMessage,
    manifest: &Manifest,
    key: &str,
) -> bool {
    let settings = &shared.settings;
    if !older_than_cutoff(settings, manifest) {
        return false;
    }
    match settings.skip_action {
        // Only a first delivery may be deleted. A message received before (count >= 2, or
        // unknown) was accepted by an earlier run, or this one, and is still undelivered:
        // deleting it would lose it.
        SkipAction::Delete if message.receive_count == 1 => {
            if delete(shared, message).await {
                info!(
                    message = "FDR batch older than start_from skipped; message deleted.",
                    message_id = %message.id,
                    files = manifest.files.len(),
                    batch_timestamp = %batch_timestamp(manifest),
                );
                emit!(CrowdstrikeFdrManifest {
                    status: FdrManifestStatus::Skipped,
                    message_id: &message.id,
                });
            }
            true
        }
        SkipAction::Delete => false,
        // Nothing is ever deleted, so any delivery may be skipped, unless this instance has
        // already delivered part of the batch (it then finishes it).
        SkipAction::Release => {
            if !shared.completions.load(key).await.is_empty() {
                return false;
            }
            if let Err(error) = bounded(
                settings.call_timeout,
                shared
                    .queue
                    .change_visibility(&message.receipt, settings.retry_delay_secs),
            )
            .await
            {
                emit!(CrowdstrikeFdrError {
                    error_code: "failed_releasing_message",
                    error_type: error_type::REQUEST_FAILED,
                    stage: error_stage::PROCESSING,
                    message_id: &message.id,
                    error,
                });
            }
            info!(
                message = "FDR batch older than start_from skipped; message released, not deleted (skip_action = release).",
                message_id = %message.id,
                files = manifest.files.len(),
                batch_timestamp = %batch_timestamp(manifest),
                visible_again_in_secs = settings.retry_delay_secs,
            );
            emit!(CrowdstrikeFdrManifest {
                status: FdrManifestStatus::Skipped,
                message_id: &message.id,
            });
            true
        }
    }
}

/// Deletes the message (when configured), forgets its per-file progress and advances the
/// cursor.
async fn finish<Q: Queue, O: ObjectStore>(
    shared: &Shared<Q, O>,
    message: &QueueMessage,
    manifest: &Manifest,
    key: &str,
    received_at: Instant,
) {
    let deleted = if shared.settings.delete_message {
        delete(shared, message).await
    } else {
        true
    };
    if !deleted {
        // Kept: the re-delivery skips every file and deletes.
        return;
    }
    shared.completions.forget(key);
    info!(
        message = "FDR batch delivered; message deleted.",
        message_id = %message.id,
        files = manifest.files.len(),
        batch_timestamp = %batch_timestamp(manifest),
        held_secs = received_at.elapsed().as_secs(),
        deleted = shared.settings.delete_message,
    );
    emit!(CrowdstrikeFdrManifest {
        status: FdrManifestStatus::Acked,
        message_id: &message.id,
    });
    if let Some(cursor) = &shared.cursor
        && let Err(error) = cursor.advance(cursor_target(shared, manifest)).await
    {
        emit!(CrowdstrikeFdrError {
            error_code: "failed_writing_cursor",
            error_type: error_type::WRITER_FAILED,
            stage: error_stage::PROCESSING,
            message_id: &message.id,
            error: error.to_string(),
        });
    }
}

/// How far a delivered batch may move the cursor: up to its own time, but never up to a batch
/// that is still held or parked (it would be skipped as old after a restart).
fn cursor_target<Q, O>(shared: &Shared<Q, O>, manifest: &Manifest) -> i64 {
    let oldest_pinned = shared.pins.lock().expect("pins lock").oldest();
    match oldest_pinned {
        Some(pinned) if pinned <= manifest.timestamp_ms => pinned.saturating_sub(1),
        _ => manifest.timestamp_ms,
    }
}

async fn delete<Q: Queue, O: ObjectStore>(shared: &Shared<Q, O>, message: &QueueMessage) -> bool {
    match bounded(
        shared.settings.call_timeout,
        shared.queue.delete(&message.receipt),
    )
    .await
    {
        Ok(()) => true,
        Err(error) => {
            emit!(CrowdstrikeFdrError {
                error_code: "failed_deleting_message",
                error_type: error_type::REQUEST_FAILED,
                stage: error_stage::PROCESSING,
                message_id: &message.id,
                error,
            });
            false
        }
    }
}

/// The batch time for logs (RFC 3339), or the raw milliseconds when out of range.
fn batch_timestamp(manifest: &Manifest) -> String {
    manifest.batch_time().map_or_else(
        || manifest.timestamp_ms.to_string(),
        |time| time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    )
}

/// Drops the hold: the message becomes visible again after `secs`. `reason` is logged.
async fn release<Q: Queue, O: ObjectStore>(
    shared: &Shared<Q, O>,
    message: &QueueMessage,
    secs: i32,
    reason: &str,
) {
    info!(
        message = "FDR batch released for re-delivery.",
        message_id = %message.id,
        reason,
        visible_again_in_secs = secs,
        receive_count = message.receive_count,
    );
    if let Err(error) = bounded(
        shared.settings.call_timeout,
        shared.queue.change_visibility(&message.receipt, secs),
    )
    .await
    {
        // Not fatal: the visibility timeout lapses anyway.
        emit!(CrowdstrikeFdrError {
            error_code: "failed_releasing_message",
            error_type: error_type::REQUEST_FAILED,
            stage: error_stage::PROCESSING,
            message_id: &message.id,
            error,
        });
    }
    emit!(CrowdstrikeFdrManifest {
        status: FdrManifestStatus::Released,
        message_id: &message.id,
    });
}

/// Extends the message's visibility every `heartbeat_interval`, forever; the caller drops it.
async fn heartbeat<Q: Queue, O: ObjectStore>(shared: &Shared<Q, O>, message: &QueueMessage) {
    let settings = &shared.settings;
    loop {
        sleep(settings.heartbeat_interval).await;
        if let Err(error) = bounded(
            settings.call_timeout,
            shared
                .queue
                .change_visibility(&message.receipt, settings.visibility_timeout_secs),
        )
        .await
        {
            emit!(CrowdstrikeFdrError {
                error_code: "failed_extending_visibility",
                error_type: error_type::REQUEST_FAILED,
                stage: error_stage::PROCESSING,
                message_id: &message.id,
                error,
            });
        }
    }
}

/// Runs an AWS call with a hard deadline on top of the SDK's own timeouts.
async fn bounded<T>(
    limit: Duration,
    call: impl Future<Output = Result<T, BoxError>>,
) -> Result<T, String> {
    match timeout(limit, call).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error_chain(&*error)),
        Err(_) => Err(format!("timed out after {}s", limit.as_secs())),
    }
}

async fn process<Q: Queue, O: ObjectStore>(
    shared: &Arc<Shared<Q, O>>,
    message_id: &str,
    manifest: &Manifest,
    key: &str,
    inflight: &mut InflightGuard<'_, Q, O>,
    out: SourceSender,
    shutdown: ShutdownSignal,
) -> Outcome {
    let settings = &shared.settings;

    // Byte budget: wait (heartbeating) until the manifest fits. A manifest larger than the
    // whole budget takes all of it rather than waiting forever.
    let _budget = match (&shared.byte_budget, settings.max_inflight_kib) {
        (Some(budget), Some(total)) => {
            let kib = u32::try_from(manifest.total_size.div_ceil(1024))
                .unwrap_or(u32::MAX)
                .clamp(1, total.max(1));
            Some(
                Arc::clone(budget)
                    .acquire_many_owned(kib)
                    .await
                    .expect("semaphore is never closed"),
            )
        }
        _ => None,
    };
    inflight.add_bytes(manifest.total_size);

    // A re-delivered batch skips the files an earlier delivery already landed.
    let done = shared.completions.load(key).await;
    let todo: Vec<usize> = manifest
        .files
        .iter()
        .enumerate()
        .filter(|(_, file)| !done.contains(&file.path))
        .map(|(index, _)| index)
        .collect();
    let skipped = manifest.files.len() - todo.len();
    if skipped > 0 {
        emit!(CrowdstrikeFdrFilesSkipped {
            message_id,
            skipped,
            remaining: todo.len(),
        });
    }

    let (acks_tx, acks_rx) = if settings.acknowledgements {
        let (tx, rx) = mpsc::unbounded_channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    // Emission and acknowledgement tracking run side by side; a file is recorded as delivered
    // as soon as its own events are acknowledged. When emission fails, the tracker still waits
    // for the files already emitted, so a re-delivery repeats at most the failed file. The
    // tracker is its own task: when the hold expires (this future is dropped) it keeps
    // recording late acknowledgements until every emitted file has resolved.
    let tracker = acks_rx.map(|acks| {
        tokio::spawn(
            track_acks(
                Arc::clone(shared),
                manifest.files.clone(),
                key.to_owned(),
                acks,
                shared.completions.begin(key),
            )
            .in_current_span(),
        )
    });
    let emitted = emit_files(shared, manifest, key, &todo, acks_tx, out, shutdown).await;
    let status = match tracker {
        None => BatchStatus::Delivered,
        Some(tracker) => match tracker.await {
            Ok(status) => status,
            Err(error) => {
                if error.is_panic() {
                    panic::resume_unwind(error.into_panic());
                }
                // Cancelled with the runtime: nothing is known to be delivered.
                BatchStatus::Errored
            }
        },
    };

    outcome_of(emitted, status, message_id, manifest)
}

/// The manifest's outcome from its emission result and combined acknowledgement status.
fn outcome_of(
    emitted: Result<(), FileError>,
    status: BatchStatus,
    message_id: &str,
    manifest: &Manifest,
) -> Outcome {
    if let Err(error) = emitted {
        if matches!(error, FileError::ShuttingDown) {
            return Outcome::Shutdown;
        }
        if matches!(error, FileError::Closed) {
            emit!(StreamClosedError { count: 0 });
            return Outcome::Shutdown;
        }
        let (code, kind) = error.code();
        emit!(CrowdstrikeFdrError {
            error_code: code,
            error_type: kind,
            stage: error_stage::RECEIVING,
            message_id,
            error: error.to_string(),
        });
        return Outcome::Failed(code);
    }

    match status {
        BatchStatus::Delivered => Outcome::Delivered,
        BatchStatus::Errored => {
            emit!(CrowdstrikeFdrError {
                error_code: "delivery_failed",
                error_type: error_type::ACKNOWLEDGMENT_FAILED,
                stage: error_stage::SENDING,
                message_id,
                error: format!(
                    "sinks reported an error for batch {} ({} files)",
                    manifest.timestamp_ms,
                    manifest.files.len()
                ),
            });
            Outcome::Failed("delivery_failed")
        }
        BatchStatus::Rejected => Outcome::Rejected,
    }
}

/// A file's acknowledgement receiver, by its index in the manifest.
type FileAck = (usize, BatchStatusReceiver);

/// Fetches the `todo` files (up to `file_concurrency` ahead, each in its own task) and emits
/// them one at a time, in manifest order. Each file gets its own batch notifier; its receiver
/// goes to `acks`, or, without acknowledgements, the file is recorded as delivered once emitted.
async fn emit_files<Q: Queue, O: ObjectStore>(
    shared: &Arc<Shared<Q, O>>,
    manifest: &Manifest,
    key: &str,
    todo: &[usize],
    acks: Option<mpsc::UnboundedSender<FileAck>>,
    mut out: SourceSender,
    shutdown: ShutdownSignal,
) -> Result<(), FileError> {
    let window = shared.settings.file_concurrency.max(1);
    let mut next = todo.iter().copied().peekable();
    let mut fetching: VecDeque<(usize, FetchTask)> = VecDeque::with_capacity(window);
    let batch_time = manifest.batch_time();
    loop {
        // Start downloads. A slot is waited for only when nothing of this manifest is in
        // flight: a fetched file holds its slot until it is emitted, so waiting while one is
        // queued could deadlock against our own queue.
        while fetching.len() < window
            && let Some(&index) = next.peek()
        {
            // Without a usable spool directory an object can only be held in memory, so its
            // memory is reserved here, under the same rule as the slot.
            let memory_kib = (!shared.settings.spool_usable)
                .then(|| memory_only_kib(&shared.settings, manifest.files[index].size))
                .flatten();
            let (slot, memory) = if fetching.is_empty() {
                tokio::select! {
                    permits = async {
                        let slot = Arc::clone(&shared.file_slots)
                            .acquire_owned()
                            .await
                            .expect("semaphore is never closed");
                        let memory = match memory_kib {
                            Some(kib) => Some(
                                Arc::clone(&shared.memory_budget)
                                    .acquire_many_owned(kib)
                                    .await
                                    .expect("semaphore is never closed"),
                            ),
                            None => None,
                        };
                        (slot, memory)
                    } => permits,
                    _ = shutdown.clone() => return Err(FileError::ShuttingDown),
                }
            } else {
                let Ok(slot) = Arc::clone(&shared.file_slots).try_acquire_owned() else {
                    break;
                };
                let memory = match memory_kib {
                    Some(kib) => {
                        match Arc::clone(&shared.memory_budget).try_acquire_many_owned(kib) {
                            Ok(permit) => Some(permit),
                            Err(_) => break,
                        }
                    }
                    None => None,
                };
                (slot, memory)
            };
            if shutdown.clone().now_or_never().is_some() {
                return Err(FileError::ShuttingDown);
            }
            next.next();
            let task = tokio::spawn(
                fetch_object(
                    Arc::clone(shared),
                    manifest.bucket.clone(),
                    manifest.files[index].clone(),
                    slot,
                    memory,
                )
                .in_current_span(),
            );
            fetching.push_back((index, FetchTask(task)));
        }

        let Some((index, mut task)) = fetching.pop_front() else {
            return Ok(());
        };
        // A download may be waiting to retry; shutdown must not wait for its tries.
        let joined = tokio::select! {
            joined = &mut task.0 => joined,
            _ = shutdown.clone() => return Err(FileError::ShuttingDown),
        };
        let fetched = match joined {
            Ok(result) => result?,
            Err(error) => {
                if error.is_panic() {
                    panic::resume_unwind(error.into_panic());
                }
                return Err(FileError::ShuttingDown);
            }
        };
        let file = &manifest.files[index];
        let context = FileContext {
            manifest,
            file,
            batch_time,
        };
        let (batch, receiver) = BatchNotifier::maybe_new_with_receiver(acks.is_some());
        emit_object(shared, &context, fetched, batch.as_ref(), &mut out).await?;
        drop(batch);
        match (receiver, &acks) {
            (Some(receiver), Some(acks)) => {
                // The tracker outlives this function; a send cannot fail while it runs.
                drop(acks.send((index, receiver)));
            }
            _ => mark_done(shared, key, file).await,
        }
    }
}

/// Waits for each emitted file's acknowledgements, records delivered files, and returns the
/// combined status. Returns once `acks` is closed and every received file has resolved; `_batch`
/// keeps the batch's record until then.
async fn track_acks<Q: Queue, O: ObjectStore>(
    shared: Arc<Shared<Q, O>>,
    files: Vec<ManifestFile>,
    key: String,
    mut acks: mpsc::UnboundedReceiver<FileAck>,
    _batch: BatchGuard,
) -> BatchStatus {
    let mut status = BatchStatus::Delivered;
    let mut waiting = FuturesUnordered::new();
    let mut open = true;
    loop {
        tokio::select! {
            next_ack = acks.recv(), if open => match next_ack {
                Some((index, receiver)) => waiting.push(async move { (index, receiver.await) }),
                None => open = false,
            },
            Some((index, file_status)) = waiting.next(), if !waiting.is_empty() => {
                if file_status == BatchStatus::Delivered {
                    mark_done(&shared, &key, &files[index]).await;
                }
                status = worst(status, file_status);
            }
            else => break,
        }
    }
    status
}

/// `Rejected` outranks `Errored`, which outranks `Delivered` (as for a single batch).
const fn worst(a: BatchStatus, b: BatchStatus) -> BatchStatus {
    match (a, b) {
        (BatchStatus::Rejected, _) | (_, BatchStatus::Rejected) => BatchStatus::Rejected,
        (BatchStatus::Errored, _) | (_, BatchStatus::Errored) => BatchStatus::Errored,
        _ => BatchStatus::Delivered,
    }
}

async fn mark_done<Q: Queue, O: ObjectStore>(
    shared: &Shared<Q, O>,
    key: &str,
    file: &ManifestFile,
) {
    if let Err(error) = shared.completions.mark(key, &file.path).await {
        // Still recorded in memory; only a restart would repeat this file.
        emit!(CrowdstrikeFdrError {
            error_code: "failed_writing_progress",
            error_type: error_type::WRITER_FAILED,
            stage: error_stage::PROCESSING,
            message_id: "",
            error: format!("recording {} as delivered failed: {error}", file.path),
        });
    }
}

/// A download in its own task, so a busy emitter never leaves a GET unread. Aborted (and its
/// spool file removed) when dropped.
struct FetchTask(tokio::task::JoinHandle<Result<Fetched, FileError>>);

impl Drop for FetchTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A complete, verified object waiting to be emitted. Keeps its download slot (and so its
/// share of memory or disk) until it is emitted.
struct Fetched {
    object: Downloaded,
    slot: OwnedSemaphorePermit,
}

struct Downloaded {
    body: Body,
    gzipped: bool,
}

enum Body {
    /// In memory, holding its share of the memory budget until emitted.
    Memory(Bytes, Option<OwnedSemaphorePermit>),
    Spool(SpoolFile),
}

/// What an object being emitted holds until it is done.
enum Held {
    Memory(#[allow(dead_code, reason = "held for its drop")] Option<OwnedSemaphorePermit>),
    Spool(#[allow(dead_code, reason = "held for its drop")] SpoolFile),
}

/// A spooled object on disk, removed when dropped.
struct SpoolFile {
    path: PathBuf,
}

impl Drop for SpoolFile {
    fn drop(&mut self) {
        drop(std::fs::remove_file(&self.path));
    }
}

/// Extension of spool files, removed at startup if a crash left any.
pub(super) const SPOOL_EXTENSION: &str = "part";

struct SpoolWriter {
    out: tokio::io::BufWriter<tokio::fs::File>,
    file: SpoolFile,
}

impl SpoolWriter {
    async fn create(dir: &std::path::Path) -> std::io::Result<Self> {
        tokio::fs::create_dir_all(dir).await?;
        let path = dir.join(format!("{}.{SPOOL_EXTENSION}", uuid::Uuid::new_v4()));
        let out = tokio::fs::File::create(&path).await?;
        Ok(Self {
            out: tokio::io::BufWriter::with_capacity(SPOOL_BUFFER, out),
            file: SpoolFile { path },
        })
    }

    async fn write(&mut self, chunk: &[u8]) -> std::io::Result<()> {
        self.out.write_all(chunk).await
    }

    async fn finish(mut self) -> std::io::Result<SpoolFile> {
        self.out.flush().await?;
        Ok(self.file)
    }
}

const SPOOL_BUFFER: usize = 256 * 1024;

/// Where a download goes: memory up to `spool_threshold_bytes`, then a spool file.
enum Sink {
    /// Held in memory, within `limit` bytes covered by `permit` (the memory budget), or by a
    /// reservation the caller holds when `permit` is `None`.
    Memory {
        buffer: Vec<u8>,
        limit: usize,
        permit: Option<OwnedSemaphorePermit>,
    },
    Spool(SpoolWriter),
}

impl Sink {
    async fn write(&mut self, chunk: &[u8], dir: &std::path::Path) -> std::io::Result<()> {
        if let Self::Memory { buffer, limit, .. } = self {
            let needed = buffer.len() + chunk.len();
            if needed <= *limit {
                if needed > buffer.capacity() {
                    // Grow exactly, never past the reserved limit (a doubling Vec could take
                    // up to twice the reservation).
                    let target = (buffer.capacity() * 2).max(needed).min(*limit);
                    buffer.reserve_exact(target - buffer.len());
                }
                buffer.extend_from_slice(chunk);
                return Ok(());
            }
            let mut writer = SpoolWriter::create(dir).await?;
            writer.write(buffer).await?;
            // Dropping the memory sink returns its share of the budget.
            *self = Self::Spool(writer);
        }
        match self {
            Self::Spool(writer) => writer.write(chunk).await,
            Self::Memory { .. } => unreachable!("spilled above"),
        }
    }

    async fn finish(self) -> std::io::Result<Body> {
        match self {
            Self::Memory { buffer, permit, .. } => Ok(Body::Memory(Bytes::from(buffer), permit)),
            Self::Spool(writer) => writer.finish().await.map(Body::Spool),
        }
    }
}

/// The memory (KiB) to reserve for an object when nothing can spool: its listed size (even
/// past `spool_threshold_bytes`), or the threshold when unlisted. `None` when it could never
/// fit in `max_memory_bytes`; the download then fails with a spool error.
fn memory_only_kib(settings: &Settings, size: Option<u64>) -> Option<u32> {
    let wanted = match size {
        Some(size) => usize::try_from(size).ok()?,
        None => settings.spool_threshold_bytes,
    };
    let kib = wanted.div_ceil(1024).max(1);
    (kib <= settings.max_memory_bytes / 1024)
        .then(|| u32::try_from(kib).ok())
        .flatten()
}

/// Reserves memory for one object, or `None` to spool it: objects larger than
/// `spool_threshold_bytes` always spool, and so does any object while the memory budget
/// (`max_memory_bytes`) is taken by others. An object of unlisted size reserves the whole
/// threshold. Returns the byte limit the reservation covers.
fn memory_reservation<Q, O>(
    shared: &Shared<Q, O>,
    size: Option<u64>,
) -> Option<(usize, OwnedSemaphorePermit)> {
    let threshold = shared.settings.spool_threshold_bytes;
    let wanted = match size {
        Some(size) if size > threshold as u64 => return None,
        Some(size) => usize::try_from(size).ok()?,
        None => threshold,
    };
    let kib = u32::try_from(wanted.div_ceil(1024).max(1)).ok()?;
    let permit = Arc::clone(&shared.memory_budget)
        .try_acquire_many_owned(kib)
        .ok()?;
    Some(((kib as usize * 1024).min(threshold.max(wanted)), permit))
}

/// Longest wait between two tries of one object.
const MAX_FETCH_RETRY_DELAY: Duration = Duration::from_secs(30);

/// `initial`, doubling with every failed try, at most [`MAX_FETCH_RETRY_DELAY`].
fn fetch_retry_delay(initial: Duration, failed_tries: u32) -> Duration {
    initial
        .saturating_mul(1u32 << failed_tries.saturating_sub(1).min(16))
        .min(MAX_FETCH_RETRY_DELAY)
}

/// Fetches one object completely and verifies it, retrying transient failures up to
/// `fetch_max_tries` times. Nothing of the object is emitted here.
async fn fetch_object<Q: Queue, O: ObjectStore>(
    shared: Arc<Shared<Q, O>>,
    bucket: String,
    file: ManifestFile,
    slot: OwnedSemaphorePermit,
    memory: Option<OwnedSemaphorePermit>,
) -> Result<Fetched, FileError> {
    let settings = &shared.settings;
    let key = &file.path;
    // A reservation made by the manifest (no usable spool) is kept across tries: giving it
    // back between tries could leave this object, the head of its manifest, waiting for memory
    // held by the objects queued behind it.
    let preset = memory.as_ref().map(|permit| permit.num_permits() * 1024);
    let mut memory = memory;

    // Fail closed before downloading when the checksum cannot be verified at all.
    let expected = match settings.checksum {
        ChecksumMode::Off => None,
        mode => match parse_checksum(file.checksum.as_deref()) {
            Ok(expected) => Some(expected),
            Err(detail) if mode == ChecksumMode::Verify => {
                return Err(FileError::Checksum {
                    bucket,
                    key: key.clone(),
                    detail,
                });
            }
            Err(detail) => {
                emit!(CrowdstrikeFdrChecksumWarning { key, detail });
                None
            }
        },
    };

    let max_tries = settings.fetch_max_tries.max(1);
    let mut tries = 1;
    loop {
        match download(&shared, &bucket, &file, expected.as_ref(), preset).await {
            Ok((mut object, bytes)) => {
                if let Body::Memory(_, held @ None) = &mut object.body {
                    *held = memory.take();
                }
                emit!(CrowdstrikeFdrFileFetched { key, bytes });
                return Ok(Fetched { object, slot });
            }
            Err(error) if error.is_transient() && tries < max_tries => {
                let delay = fetch_retry_delay(settings.fetch_retry_backoff, tries);
                warn!(
                    message = "Fetching an FDR object failed; retrying.",
                    key = %key,
                    tries,
                    max_tries,
                    retry_in_secs = delay.as_secs_f64(),
                    error = %error,
                );
                sleep(delay).await;
                tries += 1;
            }
            Err(error) => {
                if error.is_transient() {
                    warn!(
                        message = "Giving up on an FDR object; its batch will be released.",
                        key = %key,
                        tries,
                        error = %error,
                    );
                }
                return Err(error);
            }
        }
    }
}

/// One GET of a whole object, into memory or (past `spool_threshold_bytes`) a spool file,
/// hashing as it arrives, then checks the digest and size of the complete object. Every wait
/// is bounded: the GET by `call_timeout`, each body chunk by `stream_idle_timeout`.
async fn download<Q: Queue, O: ObjectStore>(
    shared: &Shared<Q, O>,
    bucket: &str,
    file: &ManifestFile,
    expected: Option<&Expected>,
    preset: Option<usize>,
) -> Result<(Downloaded, u64), FileError> {
    let settings = &shared.settings;
    let key = file.path.as_str();
    let mut body = match timeout(settings.call_timeout, shared.store.get(bucket, key)).await {
        Ok(Ok(body)) => body,
        Ok(Err(error)) => {
            return Err(FileError::Get {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                error: error_chain(&*error),
            });
        }
        Err(_) => {
            return Err(FileError::GetTimeout {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                secs: settings.call_timeout.as_secs(),
            });
        }
    };

    let spool_error = |error: std::io::Error| FileError::Spool {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        error: format!("{error} (in {})", settings.spool_dir.display()),
    };
    let reservation = match preset {
        Some(limit) => Some((limit, None)),
        None => memory_reservation(shared, file.size).map(|(limit, permit)| (limit, Some(permit))),
    };
    let mut sink = match reservation {
        Some((limit, permit)) => Sink::Memory {
            buffer: Vec::with_capacity(
                file.size
                    .and_then(|size| usize::try_from(size).ok())
                    .unwrap_or(0)
                    .min(limit),
            ),
            limit,
            permit,
        },
        None => Sink::Spool(
            SpoolWriter::create(&settings.spool_dir)
                .await
                .map_err(spool_error)?,
        ),
    };
    let mut hasher = expected.map(Hasher::for_expected);
    let mut len = 0usize;
    let mut head: Vec<u8> = Vec::with_capacity(2);
    loop {
        match timeout(settings.stream_idle_timeout, body.next()).await {
            Err(_) => {
                return Err(FileError::Stalled {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                    secs: settings.stream_idle_timeout.as_secs(),
                });
            }
            Ok(None) => break,
            Ok(Some(Err(error))) => {
                return Err(FileError::Read {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                    error: error_chain(&*error),
                });
            }
            Ok(Some(Ok(chunk))) => {
                if len + chunk.len() > settings.max_object_bytes {
                    return Err(FileError::TooLarge {
                        bucket: bucket.to_owned(),
                        key: key.to_owned(),
                        max: settings.max_object_bytes,
                    });
                }
                if let Some(hasher) = hasher.as_mut() {
                    hasher.update(&chunk);
                }
                if head.len() < 2 {
                    head.extend_from_slice(&chunk[..chunk.len().min(2 - head.len())]);
                }
                len += chunk.len();
                sink.write(&chunk, &settings.spool_dir)
                    .await
                    .map_err(spool_error)?;
            }
        }
    }
    drop(body);

    verify_object(settings, bucket, file, hasher, expected, len)?;

    let body = sink.finish().await.map_err(spool_error)?;
    Ok((
        Downloaded {
            body,
            gzipped: head == [0x1f, 0x8b],
        },
        len as u64,
    ))
}

/// Checks the digest and size of a complete object. With `checksum = warn`, a mismatch is only
/// reported.
fn verify_object(
    settings: &Settings,
    bucket: &str,
    file: &ManifestFile,
    hasher: Option<Hasher>,
    expected: Option<&Expected>,
    len: usize,
) -> Result<(), FileError> {
    let key = file.path.as_str();
    if let (Some(hasher), Some(expected)) = (hasher, expected) {
        let verdict = hasher.verify(expected).and_then(|()| match file.size {
            Some(size) if size != len as u64 => Err(format!(
                "size mismatch: manifest {size} bytes, object {len} bytes"
            )),
            _ => Ok(()),
        });
        if let Err(detail) = verdict {
            if settings.checksum == ChecksumMode::Verify {
                return Err(FileError::Checksum {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                    detail,
                });
            }
            emit!(CrowdstrikeFdrError {
                error_code: "checksum_mismatch",
                error_type: error_type::CONDITION_FAILED,
                stage: error_stage::RECEIVING,
                message_id: "",
                error: detail.clone(),
            });
            emit!(CrowdstrikeFdrChecksumWarning { key, detail });
        }
    }
    Ok(())
}

const SEND_CHUNK: usize = 1000;

/// Decodes a fetched object and sends its events, then releases its download slot.
async fn emit_object<Q: Queue, O: ObjectStore>(
    shared: &Shared<Q, O>,
    context: &FileContext<'_>,
    fetched: Fetched,
    batch: Option<&BatchNotifier>,
    out: &mut SourceSender,
) -> Result<(), FileError> {
    let settings = &shared.settings;
    let Fetched { object, slot } = fetched;
    let gzipped = object.gzipped;
    // `held` keeps the object's memory reservation or spool file until it is emitted.
    let (reader, held): (Box<dyn tokio::io::AsyncRead + Send + Unpin>, Held) = match object.body {
        Body::Memory(data, permit) if gzipped => (
            Box::new(gzip_multiple_decoder(std::io::Cursor::new(data))),
            Held::Memory(permit),
        ),
        Body::Memory(data, permit) => (Box::new(std::io::Cursor::new(data)), Held::Memory(permit)),
        Body::Spool(spool) => {
            let file =
                tokio::fs::File::open(&spool.path)
                    .await
                    .map_err(|error| FileError::Spool {
                        bucket: context.manifest.bucket.clone(),
                        key: context.file.path.clone(),
                        error: format!("{error} (reading {})", spool.path.display()),
                    })?;
            let buffered = tokio::io::BufReader::with_capacity(SPOOL_BUFFER, file);
            let reader: Box<dyn tokio::io::AsyncRead + Send + Unpin> = if gzipped {
                Box::new(gzip_multiple_decoder(buffered))
            } else {
                Box::new(buffered)
            };
            (reader, Held::Spool(spool))
        }
    };
    let mut lines = FramedRead::new(reader, LineCodec::new(settings.max_line_bytes));
    let port = (settings.route_lookups && context.file.kind == ObjectKind::Lookup)
        .then_some(LOOKUPS_OUTPUT);

    let mut pending: Vec<Event> = Vec::with_capacity(SEND_CHUNK);
    while let Some(line) = lines.next().await {
        let Line { number, bytes } = line.map_err(|error| FileError::Decode {
            bucket: context.manifest.bucket.clone(),
            key: context.file.path.clone(),
            error: error.to_string(),
        })?;
        let Some(bytes) = bytes else {
            emit!(CrowdstrikeFdrError {
                error_code: "line_too_long",
                error_type: error_type::PARSER_FAILED,
                stage: error_stage::PROCESSING,
                message_id: "",
                error: format!(
                    "dropped line {number} of s3://{}/{}: longer than max_line_bytes ({})",
                    context.manifest.bucket, context.file.path, settings.max_line_bytes
                ),
            });
            continue;
        };
        if bytes.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        shared.bytes_received.emit(ByteSize(bytes.len()));
        // Decode errors are emitted by the decoder itself.
        let Ok((events, _)) = shared.decoder.deserializer_parse(bytes) else {
            continue;
        };
        let now = Utc::now();
        for mut event in events {
            event = event.with_batch_notifier_option(&batch.cloned());
            if let Some(log) = event.maybe_as_log_mut() {
                insert_metadata(log, settings.log_namespace, context, number, now);
            }
            shared
                .events_received
                .emit(CountByteSize(1, event.estimated_json_encoded_size_of()));
            pending.push(event);
        }
        if pending.len() >= SEND_CHUNK {
            send(out, port, &mut pending).await?;
            // Decoding an in-memory object never waits; let the heartbeat (same task) run.
            tokio::task::yield_now().await;
        }
    }
    if !pending.is_empty() {
        send(out, port, &mut pending).await?;
    }
    drop(lines);
    drop(held);
    drop(slot);
    Ok(())
}

/// Per-file values copied into every event.
struct FileContext<'a> {
    manifest: &'a Manifest,
    file: &'a ManifestFile,
    batch_time: Option<DateTime<Utc>>,
}

async fn send(
    out: &mut SourceSender,
    port: Option<&str>,
    pending: &mut Vec<Event>,
) -> Result<(), FileError> {
    let events = std::mem::replace(pending, Vec::with_capacity(SEND_CHUNK));
    let result = match port {
        Some(port) => out.send_batch_named(port, events).await,
        None => out.send_batch(events).await,
    };
    match result {
        Ok(()) => Ok(()),
        Err(SendError::Closed) => Err(FileError::Closed),
        // The pipeline is up but did not take the events in time: a failure, retried later.
        Err(SendError::Timeout) => Err(FileError::SendTimeout),
    }
}

fn insert_metadata(
    log: &mut LogEvent,
    log_namespace: LogNamespace,
    context: &FileContext<'_>,
    line: u64,
    now: DateTime<Utc>,
) {
    let name = CrowdstrikeFdrConfig::NAME;
    log_namespace.insert_standard_vector_source_metadata(log, name, now);
    log_namespace.insert_source_metadata(
        name,
        log,
        Some(LegacyKey::InsertIfEmpty(path!("fdr", "file"))),
        path!("file"),
        context.file.path.clone(),
    );
    log_namespace.insert_source_metadata(
        name,
        log,
        Some(LegacyKey::InsertIfEmpty(path!("fdr", "line"))),
        path!("line"),
        i64::try_from(line).unwrap_or(i64::MAX),
    );
    log_namespace.insert_source_metadata(
        name,
        log,
        Some(LegacyKey::InsertIfEmpty(path!("fdr", "cid"))),
        path!("cid"),
        context.manifest.cid.clone(),
    );
    log_namespace.insert_source_metadata(
        name,
        log,
        Some(LegacyKey::InsertIfEmpty(path!("fdr", "bucket"))),
        path!("bucket"),
        context.manifest.bucket.clone(),
    );
    log_namespace.insert_source_metadata(
        name,
        log,
        Some(LegacyKey::InsertIfEmpty(path!("fdr", "kind"))),
        path!("kind"),
        context.file.kind.as_str(),
    );
    if let Some(batch_time) = context.batch_time {
        log_namespace.insert_source_metadata(
            name,
            log,
            Some(LegacyKey::InsertIfEmpty(path!("fdr", "batch_timestamp"))),
            path!("batch_timestamp"),
            batch_time,
        );
    }
}

async fn poll_queue_depth<Q: Queue>(
    queue: Arc<Q>,
    interval: Duration,
    call_timeout: Duration,
    shutdown: ShutdownSignal,
) {
    let mut shutdown = shutdown;
    loop {
        let depth = timeout(call_timeout, queue.depth())
            .await
            .unwrap_or_else(|_| Err(DepthError::Other("timed out".into())));
        match depth {
            Ok(depth) => emit!(CrowdstrikeFdrQueueDepth {
                visible: depth.visible,
                not_visible: depth.not_visible,
            }),
            Err(DepthError::Denied) => {
                info!(
                    message = "GetQueueAttributes is denied for these credentials; the queue depth gauge is disabled."
                );
                return;
            }
            Err(DepthError::Other(error)) => {
                debug!(message = "Failed to read FDR queue depth.", error = %error_chain(&*error));
            }
        }
        tokio::select! {
            _ = &mut shutdown => return,
            () = sleep(interval) => {}
        }
    }
}
