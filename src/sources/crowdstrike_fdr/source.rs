//! The FDR consumer: receive manifests, fetch their objects, hold each SQS message until
//! everything it names is delivered, then delete it.
//!
//! The engine is generic over [`Queue`] and [`ObjectStore`] so the hold / release / heartbeat
//! behaviour is unit-tested without AWS; `aws.rs` provides the SQS and S3 implementations.

use std::{
    future::Future,
    panic,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::{FutureExt, StreamExt, stream::BoxStream};
use snafu::Snafu;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
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
    CrowdstrikeFdrConfig, LOOKUPS_OUTPUT,
    cursor::Cursor,
    lines::{Line, LineCodec},
    manifest::{
        ChecksumMode, Hasher, Manifest, ManifestFile, ObjectKind, parse_checksum, parse_manifest,
    },
};
use crate::{
    SourceSender,
    codecs::Decoder,
    common::backoff::ExponentialBackoff,
    event::{BatchNotifier, BatchStatus, EstimatedJsonEncodedSizeOf, Event, LogEvent},
    internal_events::{
        CrowdstrikeFdrBatchLag, CrowdstrikeFdrChecksumWarning, CrowdstrikeFdrError,
        CrowdstrikeFdrFileFetched, CrowdstrikeFdrHoldExpired, CrowdstrikeFdrInflight,
        CrowdstrikeFdrManifest, CrowdstrikeFdrQueueDepth, EventsReceived, FdrManifestStatus,
        StreamClosedError,
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
    /// `ApproximateReceiveCount`; 1 on first delivery.
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
}

/// Shared by every manifest task.
struct Shared<Q, O> {
    queue: Arc<Q>,
    store: Arc<O>,
    settings: Arc<Settings>,
    decoder: Decoder,
    cursor: Option<Arc<Cursor>>,
    file_slots: Arc<Semaphore>,
    byte_budget: Option<Arc<Semaphore>>,
    inflight_manifests: AtomicUsize,
    inflight_bytes: AtomicU64,
    bytes_received: Registered<BytesReceived>,
    events_received: Registered<EventsReceived>,
}

impl<Q: Queue, O: ObjectStore> Engine<Q, O> {
    pub(super) fn new(
        queue: Arc<Q>,
        store: Arc<O>,
        settings: Settings,
        decoder: Decoder,
        cursor: Option<Arc<Cursor>>,
    ) -> Self {
        Self {
            queue,
            store,
            settings: Arc::new(settings),
            decoder,
            cursor,
        }
    }

    pub(super) async fn run(self, out: SourceSender, shutdown: ShutdownSignal) -> Result<(), ()> {
        let settings = Arc::clone(&self.settings);
        let shared = Arc::new(Shared {
            queue: Arc::clone(&self.queue),
            store: self.store,
            settings: Arc::clone(&settings),
            decoder: self.decoder,
            cursor: self.cursor,
            file_slots: Arc::new(Semaphore::new(settings.file_concurrency.max(1))),
            byte_budget: settings
                .max_inflight_kib
                .map(|kib| Arc::new(Semaphore::new(kib as usize))),
            inflight_manifests: AtomicUsize::new(0),
            inflight_bytes: AtomicU64::new(0),
            bytes_received: register!(BytesReceived::from(Protocol::HTTPS)),
            events_received: register!(EventsReceived),
        });
        let manifest_slots = Arc::new(Semaphore::new(settings.max_inflight_manifests.max(1)));

        let depth_task = settings.queue_depth_interval.map(|interval| {
            tokio::spawn(
                poll_queue_depth(
                    Arc::clone(&self.queue),
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
                    self.queue.receive(max, settings.visibility_timeout_secs),
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
    /// A fetch, decode or delivery failed; re-deliver.
    Failed,
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
            Self::Decode { .. } => ("failed_decompressing_object", error_type::PARSER_FAILED),
            Self::SendTimeout => ("send_timed_out", error_type::TIMED_OUT),
            Self::Closed | Self::ShuttingDown => ("pipeline_closed", error_type::WRITER_FAILED),
        }
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

    if message.receive_count > settings.max_tries {
        emit!(CrowdstrikeFdrManifest {
            status: FdrManifestStatus::Parked,
            message_id: &message.id,
        });
        return;
    }

    let manifest = match parse_manifest(&message.body) {
        Ok(manifest) => manifest,
        Err(error) => {
            emit!(CrowdstrikeFdrError {
                error_code: "invalid_manifest",
                error_type: error_type::PARSER_FAILED,
                stage: error_stage::PROCESSING,
                message_id: &message.id,
                error: error.to_string(),
            });
            release(&shared, &message, settings.retry_delay_secs).await;
            return;
        }
    };

    if let Some(batch_time) = manifest.batch_time() {
        if let Ok(lag) = (Utc::now() - batch_time).to_std() {
            emit!(CrowdstrikeFdrBatchLag { lag });
        }
        if settings.cutoff.is_some_and(|cutoff| batch_time < cutoff) {
            if delete(&shared, &message).await {
                emit!(CrowdstrikeFdrManifest {
                    status: FdrManifestStatus::Skipped,
                    message_id: &message.id,
                });
            }
            return;
        }
    }

    let mut inflight = InflightGuard::new(&shared);
    let deadline = received_at + settings.max_hold;
    let outcome = tokio::select! {
        outcome = process(&shared, &message.id, &manifest, &mut inflight, out, shutdown) => outcome,
        () = heartbeat(&shared, &message) => unreachable!("heartbeat never returns"),
        () = sleep_until(deadline) => Outcome::HoldExpired,
    };
    drop(inflight);

    match outcome {
        Outcome::Delivered => {
            finish(&shared, &message, &manifest).await;
        }
        Outcome::Rejected if settings.delete_failed_message => {
            warn!(
                message = "Sinks rejected events of an FDR manifest; deleting it (delete_failed_message = true).",
                message_id = %message.id,
            );
            finish(&shared, &message, &manifest).await;
        }
        Outcome::Rejected | Outcome::Failed => {
            release(&shared, &message, settings.retry_delay_secs).await;
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
            release(&shared, &message, 0).await;
        }
    }
}

/// Deletes the message (when configured) and advances the cursor.
async fn finish<Q: Queue, O: ObjectStore>(
    shared: &Shared<Q, O>,
    message: &QueueMessage,
    manifest: &Manifest,
) {
    let deleted = if shared.settings.delete_message {
        delete(shared, message).await
    } else {
        true
    };
    if !deleted {
        return;
    }
    emit!(CrowdstrikeFdrManifest {
        status: FdrManifestStatus::Acked,
        message_id: &message.id,
    });
    if let Some(cursor) = &shared.cursor
        && let Err(error) = cursor.advance(manifest.timestamp_ms).await
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

/// Drops the hold: the message becomes visible again after `secs`.
async fn release<Q: Queue, O: ObjectStore>(
    shared: &Shared<Q, O>,
    message: &QueueMessage,
    secs: i32,
) {
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
    shared: &Shared<Q, O>,
    message_id: &str,
    manifest: &Manifest,
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

    let (batch, receiver) = BatchNotifier::maybe_new_with_receiver(settings.acknowledgements);
    let fetches: Vec<_> = manifest
        .files
        .iter()
        .map(|file| {
            fetch_file(
                shared,
                manifest,
                file,
                batch.clone(),
                out.clone(),
                shutdown.clone(),
            )
        })
        .collect();
    let mut fetches =
        futures::stream::iter(fetches).buffer_unordered(settings.file_concurrency.max(1));
    let mut failure = None;
    while let Some(result) = fetches.next().await {
        if let Err(error) = result {
            failure = Some(error);
            break;
        }
    }
    drop(fetches);
    drop(batch);

    if let Some(error) = failure {
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
        return Outcome::Failed;
    }

    match receiver {
        None => Outcome::Delivered,
        Some(receiver) => match receiver.await {
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
                Outcome::Failed
            }
            BatchStatus::Rejected => Outcome::Rejected,
        },
    }
}

/// Per-file values copied into every event.
struct FileContext<'a> {
    manifest: &'a Manifest,
    file: &'a ManifestFile,
    batch_time: Option<DateTime<Utc>>,
}

async fn fetch_file<Q: Queue, O: ObjectStore>(
    shared: &Shared<Q, O>,
    manifest: &Manifest,
    file: &ManifestFile,
    batch: Option<BatchNotifier>,
    mut out: SourceSender,
    shutdown: ShutdownSignal,
) -> Result<(), FileError> {
    let settings = &shared.settings;
    let _slot = shared
        .file_slots
        .acquire()
        .await
        .expect("semaphore is never closed");
    if shutdown.clone().now_or_never().is_some() {
        return Err(FileError::ShuttingDown);
    }

    let bucket = &manifest.bucket;
    let key = &file.path;

    // Fail closed before downloading when the checksum cannot be verified at all.
    let expected = match settings.checksum {
        ChecksumMode::Off => None,
        mode => match parse_checksum(file.checksum.as_deref()) {
            Ok(expected) => Some(expected),
            Err(detail) if mode == ChecksumMode::Verify => {
                return Err(FileError::Checksum {
                    bucket: bucket.clone(),
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

    let mut hasher = expected.as_ref().map(Hasher::for_expected);
    let data = download(shared, bucket, key, file.size, hasher.as_mut()).await?;
    emit!(CrowdstrikeFdrFileFetched {
        key,
        bytes: data.len() as u64,
    });

    if let (Some(hasher), Some(expected)) = (hasher, expected.as_ref()) {
        let verdict = hasher.verify(expected).and_then(|()| match file.size {
            Some(size) if size != data.len() as u64 => Err(format!(
                "size mismatch: manifest {size} bytes, object {} bytes",
                data.len()
            )),
            _ => Ok(()),
        });
        if let Err(detail) = verdict {
            if settings.checksum == ChecksumMode::Verify {
                return Err(FileError::Checksum {
                    bucket: bucket.clone(),
                    key: key.clone(),
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

    let context = FileContext {
        manifest,
        file,
        batch_time: manifest.batch_time(),
    };
    emit_lines(shared, &context, data.freeze(), batch.as_ref(), &mut out).await
}

/// GETs one object into memory, hashing as it arrives. Every wait is bounded: the GET by
/// `call_timeout`, each body chunk by `stream_idle_timeout`.
async fn download<Q: Queue, O: ObjectStore>(
    shared: &Shared<Q, O>,
    bucket: &str,
    key: &str,
    size: Option<u64>,
    mut hasher: Option<&mut Hasher>,
) -> Result<BytesMut, FileError> {
    let settings = &shared.settings;
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

    let capacity = size
        .and_then(|size| usize::try_from(size).ok())
        .unwrap_or(0)
        .min(64 * 1024 * 1024);
    let mut data = BytesMut::with_capacity(capacity);
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
                if let Some(hasher) = hasher.as_deref_mut() {
                    hasher.update(&chunk);
                }
                if data.len() + chunk.len() > settings.max_object_bytes {
                    return Err(FileError::TooLarge {
                        bucket: bucket.to_owned(),
                        key: key.to_owned(),
                        max: settings.max_object_bytes,
                    });
                }
                data.extend_from_slice(&chunk);
            }
        }
    }
    drop(body);
    Ok(data)
}

const SEND_CHUNK: usize = 1000;

async fn emit_lines<Q: Queue, O: ObjectStore>(
    shared: &Shared<Q, O>,
    context: &FileContext<'_>,
    data: Bytes,
    batch: Option<&BatchNotifier>,
    out: &mut SourceSender,
) -> Result<(), FileError> {
    let settings = &shared.settings;
    let gzipped = data.starts_with(&[0x1f, 0x8b]);
    let reader: Box<dyn tokio::io::AsyncRead + Send + Unpin> = if gzipped {
        Box::new(gzip_multiple_decoder(std::io::Cursor::new(data)))
    } else {
        Box::new(std::io::Cursor::new(data))
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
        }
    }
    if !pending.is_empty() {
        send(out, port, &mut pending).await?;
    }
    Ok(())
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
