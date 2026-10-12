use std::{
    collections::{HashMap, HashSet},
    future::ready,
    num::NonZeroUsize,
    panic,
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use aws_sdk_s3::{Client as S3Client, operation::get_object::GetObjectError};
use aws_sdk_sqs::{
    Client as SqsClient,
    operation::{
        delete_message_batch::{DeleteMessageBatchError, DeleteMessageBatchOutput},
        receive_message::ReceiveMessageError,
        send_message_batch::{SendMessageBatchError, SendMessageBatchOutput},
    },
    types::{DeleteMessageBatchRequestEntry, Message, SendMessageBatchRequestEntry},
};
use aws_smithy_runtime_api::client::{orchestrator::HttpResponse, result::SdkError};
use aws_types::region::Region;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use futures::{FutureExt, Stream, StreamExt, TryFutureExt};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::serde_as;
use smallvec::SmallVec;
use snafu::{ResultExt, Snafu};
use tokio::select;
use tokio_util::codec::FramedRead;
use vector_lib::{
    codecs::decoding::FramingError,
    config::{LegacyKey, LogNamespace, log_schema},
    configurable::configurable_component,
    event::MaybeAsLogMut,
    internal_event::{
        ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Protocol, Registered,
    },
    lookup::{PathPrefix, metadata_path, path},
    source_sender::SendError,
};

use crate::{
    SourceSender,
    aws::AwsTimeout,
    codecs::Decoder,
    common::backoff::ExponentialBackoff,
    config::{SourceAcknowledgementsConfig, SourceContext},
    event::{BatchNotifier, BatchStatus, EstimatedJsonEncodedSizeOf, Event, LogEvent},
    internal_events::{
        EventsReceived, S3ObjectProcessingFailed, S3ObjectProcessingSucceeded,
        SqsMessageDeleteBatchError, SqsMessageDeletePartialError, SqsMessageDeleteSucceeded,
        SqsMessageProcessingError, SqsMessageProcessingSucceeded, SqsMessageReceiveError,
        SqsMessageReceiveSucceeded, SqsMessageSendBatchError, SqsMessageSentPartialError,
        SqsMessageSentSucceeded, SqsS3EventRecordInvalidEventIgnored, StreamClosedError,
    },
    line_agg::{self, LineAgg},
    shutdown::ShutdownSignal,
    sources::{
        aws_s3::AwsS3Config,
        aws_sqs::release::{self, SHUTDOWN_ACK_TIMEOUT, SqsRelease},
    },
    tls::TlsConfig,
};

static SUPPORTED_S3_EVENT_VERSION: LazyLock<semver::VersionReq> =
    LazyLock::new(|| semver::VersionReq::parse("~2").unwrap());

/// Configuration for deferring events based on their age.
#[serde_as]
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct DeferredConfig {
    /// The URL of the queue to forward events to when they are older than `max_age_secs`.
    #[configurable(metadata(
        docs::examples = "https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue"
    ))]
    #[configurable(validation(format = "uri"))]
    pub(super) queue_url: String,

    /// Event must have been emitted within the last `max_age_secs` seconds to be processed.
    ///
    /// If the event is older, it is forwarded to the `queue_url` for later processing.
    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[configurable(metadata(docs::examples = 3600))]
    pub(super) max_age_secs: u64,
}

/// SQS configuration options.
//
// TODO: It seems awfully likely that we could re-use the existing configuration type for the `aws_sqs` source in some
// way, given the near 100% overlap in configurable values.
#[serde_as]
#[configurable_component]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    /// The URL of the SQS queue to poll for bucket notifications.
    #[configurable(metadata(
        docs::examples = "https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue"
    ))]
    #[configurable(validation(format = "uri"))]
    pub(super) queue_url: String,

    /// How long to wait while polling the queue for new messages, in seconds.
    ///
    /// Generally, this should not be changed unless instructed to do so, as if messages are available,
    /// they are always consumed, regardless of the value of `poll_secs`.
    // NOTE: We restrict this to u32 for safe conversion to i32 later.
    // NOTE: This value isn't used as a `Duration` downstream, so we don't bother using `serde_with`
    #[serde(default = "default_poll_secs")]
    #[derivative(Default(value = "default_poll_secs()"))]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub(super) poll_secs: u32,

    /// The visibility timeout to use for messages, in seconds.
    ///
    /// This controls how long a message is left unavailable after it is received. If a message is received, and
    /// takes longer than `visibility_timeout_secs` to process and delete the message from the queue, it is made available again for another consumer.
    ///
    /// This can happen if there is an issue between consuming a message and deleting it.
    // NOTE: We restrict this to u32 for safe conversion to i32 later.
    // NOTE: This value isn't used as a `Duration` downstream, so we don't bother using `serde_with`
    #[serde(default = "default_visibility_timeout_secs")]
    #[derivative(Default(value = "default_visibility_timeout_secs()"))]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[configurable(metadata(docs::human_name = "Visibility Timeout"))]
    pub(super) visibility_timeout_secs: u32,

    /// Whether to delete the message once it is processed.
    ///
    /// It can be useful to set this to `false` for debugging or during the initial setup.
    #[serde(default = "default_true")]
    #[derivative(Default(value = "default_true()"))]
    pub(super) delete_message: bool,

    /// Whether to delete non-retryable messages.
    ///
    /// If a message is rejected by the sink and not retryable, it is deleted from the queue.
    #[serde(default = "default_true")]
    #[derivative(Default(value = "default_true()"))]
    pub(super) delete_failed_message: bool,

    /// Number of concurrent tasks to create for polling the queue for messages.
    ///
    /// Defaults to the number of available CPUs on the system.
    ///
    /// Should not typically need to be changed, but it can sometimes be beneficial to raise this
    /// value when there is a high rate of messages being pushed into the queue and the objects
    /// being fetched are small. In these cases, system resources may not be fully utilized without
    /// fetching more messages per second, as the SQS message consumption rate affects the S3 object
    /// retrieval rate.
    #[configurable(metadata(docs::type_unit = "tasks"))]
    #[configurable(metadata(docs::examples = 5))]
    pub(super) client_concurrency: Option<NonZeroUsize>,

    /// Maximum number of messages to poll from SQS in a batch
    ///
    /// Defaults to 10
    ///
    /// Should be set to a smaller value when the files are large to help prevent the ingestion of
    /// one file from causing the other files to exceed the visibility_timeout. Valid values are 1 - 10
    // NOTE: We restrict this to u32 for safe conversion to i32 later.
    #[serde(default = "default_max_number_of_messages")]
    #[derivative(Default(value = "default_max_number_of_messages()"))]
    #[configurable(metadata(docs::human_name = "Max Messages"))]
    #[configurable(metadata(docs::examples = 1))]
    pub(super) max_number_of_messages: u32,

    #[serde(default)]
    #[derivative(Default)]
    pub(super) tls_options: Option<TlsConfig>,

    // Client timeout configuration for SQS operations. Take care when configuring these settings
    // to allow enough time for the polling interval configured in `poll_secs`.
    #[derivative(Default)]
    #[serde(default)]
    #[serde(flatten)]
    pub(super) timeout: Option<AwsTimeout>,

    /// Configuration for deferring events to another queue based on their age.
    pub(super) deferred: Option<DeferredConfig>,
}

pub(super) struct S3Options {
    pub(super) compression: super::Compression,
    pub(super) request_payer: Option<super::S3RequestPayer>,
}

const fn default_poll_secs() -> u32 {
    15
}

const fn default_visibility_timeout_secs() -> u32 {
    300
}

const fn default_max_number_of_messages() -> u32 {
    10
}

const fn default_true() -> bool {
    true
}

#[derive(Debug, Snafu)]
pub(super) enum IngestorNewError {
    #[snafu(display("Invalid value for max_number_of_messages {}", messages))]
    InvalidNumberOfMessages { messages: u32 },
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
pub enum ProcessingError {
    #[snafu(display(
        "Could not parse SQS message with id {} as S3 notification: {}",
        message_id,
        source
    ))]
    InvalidSqsMessage {
        source: serde_json::Error,
        message_id: String,
    },
    #[snafu(display("Failed to fetch s3://{}/{}: {}", bucket, key, source))]
    GetObject {
        source: SdkError<GetObjectError, HttpResponse>,
        bucket: String,
        key: String,
    },
    #[snafu(display("Failed to read all of s3://{}/{}: {}", bucket, key, source))]
    ReadObject {
        source: Box<dyn FramingError>,
        bucket: String,
        key: String,
    },
    #[snafu(display("Failed to flush all of s3://{}/{}: {}", bucket, key, source))]
    PipelineSend {
        source: vector_lib::source_sender::SendError,
        bucket: String,
        key: String,
    },
    #[snafu(display(
        "Object notification for s3://{}/{} is a bucket in another region: {}",
        bucket,
        key,
        region
    ))]
    WrongRegion {
        region: String,
        bucket: String,
        key: String,
    },
    #[snafu(display("Unsupported S3 event version: {}.", version,))]
    UnsupportedS3EventVersion { version: semver::Version },
    #[snafu(display(
        "Sink reported an error sending events for an s3 object in region {}: s3://{}/{}",
        region,
        bucket,
        key
    ))]
    ErrorAcknowledgement {
        region: String,
        bucket: String,
        key: String,
    },
    #[snafu(display(
        "File s3://{}/{} too old.  Forwarded to deferred queue {}",
        bucket,
        key,
        deferred_queue
    ))]
    FileTooOld {
        bucket: String,
        key: String,
        deferred_queue: String,
    },
}

pub struct State {
    region: Region,

    s3_client: S3Client,
    s3_options: S3Options,
    sqs_client: SqsClient,

    multiline: Option<line_agg::Config>,

    queue_url: String,
    poll_secs: i32,
    max_number_of_messages: i32,
    client_concurrency: usize,
    visibility_timeout_secs: i32,
    delete_message: bool,
    delete_failed_message: bool,
    decoder: Decoder,

    deferred: Option<DeferredConfig>,
}

pub(super) struct Ingestor {
    state: Arc<State>,
}

impl Ingestor {
    pub(super) fn new(
        region: Region,
        sqs_client: SqsClient,
        s3_client: S3Client,
        config: Config,
        s3_options: S3Options,
        multiline: Option<line_agg::Config>,
        decoder: Decoder,
    ) -> Result<Ingestor, IngestorNewError> {
        if config.max_number_of_messages < 1 || config.max_number_of_messages > 10 {
            return Err(IngestorNewError::InvalidNumberOfMessages {
                messages: config.max_number_of_messages,
            });
        }
        let state = Arc::new(State {
            region,

            s3_client,
            s3_options,
            sqs_client,

            multiline,

            queue_url: config.queue_url,
            poll_secs: config.poll_secs as i32,
            max_number_of_messages: config.max_number_of_messages as i32,
            client_concurrency: config
                .client_concurrency
                .map(|n| n.get())
                .unwrap_or_else(crate::num_threads),
            visibility_timeout_secs: config.visibility_timeout_secs as i32,
            delete_message: config.delete_message,
            delete_failed_message: config.delete_failed_message,
            decoder,

            deferred: config.deferred,
        });

        Ok(Ingestor { state })
    }

    pub(super) async fn run(
        self,
        cx: SourceContext,
        acknowledgements: SourceAcknowledgementsConfig,
        log_namespace: LogNamespace,
    ) -> Result<(), ()> {
        let acknowledgements = cx.do_acknowledgements(acknowledgements);
        let mut handles = Vec::new();
        for _ in 0..self.state.client_concurrency {
            let process = IngestorProcess::new(
                Arc::clone(&self.state),
                cx.out.clone(),
                cx.shutdown.clone(),
                log_namespace,
                acknowledgements,
            );
            let fut = process.run();
            let handle = crate::spawn_in_current_span(fut);
            handles.push(handle);
        }

        // Wait for all of the processes to finish.  If any one of them panics, we resume
        // that panic here to properly shutdown Vector.
        for handle in handles.drain(..) {
            if let Err(e) = handle.await
                && e.is_panic()
            {
                panic::resume_unwind(e.into_panic());
            }
        }

        Ok(())
    }
}

pub struct IngestorProcess {
    state: Arc<State>,
    out: SourceSender,
    shutdown: ShutdownSignal,
    acknowledgements: bool,
    log_namespace: LogNamespace,
    bytes_received: Registered<BytesReceived>,
    events_received: Registered<EventsReceived>,
    backoff: ExponentialBackoff,
}

impl IngestorProcess {
    pub fn new(
        state: Arc<State>,
        out: SourceSender,
        shutdown: ShutdownSignal,
        log_namespace: LogNamespace,
        acknowledgements: bool,
    ) -> Self {
        Self {
            state,
            out,
            shutdown,
            acknowledgements,
            log_namespace,
            bytes_received: register!(BytesReceived::from(Protocol::HTTP)),
            events_received: register!(EventsReceived),
            backoff: ExponentialBackoff::default().max_delay(Duration::from_secs(30)),
        }
    }

    async fn run(mut self) {
        // Shutdown is checked between polls rather than cancelling one: a long poll abandoned
        // client-side can still be answered server-side, leaving its messages invisible for the
        // whole visibility timeout. So a poll in progress at shutdown completes (within
        // `poll_secs`), and a batch received during shutdown is released unprocessed (see
        // `process_messages`), so its messages are either deleted or released.
        while self.shutdown.clone().now_or_never().is_none() {
            let received = self.receive_messages().await;
            match self.run_once(received).await {
                Ok(()) => {
                    // Reset backoff on successful receive
                    self.backoff.reset();
                }
                Err(_) => {
                    let delay = self.backoff.next().expect("backoff never ends");
                    trace!(
                        delay_ms = delay.as_millis(),
                        "`run_once` failed, will retry after delay.",
                    );
                    select! {
                        _ = self.shutdown.clone() => break,
                        _ = tokio::time::sleep(delay) => {},
                    }
                }
            }
        }
    }

    async fn run_once(
        &mut self,
        received: Result<Vec<Message>, SdkError<ReceiveMessageError, HttpResponse>>,
    ) -> Result<(), ()> {
        let messages = match received {
            Ok(messages) => {
                emit!(SqsMessageReceiveSucceeded {
                    count: messages.len(),
                });
                messages
            }
            Err(err) => {
                emit!(SqsMessageReceiveError { error: &err });
                return Err(());
            }
        };

        let mut batch = BatchProgress::new(&messages, self.state.delete_message);
        // On shutdown, the batch gets `SHUTDOWN_ACK_TIMEOUT` to finish (its objects' events to be
        // acknowledged); then it is cut short and its unfinished messages are released.
        let shutdown = self.shutdown.clone();
        let drain_timeout = async move {
            shutdown.await;
            tokio::time::sleep(SHUTDOWN_ACK_TIMEOUT).await;
        };
        let ack_wait_timed_out = select! {
            biased;
            () = self.process_messages(messages, &mut batch) => false,
            () = drain_timeout => true,
        };
        let BatchProgress {
            delete_entries,
            deferred_entries,
            unsettled,
        } = batch;

        // Should consider removing failed deferrals from the delete_entries
        if !deferred_entries.is_empty() {
            let Some(deferred) = &self.state.deferred else {
                warn!("Deferred queue not configured, but received deferred entries.");
                return Ok(());
            };
            let cloned_entries = deferred_entries.clone();
            match self
                .send_messages(deferred_entries, deferred.queue_url.clone())
                .await
            {
                Ok(result) => {
                    if !result.successful.is_empty() {
                        emit!(SqsMessageSentSucceeded {
                            message_ids: result.successful,
                        })
                    }

                    if !result.failed.is_empty() {
                        emit!(SqsMessageSentPartialError {
                            entries: result.failed
                        })
                    }
                }
                Err(err) => {
                    emit!(SqsMessageSendBatchError {
                        entries: cloned_entries,
                        error: err,
                    });
                }
            }
        }
        if !delete_entries.is_empty() {
            // We need these for a correct error message if the batch fails overall.
            let cloned_entries = delete_entries.clone();
            match self.delete_messages(delete_entries).await {
                Ok(result) => {
                    // Batch deletes can have partial successes/failures, so we have to check
                    // for both cases and emit accordingly.
                    if !result.successful.is_empty() {
                        emit!(SqsMessageDeleteSucceeded {
                            message_ids: result.successful,
                        });
                    }

                    if !result.failed.is_empty() {
                        emit!(SqsMessageDeletePartialError {
                            entries: result.failed
                        });
                    }
                }
                Err(err) => {
                    emit!(SqsMessageDeleteBatchError {
                        entries: cloned_entries,
                        error: err,
                    });
                }
            }
        }
        // Only on shutdown: the messages of this batch that were not processed in time.
        release::release(
            "aws_s3",
            &SqsRelease {
                client: &self.state.sqs_client,
                queue_url: &self.state.queue_url,
            },
            unsettled.into_iter().collect(),
            ack_wait_timed_out,
        )
        .await;
        Ok(())
    }

    /// Processes the messages of one batch in order, recording in `batch` which to delete or
    /// defer. Each message leaves `batch.unsettled` once processed, whatever the outcome; on
    /// shutdown the remaining messages are not started.
    async fn process_messages(&mut self, messages: Vec<Message>, batch: &mut BatchProgress) {
        for message in messages {
            if self.shutdown.clone().now_or_never().is_some() {
                break;
            }
            let receipt_handle = match message.receipt_handle {
                None => {
                    // I don't think this will ever actually happen, but is just an artifact of the
                    // AWS's API predilection for returning nullable values for all response
                    // attributes
                    warn!(message = "Refusing to process message with no receipt_handle.", ?message.message_id);
                    continue;
                }
                Some(ref handle) => handle.to_owned(),
            };

            let message_id = message
                .message_id
                .clone()
                .unwrap_or_else(|| "<unknown>".to_owned());
            let result = self.handle_sqs_message(message.clone()).await;
            // Processed: deleted, deferred, or left to its visibility timeout; never released.
            batch.unsettled.remove(&receipt_handle);
            match result {
                Ok(()) => {
                    emit!(SqsMessageProcessingSucceeded {
                        message_id: &message_id
                    });
                    if self.state.delete_message {
                        trace!(
                            message = "Queued SQS message for deletion.",
                            id = message_id,
                            receipt_handle = receipt_handle,
                        );
                        batch.delete_entries.push(
                            DeleteMessageBatchRequestEntry::builder()
                                .id(message_id.clone())
                                .receipt_handle(receipt_handle)
                                .build()
                                .expect("all required builder params specified"),
                        );
                    }
                }
                Err(err) => {
                    match err {
                        ProcessingError::FileTooOld { .. } => {
                            emit!(SqsMessageProcessingSucceeded {
                                message_id: &message_id
                            });
                            if let Some(deferred) = &self.state.deferred {
                                trace!(
                                    message = "Forwarding message to deferred queue.",
                                    id = message_id,
                                    receipt_handle = receipt_handle,
                                    deferred_queue = deferred.queue_url,
                                );

                                batch.deferred_entries.push(
                                    SendMessageBatchRequestEntry::builder()
                                        .id(message_id.clone())
                                        .message_body(message.body.unwrap_or_default())
                                        .build()
                                        .expect("all required builder params specified"),
                                );
                            }
                            //  maybe delete the message from current queue since we have processed it
                            if self.state.delete_message {
                                trace!(
                                    message = "Queued SQS message for deletion.",
                                    id = message_id,
                                    receipt_handle = receipt_handle,
                                );
                                batch.delete_entries.push(
                                    DeleteMessageBatchRequestEntry::builder()
                                        .id(message_id)
                                        .receipt_handle(receipt_handle)
                                        .build()
                                        .expect("all required builder params specified"),
                                );
                            }
                        }
                        _ => {
                            emit!(SqsMessageProcessingError {
                                message_id: &message_id,
                                error: &err,
                            });
                        }
                    }
                }
            }
        }
    }

    async fn handle_sqs_message(&mut self, message: Message) -> Result<(), ProcessingError> {
        let sqs_body = message.body.unwrap_or_default();
        let sqs_body = serde_json::from_str::<SnsNotification>(sqs_body.as_ref())
            .map(|notification| notification.message)
            .unwrap_or(sqs_body);
        let s3_event: SqsEvent =
            serde_json::from_str(sqs_body.as_ref()).context(InvalidSqsMessageSnafu {
                message_id: message
                    .message_id
                    .clone()
                    .unwrap_or_else(|| "<empty>".to_owned()),
            })?;

        match s3_event {
            SqsEvent::TestEvent(_s3_test_event) => {
                debug!(?message.message_id, message = "Found S3 Test Event.");
                Ok(())
            }
            SqsEvent::Event(s3_event) => self.handle_s3_event(s3_event).await,
        }
    }

    async fn handle_s3_event(&mut self, s3_event: S3Event) -> Result<(), ProcessingError> {
        for record in s3_event.records {
            self.handle_s3_event_record(record, self.log_namespace)
                .await?
        }
        Ok(())
    }

    async fn handle_s3_event_record(
        &mut self,
        s3_event: S3EventRecord,
        log_namespace: LogNamespace,
    ) -> Result<(), ProcessingError> {
        let event_version: semver::Version = s3_event.event_version.clone().into();
        if !SUPPORTED_S3_EVENT_VERSION.matches(&event_version) {
            return Err(ProcessingError::UnsupportedS3EventVersion {
                version: event_version.clone(),
            });
        }

        if s3_event.event_name.kind != "ObjectCreated" {
            emit!(SqsS3EventRecordInvalidEventIgnored {
                bucket: &s3_event.s3.bucket.name,
                key: &s3_event.s3.object.key,
                kind: &s3_event.event_name.kind,
                name: &s3_event.event_name.name,
            });
            return Ok(());
        }

        // S3 has to send notifications to a queue in the same region so I don't think this will
        // actually ever be hit unless messages are being forwarded from one queue to another
        if self.state.region.as_ref() != s3_event.aws_region.as_str() {
            return Err(ProcessingError::WrongRegion {
                bucket: s3_event.s3.bucket.name.clone(),
                key: s3_event.s3.object.key.clone(),
                region: s3_event.aws_region,
            });
        }

        if let Some(deferred) = &self.state.deferred {
            let delta = Utc::now() - s3_event.event_time;
            if delta.num_seconds() > deferred.max_age_secs as i64 {
                return Err(ProcessingError::FileTooOld {
                    bucket: s3_event.s3.bucket.name.clone(),
                    key: s3_event.s3.object.key.clone(),
                    deferred_queue: deferred.queue_url.clone(),
                });
            }
        }

        let download_start = Instant::now();

        let object_result = self
            .state
            .s3_client
            .get_object()
            .bucket(s3_event.s3.bucket.name.clone())
            .key(s3_event.s3.object.key.clone())
            .set_request_payer(self.state.s3_options.request_payer.map(Into::into))
            .send()
            .await
            .context(GetObjectSnafu {
                bucket: s3_event.s3.bucket.name.clone(),
                key: s3_event.s3.object.key.clone(),
            });

        let object = object_result?;

        debug!(
            message = "Got S3 object from SQS notification.",
            bucket = s3_event.s3.bucket.name,
            key = s3_event.s3.object.key,
        );

        let metadata = object.metadata;

        let timestamp = object.last_modified.map(|ts| {
            Utc.timestamp_opt(ts.secs(), ts.subsec_nanos())
                .single()
                .expect("invalid timestamp")
        });

        let (batch, receiver) = BatchNotifier::maybe_new_with_receiver(self.acknowledgements);
        let object_reader = super::s3_object_decoder(
            self.state.s3_options.compression,
            &s3_event.s3.object.key,
            object.content_encoding.as_deref(),
            object.content_type.as_deref(),
            object.body,
        )
        .await;

        // Record the read error seen to propagate up later so we avoid ack'ing the SQS
        // message
        //
        // String is used as we cannot clone std::io::Error to take ownership in closure
        //
        // FramedRead likely stops when it gets an i/o error but I found it more clear to
        // show that we `take_while` there hasn't been an error
        //
        // This can result in objects being partially processed before an error, but we
        // prefer duplicate lines over message loss. Future work could include recording
        // the offset of the object that has been read, but this would only be relevant in
        // the case that the same vector instance processes the same message.
        let mut read_error = None;
        let bytes_received = self.bytes_received.clone();
        let events_received = self.events_received.clone();
        let lines: Box<dyn Stream<Item = Bytes> + Send + Unpin> = Box::new(
            FramedRead::new(object_reader, self.state.decoder.framer.clone())
                .map(|res| {
                    res.inspect(|bytes| {
                        bytes_received.emit(ByteSize(bytes.len()));
                    })
                    .map_err(|err| {
                        read_error = Some(err);
                    })
                    .ok()
                })
                .take_while(|res| ready(res.is_some()))
                .map(|r| r.expect("validated by take_while")),
        );

        let lines: Box<dyn Stream<Item = Bytes> + Send + Unpin> = match &self.state.multiline {
            Some(config) => Box::new(
                LineAgg::new(
                    lines.map(|line| ((), line, ())),
                    line_agg::Logic::new(config.clone()),
                )
                .map(|(_src, line, _context, _lastline_context)| line),
            ),
            None => lines,
        };

        let mut stream = lines.flat_map(|line| {
            let events = match self.state.decoder.deserializer_parse(line) {
                Ok((events, _events_size)) => events,
                Err(_error) => {
                    // Error is handled by `codecs::Decoder`, no further handling
                    // is needed here.
                    SmallVec::new()
                }
            };

            let events = events
                .into_iter()
                .map(|mut event: Event| {
                    event = event.with_batch_notifier_option(&batch);
                    if let Some(log_event) = event.maybe_as_log_mut() {
                        handle_single_log(
                            log_event,
                            log_namespace,
                            &s3_event,
                            &metadata,
                            timestamp,
                        );
                    }
                    events_received.emit(CountByteSize(1, event.estimated_json_encoded_size_of()));
                    event
                })
                .collect::<Vec<Event>>();
            futures::stream::iter(events)
        });

        let send_error = match self.out.send_event_stream(&mut stream).await {
            Ok(_) => None,
            Err(SendError::Closed) => {
                let (count, _) = stream.size_hint();
                emit!(StreamClosedError { count });
                Some(SendError::Closed)
            }
            Err(SendError::Timeout) => unreachable!("No timeout is configured here"),
        };

        // Up above, `lines` captures `read_error`, and eventually is captured by `stream`,
        // so we explicitly drop it so that we can again utilize `read_error` below.
        drop(stream);

        let bucket = &s3_event.s3.bucket.name;
        let duration = download_start.elapsed();

        if read_error.is_some() {
            emit!(S3ObjectProcessingFailed { bucket, duration });
        } else {
            emit!(S3ObjectProcessingSucceeded { bucket, duration });
        }

        // The BatchNotifier is cloned for each LogEvent in the batch stream, but the last
        // reference must be dropped before the status of the batch is sent to the channel.
        drop(batch);

        if let Some(error) = read_error {
            Err(ProcessingError::ReadObject {
                source: error,
                bucket: s3_event.s3.bucket.name.clone(),
                key: s3_event.s3.object.key.clone(),
            })
        } else if let Some(error) = send_error {
            Err(ProcessingError::PipelineSend {
                source: error,
                bucket: s3_event.s3.bucket.name.clone(),
                key: s3_event.s3.object.key.clone(),
            })
        } else {
            match receiver {
                None => Ok(()),
                Some(receiver) => {
                    let result = receiver.await;
                    match result {
                        BatchStatus::Delivered => {
                            debug!(
                                message = "S3 object from SQS delivered.",
                                bucket = s3_event.s3.bucket.name,
                                key = s3_event.s3.object.key,
                            );
                            Ok(())
                        }
                        BatchStatus::Errored => Err(ProcessingError::ErrorAcknowledgement {
                            bucket: s3_event.s3.bucket.name,
                            key: s3_event.s3.object.key,
                            region: s3_event.aws_region,
                        }),
                        BatchStatus::Rejected => {
                            if self.state.delete_failed_message {
                                warn!(
                                    message =
                                        "S3 object from SQS was rejected. Deleting failed message.",
                                    bucket = s3_event.s3.bucket.name,
                                    key = s3_event.s3.object.key,
                                );
                                Ok(())
                            } else {
                                Err(ProcessingError::ErrorAcknowledgement {
                                    bucket: s3_event.s3.bucket.name,
                                    key: s3_event.s3.object.key,
                                    region: s3_event.aws_region,
                                })
                            }
                        }
                    }
                }
            }
        }
    }

    async fn receive_messages(
        &mut self,
    ) -> Result<Vec<Message>, SdkError<ReceiveMessageError, HttpResponse>> {
        self.state
            .sqs_client
            .receive_message()
            .queue_url(self.state.queue_url.clone())
            .max_number_of_messages(self.state.max_number_of_messages)
            .visibility_timeout(self.state.visibility_timeout_secs)
            .wait_time_seconds(self.state.poll_secs)
            .send()
            .map_ok(|res| res.messages.unwrap_or_default())
            .await
    }

    async fn delete_messages(
        &mut self,
        entries: Vec<DeleteMessageBatchRequestEntry>,
    ) -> Result<DeleteMessageBatchOutput, SdkError<DeleteMessageBatchError, HttpResponse>> {
        self.state
            .sqs_client
            .delete_message_batch()
            .queue_url(self.state.queue_url.clone())
            .set_entries(Some(entries))
            .send()
            .await
    }

    async fn send_messages(
        &mut self,
        entries: Vec<SendMessageBatchRequestEntry>,
        queue_url: String,
    ) -> Result<SendMessageBatchOutput, SdkError<SendMessageBatchError, HttpResponse>> {
        self.state
            .sqs_client
            .send_message_batch()
            .queue_url(queue_url.clone())
            .set_entries(Some(entries))
            .send()
            .await
    }
}

/// The bookkeeping of one received batch of SQS messages.
struct BatchProgress {
    delete_entries: Vec<DeleteMessageBatchRequestEntry>,
    deferred_entries: Vec<SendMessageBatchRequestEntry>,
    /// Receipts of messages not processed yet. Only tracked with `delete_message`: without it,
    /// messages are never deleted and simply reappear after their visibility timeout.
    unsettled: HashSet<String>,
}

impl BatchProgress {
    fn new(messages: &[Message], delete_message: bool) -> Self {
        Self {
            delete_entries: Vec::new(),
            deferred_entries: Vec::new(),
            unsettled: if delete_message {
                messages
                    .iter()
                    .filter_map(|message| message.receipt_handle.clone())
                    .collect()
            } else {
                HashSet::new()
            },
        }
    }
}

fn handle_single_log(
    log: &mut LogEvent,
    log_namespace: LogNamespace,
    s3_event: &S3EventRecord,
    metadata: &Option<HashMap<String, String>>,
    timestamp: Option<DateTime<Utc>>,
) {
    log_namespace.insert_source_metadata(
        AwsS3Config::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("bucket"))),
        path!("bucket"),
        Bytes::from(s3_event.s3.bucket.name.as_bytes().to_vec()),
    );

    log_namespace.insert_source_metadata(
        AwsS3Config::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("object"))),
        path!("object"),
        Bytes::from(s3_event.s3.object.key.as_bytes().to_vec()),
    );
    log_namespace.insert_source_metadata(
        AwsS3Config::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("region"))),
        path!("region"),
        Bytes::from(s3_event.aws_region.as_bytes().to_vec()),
    );

    if let Some(metadata) = metadata {
        for (key, value) in metadata {
            log_namespace.insert_source_metadata(
                AwsS3Config::NAME,
                log,
                Some(LegacyKey::Overwrite(path!(key))),
                path!("metadata", key.as_str()),
                value.clone(),
            );
        }
    }

    // When S3 accepted the object (the notification's `eventTime`), as opposed to its
    // Last-Modified time, which a restore or copy can carry over from long before. Inserted after
    // the object's user metadata so that a metadata key of the same name cannot replace it.
    log_namespace.insert_source_metadata(
        AwsS3Config::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("s3_event_time"))),
        path!("event_time"),
        s3_event.event_time,
    );

    log_namespace.insert_vector_metadata(
        log,
        log_schema().source_type_key(),
        path!("source_type"),
        Bytes::from_static(AwsS3Config::NAME.as_bytes()),
    );

    // This handles the transition from the original timestamp logic. Originally the
    // `timestamp_key` was populated by the `last_modified` time on the object, falling
    // back to calling `now()`.
    match log_namespace {
        LogNamespace::Vector => {
            if let Some(timestamp) = timestamp {
                log.insert(metadata_path!(AwsS3Config::NAME, "timestamp"), timestamp);
            }

            log.insert(metadata_path!("vector", "ingest_timestamp"), Utc::now());
        }
        LogNamespace::Legacy => {
            if let Some(timestamp_key) = log_schema().timestamp_key() {
                log.try_insert(
                    (PathPrefix::Event, timestamp_key),
                    timestamp.unwrap_or_else(Utc::now),
                );
            }
        }
    };
}

// https://docs.aws.amazon.com/sns/latest/dg/sns-sqs-as-subscriber.html
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SnsNotification {
    pub message: String,
    pub timestamp: DateTime<Utc>,
}

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/how-to-enable-disable-notification-intro.html
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum SqsEvent {
    Event(S3Event),
    TestEvent(S3TestEvent),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct S3TestEvent {
    pub service: String,
    pub event: S3EventName,
    pub bucket: String,
}

// https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct S3Event {
    pub records: Vec<S3EventRecord>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct S3EventRecord {
    pub event_version: S3EventVersion,
    pub event_source: String,
    pub aws_region: String,
    pub event_name: S3EventName,
    pub event_time: DateTime<Utc>,

    pub s3: S3Message,
}

#[derive(Clone, Debug)]
pub struct S3EventVersion {
    pub major: u64,
    pub minor: u64,
}

impl From<S3EventVersion> for semver::Version {
    fn from(v: S3EventVersion) -> semver::Version {
        semver::Version::new(v.major, v.minor, 0)
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
// <major>.<minor>
impl<'de> Deserialize<'de> for S3EventVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        let s = String::deserialize(deserializer)?;

        let mut parts = s.splitn(2, '.');

        let major = parts
            .next()
            .ok_or_else(|| D::Error::custom("Missing major version number"))?
            .parse::<u64>()
            .map_err(D::Error::custom)?;

        let minor = parts
            .next()
            .ok_or_else(|| D::Error::custom("Missing minor version number"))?
            .parse::<u64>()
            .map_err(D::Error::custom)?;

        Ok(S3EventVersion { major, minor })
    }
}

impl Serialize for S3EventVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}.{}", self.major, self.minor))
    }
}

#[derive(Clone, Debug)]
pub struct S3EventName {
    pub kind: String,
    pub name: String,
}

// https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html#supported-notification-event-types
//
// we could use enums here, but that seems overly brittle as deserialization would break if they
// add new event types or names
impl<'de> Deserialize<'de> for S3EventName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        let s = String::deserialize(deserializer)?;

        let mut parts = s.splitn(2, ':');

        let kind = parts
            .next()
            .ok_or_else(|| D::Error::custom("Missing event kind"))?
            .parse::<String>()
            .map_err(D::Error::custom)?;

        let name = parts
            .next()
            .ok_or_else(|| D::Error::custom("Missing event name"))?
            .parse::<String>()
            .map_err(D::Error::custom)?;

        Ok(S3EventName { kind, name })
    }
}

impl Serialize for S3EventName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}:{}", self.kind, self.name))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct S3Message {
    pub bucket: S3Bucket,
    pub object: S3Object,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct S3Bucket {
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct S3Object {
    // S3ObjectKeys are URL encoded
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html
    #[serde(with = "urlencoded_string")]
    pub key: String,
}

mod urlencoded_string {
    use percent_encoding::{percent_decode, utf8_percent_encode};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        use serde::de::Error;

        serde::de::Deserialize::deserialize(deserializer).and_then(|s: &[u8]| {
            let decoded = if s.contains(&b'+') {
                // AWS encodes spaces as `+` rather than `%20`, so we first need to handle this.
                let s = s
                    .iter()
                    .map(|c| if *c == b'+' { b' ' } else { *c })
                    .collect::<Vec<_>>();
                percent_decode(&s).decode_utf8().map(Into::into)
            } else {
                percent_decode(s).decode_utf8().map(Into::into)
            };

            decoded
                .map_err(|err| D::Error::custom(format!("error url decoding S3 object key: {err}")))
        })
    }

    pub fn serialize<S>(s: &str, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(
            &utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).collect::<String>(),
        )
    }
}

#[test]
fn test_key_deserialize() {
    let value = serde_json::from_str(r#"{"key": "noog+nork"}"#).unwrap();
    assert_eq!(
        S3Object {
            key: "noog nork".to_string(),
        },
        value
    );

    let value = serde_json::from_str(r#"{"key": "noog%2bnork"}"#).unwrap();
    assert_eq!(
        S3Object {
            key: "noog+nork".to_string(),
        },
        value
    );
}

#[test]
fn test_s3_testevent() {
    let value: S3TestEvent = serde_json::from_str(
        r#"{
        "Service":"Amazon S3",
        "Event":"s3:TestEvent",
        "Time":"2014-10-13T15:57:02.089Z",
        "Bucket":"bucketname",
        "RequestId":"5582815E1AEA5ADF",
        "HostId":"8cLeGAmw098X5cv4Zkwcmo8vvZa3eH3eKxsPzbB9wrR+YstdA6Knx4Ip8EXAMPLE"
     }"#,
    )
    .unwrap();

    assert_eq!(value.service, "Amazon S3".to_string());
    assert_eq!(value.bucket, "bucketname".to_string());
    assert_eq!(value.event.kind, "s3".to_string());
    assert_eq!(value.event.name, "TestEvent".to_string());
}

#[test]
fn test_s3_sns_testevent() {
    let sns_value: SnsNotification = serde_json::from_str(
        r#"{
        "Type" : "Notification",
        "MessageId" : "63a3f6b6-d533-4a47-aef9-fcf5cf758c76",
        "TopicArn" : "arn:aws:sns:us-west-2:123456789012:MyTopic",
        "Subject" : "Testing publish to subscribed queues",
        "Message" : "{\"Bucket\":\"bucketname\",\"Event\":\"s3:TestEvent\",\"HostId\":\"8cLeGAmw098X5cv4Zkwcmo8vvZa3eH3eKxsPzbB9wrR+YstdA6Knx4Ip8EXAMPLE\",\"RequestId\":\"5582815E1AEA5ADF\",\"Service\":\"Amazon S3\",\"Time\":\"2014-10-13T15:57:02.089Z\"}",
        "Timestamp" : "2012-03-29T05:12:16.901Z",
        "SignatureVersion" : "1",
        "Signature" : "EXAMPLEnTrFPa3...",
        "SigningCertURL" : "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-f3ecfb7224c7233fe7bb5f59f96de52f.pem",
        "UnsubscribeURL" : "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:123456789012:MyTopic:c7fe3a54-ab0e-4ec2-88e0-db410a0f2bee"
     }"#,
    ).unwrap();

    assert_eq!(
        sns_value.timestamp,
        DateTime::parse_from_rfc3339("2012-03-29T05:12:16.901Z")
            .unwrap()
            .to_utc()
    );

    let value: S3TestEvent = serde_json::from_str(sns_value.message.as_ref()).unwrap();

    assert_eq!(value.service, "Amazon S3".to_string());
    assert_eq!(value.bucket, "bucketname".to_string());
    assert_eq!(value.event.kind, "s3".to_string());
    assert_eq!(value.event.name, "TestEvent".to_string());
}

#[cfg(test)]
fn s3_event_record(event_time: &str) -> S3EventRecord {
    // `from_str`, not `from_value`: the object key deserializer borrows from the input.
    let json = serde_json::json!({
        "eventVersion": "2.1",
        "eventSource": "aws:s3",
        "awsRegion": "us-east-1",
        "eventTime": event_time,
        "eventName": "ObjectCreated:Copy",
        "s3": {
            "s3SchemaVersion": "1.0",
            "configurationId": "config",
            "bucket": { "name": "bucket", "arn": "arn:aws:s3:::bucket" },
            "object": { "key": "AWSLogs/restored.log", "size": 3 }
        }
    })
    .to_string();
    serde_json::from_str(&json).unwrap()
}

#[test]
fn handle_single_log_adds_s3_event_time() {
    use vrl::value::Value;

    use crate::config::SourceConfig;

    // A restored object: Last-Modified is years before the notification.
    let event_time = DateTime::parse_from_rfc3339("2026-10-12T14:00:00.123Z")
        .unwrap()
        .to_utc();
    let last_modified = DateTime::parse_from_rfc3339("2021-01-01T00:00:00Z")
        .unwrap()
        .to_utc();
    let record = s3_event_record("2026-10-12T14:00:00.123Z");
    assert_eq!(record.event_time, event_time);
    // A user metadata key of the same name cannot replace it.
    let metadata = Some(HashMap::from([(
        "s3_event_time".to_string(),
        "user value".to_string(),
    )]));

    for namespace in [LogNamespace::Legacy, LogNamespace::Vector] {
        let mut log = match namespace {
            // Not `LogEvent::from(&str)`, which already sets `timestamp` to now.
            LogNamespace::Legacy => {
                let mut log = LogEvent::default();
                log.insert(vrl::event_path!("message"), "line");
                log
            }
            LogNamespace::Vector => LogEvent::from(Value::from("line")),
        };
        handle_single_log(&mut log, namespace, &record, &metadata, Some(last_modified));

        assert_eq!(
            namespace.get_source_metadata(
                AwsS3Config::NAME,
                &log,
                path!("s3_event_time"),
                path!("event_time"),
            ),
            Some(&Value::Timestamp(event_time)),
            "{namespace:?}"
        );
        match namespace {
            LogNamespace::Legacy => {
                assert_eq!(log["s3_event_time"], Value::Timestamp(event_time));
                // `timestamp` keeps its meaning: the object's Last-Modified time.
                assert_eq!(log["timestamp"], Value::Timestamp(last_modified));
            }
            LogNamespace::Vector => {
                assert_eq!(
                    log.get(metadata_path!("aws_s3", "event_time")),
                    Some(&Value::Timestamp(event_time))
                );
                assert_eq!(
                    log.get(metadata_path!("aws_s3", "timestamp")),
                    Some(&Value::Timestamp(last_modified))
                );
                // Nothing is added to the event itself.
                assert_eq!(log.value(), &Value::from("line"));
            }
        }

        // The field is part of the declared schema.
        let config = AwsS3Config::default();
        let definition = config
            .outputs(namespace)
            .pop()
            .unwrap()
            .schema_definition
            .unwrap();
        definition
            .is_valid_for_event(&Event::Log(log))
            .unwrap_or_else(|error| panic!("{namespace:?}: {error}"));
    }
}

#[test]
fn parse_sqs_config() {
    let config: Config = serde_yaml::from_str(
        r#"queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"
"#,
    )
    .unwrap();
    assert_eq!(
        config.queue_url,
        "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"
    );
    assert!(config.deferred.is_none());

    let config: Config = serde_yaml::from_str(indoc::indoc! {r#"
        queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"
        deferred:
          queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/MyDeferredQueue"
          max_age_secs: 3600
    "#})
    .unwrap();
    assert_eq!(
        config.queue_url,
        "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"
    );
    let Some(deferred) = config.deferred else {
        panic!("Expected deferred config");
    };
    assert_eq!(
        deferred.queue_url,
        "https://sqs.us-east-1.amazonaws.com/123456789012/MyDeferredQueue"
    );
    assert_eq!(deferred.max_age_secs, 3600);

    let test: Result<Config, serde_yaml::Error> = serde_yaml::from_str(indoc::indoc! {r#"
        queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"
        deferred:
          max_age_secs: 3600
    "#});
    assert!(test.is_err());

    let test: Result<Config, serde_yaml::Error> = serde_yaml::from_str(indoc::indoc! {r#"
        queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"
        deferred:
          queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/MyDeferredQueue"
    "#});
    assert!(test.is_err());
}

#[test]
fn batch_progress_tracks_receipts_only_when_deleting() {
    let messages = vec![
        Message::builder().receipt_handle("r1").build(),
        Message::builder().build(),
        Message::builder().receipt_handle("r2").build(),
    ];
    let progress = BatchProgress::new(&messages, true);
    assert_eq!(
        progress.unsettled,
        HashSet::from(["r1".to_string(), "r2".to_string()])
    );
    // Without `delete_message`, messages are never deleted, so none is released either.
    assert!(BatchProgress::new(&messages, false).unsettled.is_empty());
}
