use std::{
    collections::HashSet,
    error::Error as _,
    future::Future,
    pin::Pin,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use chrono::DateTime;
use derivative::Derivative;
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, stream, stream::FuturesUnordered};
use http::uri::{InvalidUri, Scheme, Uri};
use serde_with::serde_as;
use snafu::{ResultExt, Snafu};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Code, Request, Status,
    metadata::MetadataValue,
    transport::{Certificate, ClientTlsConfig, Endpoint, Identity},
};
use vector_lib::{
    byte_size_of::ByteSizeOf,
    codecs::decoding::{DeserializerConfig, FramingConfig},
    config::{LegacyKey, LogNamespace},
    configurable::configurable_component,
    finalizer::UnorderedFinalizer,
    internal_event::{
        ByteSize, BytesReceived, EventsReceived, InternalEventHandle as _, Protocol, Registered,
    },
    lookup::owned_value_path,
};
use vrl::{
    path,
    value::{Kind, kind::Collection},
};

use crate::{
    SourceSender,
    codecs::{Decoder, DecodingConfig},
    config::{DataType, SourceAcknowledgementsConfig, SourceConfig, SourceContext, SourceOutput},
    event::{BatchNotifier, BatchStatus, Event, MaybeAsLogMut, Value},
    gcp::{GcpAuthConfig, GcpAuthenticator, PUBSUB_URL, Scope},
    internal_events::{
        GcpPubsubConnectError, GcpPubsubReceiveError, GcpPubsubStreamingPullError,
        SourceMessagesReleased, StreamClosedError,
    },
    serde::{bool_or_struct, default_decoding, default_framing_message_based},
    shutdown::ShutdownSignal,
    sources::util,
    tls::{TlsConfig, TlsSettings},
};

const MIN_ACK_DEADLINE_SECS: u64 = 10;
const MAX_ACK_DEADLINE_SECS: u64 = 600;

// We use a bounded channel for the acknowledgement ID communication
// between the request stream and receiver. During benchmark runs,
// this channel had only a single element over 80% of the time and
// rarely went over 8 elements. Having it too small does not introduce
// deadlocks, as the worst case is slightly less efficient ack
// processing.
const ACK_QUEUE_SIZE: usize = 8;

type Finalizer = UnorderedFinalizer<Vec<String>>;

/// The time a shutting-down source has in all before Vector stops it outright: nano's pull
/// container runs Vector with `--graceful-shutdown-limit-secs 15` (`VECTOR_GRACEFUL_SHUTDOWN_LIMIT_SECS`
/// in nanosiem's `pull-container.js`). The release must be sent within it, or the messages wait
/// out their ack deadline after all.
const GRACEFUL_SHUTDOWN_LIMIT: Duration = Duration::from_secs(15);

/// How long a stream waits on shutdown for the acknowledgements of messages already sent
/// downstream before it releases them for redelivery.
const SHUTDOWN_ACK_WAIT: Duration = Duration::from_secs(8);

// The worst case of a shutdown (acknowledgements that never come, a slow release or close)
// ends `SHUTDOWN_ACK_WAIT + SHUTDOWN_FLUSH_TIMEOUT` (13 s) after the signal, with headroom under
// the graceful shutdown limit. Checked at compile time.
const _: () = assert!(
    SHUTDOWN_ACK_WAIT.as_secs() + SHUTDOWN_FLUSH_TIMEOUT.as_secs() + 2
        <= GRACEFUL_SHUTDOWN_LIMIT.as_secs()
);

/// [`SHUTDOWN_ACK_WAIT`], shortened in tests.
const SHUTDOWN_ACK_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_secs(1)
} else {
    SHUTDOWN_ACK_WAIT
};

/// How long a stream takes on shutdown, after the acknowledgement wait, to send the releases and
/// wait for the server to close the stream (so that the last acknowledgements and releases are
/// not lost with the connection).
const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// The most ack IDs released in one request.
const RELEASE_CHUNK_SIZE: usize = 250;

/// An update sent on the streaming pull request stream.
#[derive(Debug, PartialEq)]
enum StreamUpdate {
    /// Acknowledge these messages: they were delivered.
    Ack(Vec<String>),
    /// Set the ack deadline of these messages to zero, so that Pub/Sub redelivers them right away
    /// instead of after the ack deadline. Used on shutdown for messages that were received but
    /// not delivered; never for a message that is or will be acknowledged.
    Release(Vec<String>),
}

/// The ack IDs of one stream that were sent downstream and whose acknowledgement (end-to-end
/// delivery status) has not come back yet.
#[derive(Debug, Default)]
struct InFlight(HashSet<String>);

impl InFlight {
    fn add(&mut self, ids: &[String]) {
        self.0.extend(ids.iter().cloned());
    }

    /// The finalizer reported these IDs (delivered or not); they are settled.
    fn settle(&mut self, ids: &[String]) {
        for id in ids {
            self.0.remove(id);
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    /// Takes the IDs still unsettled. The caller must stop processing acknowledgements before
    /// releasing them, so that no released ID is acknowledged afterwards.
    fn take(&mut self) -> Vec<String> {
        self.0.drain().collect()
    }
}

/// Builds the request carrying a group of updates.
fn update_request(updates: Vec<StreamUpdate>) -> proto::StreamingPullRequest {
    let mut ack_ids = Vec::new();
    let mut release_ids = Vec::new();
    for update in updates {
        match update {
            StreamUpdate::Ack(ids) => ack_ids.extend(ids),
            StreamUpdate::Release(ids) => release_ids.extend(ids),
        }
    }
    proto::StreamingPullRequest {
        ack_ids,
        modify_deadline_seconds: vec![0; release_ids.len()],
        modify_deadline_ack_ids: release_ids,
        ..Default::default()
    }
}

/// Why a stream's receive loop ended during shutdown.
enum Drained {
    /// Every message sent downstream was acknowledged.
    Done,
    /// The acknowledgements did not all come back in time.
    TimedOut,
    /// The stream ended; nothing more can be sent on it.
    StreamEnded,
}

// https://github.com/hyperium/tonic/issues/1350
#[allow(clippy::missing_const_for_fn)]
#[allow(warnings)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/google.pubsub.v1.rs"));

    use vector_lib::ByteSizeOf;

    impl ByteSizeOf for StreamingPullResponse {
        fn allocated_bytes(&self) -> usize {
            self.received_messages.size_of()
        }
    }

    impl ByteSizeOf for ReceivedMessage {
        fn allocated_bytes(&self) -> usize {
            self.ack_id.size_of() + self.message.as_ref().map_or(0, ByteSizeOf::size_of)
        }
    }

    impl ByteSizeOf for PubsubMessage {
        fn allocated_bytes(&self) -> usize {
            self.data.len()
                + self.message_id.len()
                + self.ordering_key.len()
                + self.attributes.size_of()
        }
    }
}

#[derive(Debug, Snafu)]
pub(crate) enum PubsubError {
    #[snafu(display("Invalid endpoint URI: {}", source))]
    Uri { source: InvalidUri },
    #[snafu(display("Could not create endpoint: {}", source))]
    Endpoint { source: tonic::transport::Error },
    #[snafu(display("Could not set up endpoint TLS settings: {}", source))]
    EndpointTls { source: tonic::transport::Error },
    #[snafu(display(
        "`ack_deadline_secs` is outside the valid range of {} to {}",
        MIN_ACK_DEADLINE_SECS,
        MAX_ACK_DEADLINE_SECS
    ))]
    InvalidAckDeadline,
}

static CLIENT_ID: LazyLock<String> = LazyLock::new(|| uuid::Uuid::new_v4().to_string());

/// Configuration for the `gcp_pubsub` source.
#[serde_as]
#[configurable_component(source(
    "gcp_pubsub",
    "Fetch observability events from GCP's Pub/Sub messaging system."
))]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub struct PubsubConfig {
    /// The project name from which to pull logs.
    #[configurable(metadata(docs::examples = "my-log-source-project"))]
    pub project: String,

    /// The subscription within the project which is configured to receive logs.
    #[configurable(metadata(docs::examples = "my-vector-source-subscription"))]
    pub subscription: String,

    /// The endpoint from which to pull data.
    #[configurable(metadata(docs::examples = "https://us-central1-pubsub.googleapis.com"))]
    #[serde(default = "default_endpoint")]
    pub endpoint: String,

    #[serde(flatten)]
    pub auth: GcpAuthConfig,

    pub tls: Option<TlsConfig>,

    /// The maximum number of concurrent stream connections to open at once.
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,

    /// The number of messages in a response to mark a stream as
    /// "busy". This is used to determine if more streams should be
    /// started.
    ///
    /// The GCP Pub/Sub servers send responses with 100 or more messages when
    /// the subscription is busy.
    #[serde(default = "default_full_response")]
    pub full_response_size: usize,

    /// How often to poll the currently active streams to see if they
    /// are all busy and so open a new stream.
    #[serde(default = "default_poll_time")]
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    #[configurable(metadata(docs::human_name = "Poll Time"))]
    pub poll_time_seconds: Duration,

    /// The acknowledgement deadline, in seconds, to use for this stream.
    ///
    /// Messages that are not acknowledged when this deadline expires may be retransmitted.
    #[serde(default = "default_ack_deadline")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[configurable(metadata(docs::human_name = "Acknowledgement Deadline"))]
    pub ack_deadline_secs: Duration,

    /// The acknowledgement deadline, in seconds, to use for this stream.
    ///
    /// Messages that are not acknowledged when this deadline expires may be retransmitted.
    #[configurable(
        deprecated = "This option has been deprecated, use `ack_deadline_secs` instead."
    )]
    pub ack_deadline_seconds: Option<u16>,

    /// The amount of time, in seconds, to wait between retry attempts after an error.
    #[serde(default = "default_retry_delay")]
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    #[configurable(metadata(docs::human_name = "Retry Delay"))]
    pub retry_delay_secs: Duration,

    /// The amount of time, in seconds, to wait between retry attempts after an error.
    #[configurable(
        deprecated = "This option has been deprecated, use `retry_delay_secs` instead."
    )]
    pub retry_delay_seconds: Option<f64>,

    /// The amount of time, in seconds, with no received activity
    /// before sending a keepalive request. If this is set larger than
    /// `60`, you may see periodic errors sent from the server.
    #[serde(default = "default_keepalive")]
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    #[configurable(metadata(docs::human_name = "Keepalive"))]
    pub keepalive_secs: Duration,

    /// The namespace to use for logs. This overrides the global setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    pub log_namespace: Option<bool>,

    #[serde(default = "default_framing_message_based")]
    #[derivative(Default(value = "default_framing_message_based()"))]
    pub framing: FramingConfig,

    #[serde(default = "default_decoding")]
    #[derivative(Default(value = "default_decoding()"))]
    pub decoding: DeserializerConfig,

    #[serde(default, deserialize_with = "bool_or_struct")]
    pub acknowledgements: SourceAcknowledgementsConfig,
}

fn default_endpoint() -> String {
    PUBSUB_URL.to_string()
}

const fn default_ack_deadline() -> Duration {
    Duration::from_secs(600)
}

const fn default_retry_delay() -> Duration {
    Duration::from_secs(1)
}

const fn default_keepalive() -> Duration {
    Duration::from_secs(60)
}

const fn default_max_concurrency() -> usize {
    10
}

const fn default_full_response() -> usize {
    100
}

const fn default_poll_time() -> Duration {
    Duration::from_secs(2)
}

impl PubsubConfig {
    fn build_with_auth(
        &self,
        cx: SourceContext,
        auth: GcpAuthenticator,
    ) -> crate::Result<crate::sources::Source> {
        let log_namespace = cx.log_namespace(self.log_namespace);
        let ack_deadline_secs = match self.ack_deadline_seconds {
            None => self.ack_deadline_secs,
            Some(ads) => {
                warn!(
                    "The `ack_deadline_seconds` setting is deprecated, use `ack_deadline_secs` instead."
                );
                Duration::from_secs(ads as u64)
            }
        };
        if !(MIN_ACK_DEADLINE_SECS..=MAX_ACK_DEADLINE_SECS).contains(&ack_deadline_secs.as_secs()) {
            return Err(PubsubError::InvalidAckDeadline.into());
        }

        let retry_delay_secs = match self.retry_delay_seconds {
            None => self.retry_delay_secs,
            Some(rds) => {
                warn!(
                    "The `retry_delay_seconds` setting is deprecated, use `retry_delay_secs` instead."
                );
                Duration::from_secs_f64(rds)
            }
        };

        let mut uri: Uri = self.endpoint.parse().context(UriSnafu)?;
        auth.apply_uri(&mut uri);

        let tls = TlsSettings::from_options(self.tls.as_ref())?;
        let host = uri.host().unwrap_or("pubsub.googleapis.com");
        let mut tls_config = ClientTlsConfig::new().domain_name(host);
        if let Some((cert, key)) = tls.identity_pem() {
            tls_config = tls_config.identity(Identity::from_pem(cert, key));
        }
        for authority in tls.authorities_pem() {
            tls_config = tls_config.ca_certificate(Certificate::from_pem(authority));
        }

        let mut endpoint: Endpoint = uri.to_string().parse().context(EndpointSnafu)?;
        if uri.scheme() != Some(&Scheme::HTTP) {
            endpoint = endpoint.tls_config(tls_config).context(EndpointTlsSnafu)?;
        }

        let token_generator = auth.spawn_regenerate_token();

        let protocol = uri
            .scheme()
            .map(|scheme| Protocol(scheme.to_string().into()))
            .unwrap_or(Protocol::HTTP);

        let source = PubsubSource {
            endpoint,
            auth,
            token_generator,
            subscription: format!(
                "projects/{}/subscriptions/{}",
                self.project, self.subscription
            ),
            decoder: DecodingConfig::new(
                self.framing.clone(),
                self.decoding.clone(),
                log_namespace,
            )
            .build()?,
            acknowledgements: cx.do_acknowledgements(self.acknowledgements),
            shutdown: cx.shutdown,
            out: cx.out,
            ack_deadline_secs,
            retry_delay: retry_delay_secs,
            keepalive: self.keepalive_secs,
            concurrency: Default::default(),
            full_response_size: self.full_response_size,
            log_namespace,
            bytes_received: register!(BytesReceived::from(protocol)),
            events_received: register!(EventsReceived),
        }
        .run_all(self.max_concurrency, self.poll_time_seconds)
        .map_err(|error| error!(message = "Source failed.", %error));
        Ok(Box::pin(source))
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "gcp_pubsub")]
impl SourceConfig for PubsubConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<crate::sources::Source> {
        // Refused credentials must not fail the build (and with it every other source of this
        // Vector): the source starts, logs the error, and starts pulling once a token is granted.
        let auth = self.auth.build_deferred(Scope::PubSub).await?;
        self.build_with_auth(cx, auth)
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);
        let schema_definition = self
            .decoding
            .schema_definition(log_namespace)
            .with_standard_vector_source_metadata()
            .with_source_metadata(
                PubsubConfig::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!("timestamp"))),
                &owned_value_path!("timestamp"),
                Kind::timestamp().or_undefined(),
                Some("timestamp"),
            )
            .with_source_metadata(
                PubsubConfig::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!("attributes"))),
                &owned_value_path!("attributes"),
                Kind::object(Collection::empty().with_unknown(Kind::bytes())),
                None,
            )
            .with_source_metadata(
                PubsubConfig::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!("message_id"))),
                &owned_value_path!("message_id"),
                Kind::bytes(),
                None,
            );

        vec![SourceOutput::new_maybe_logs(
            DataType::Log,
            schema_definition,
        )]
    }

    fn can_acknowledge(&self) -> bool {
        true
    }
}

impl_generate_config_from_default!(PubsubConfig);

#[derive(Clone)]
struct PubsubSource {
    endpoint: Endpoint,
    auth: GcpAuthenticator,
    token_generator: watch::Receiver<()>,
    subscription: String,
    decoder: Decoder,
    acknowledgements: bool,
    ack_deadline_secs: Duration,
    shutdown: ShutdownSignal,
    out: SourceSender,
    retry_delay: Duration,
    keepalive: Duration,
    // The current concurrency is shared across all tasks. It is used
    // by the streams to avoid shutting down the last stream, which
    // would result in repeatedly re-opening the stream on idle.
    concurrency: Arc<AtomicUsize>,
    full_response_size: usize,
    log_namespace: LogNamespace,
    bytes_received: Registered<BytesReceived>,
    events_received: Registered<EventsReceived>,
}

enum State {
    RetryNow,
    RetryDelay,
    Shutdown,
}

impl PubsubSource {
    async fn run_all(mut self, max_concurrency: usize, poll_time: Duration) -> crate::Result<()> {
        let mut tasks = FuturesUnordered::new();

        loop {
            self.concurrency.store(tasks.len(), Ordering::Relaxed);
            tokio::select! {
                _ = &mut self.shutdown => break,
                _ = tasks.next() => {
                    if tasks.is_empty() {
                        // Either no tasks were started or a race
                        // condition resulted in the last task
                        // exiting. Start up a new stream immediately.
                        self.start_one(&tasks);
                    }

                },
                _ = tokio::time::sleep(poll_time) => {
                    // If all of the tasks are marked as busy, start
                    // up a new one.
                    if tasks.len() < max_concurrency
                        && tasks.iter().all(|task| task.busy_flag.load(Ordering::Relaxed))
                    {
                        self.start_one(&tasks);
                    }
                }
            }
        }

        // Wait for all active streams to exit on shutdown
        while tasks.next().await.is_some() {}

        Ok(())
    }

    fn start_one(&self, tasks: &FuturesUnordered<Task>) {
        info!(message = "Starting stream.", concurrency = tasks.len() + 1);
        // The `busy_flag` is used to monitor the status of a
        // stream. It will start marked as idle to prevent the above
        // scan from spinning up too many at once. When a stream
        // receives "full" batches, it will mark itself as busy, and
        // when it has an idle interval it will mark itself as not
        // busy.
        let busy_flag = Arc::new(AtomicBool::new(false));
        let task = crate::spawn_in_current_span(self.clone().run(Arc::clone(&busy_flag)));
        tasks.push(Task { task, busy_flag });
    }

    async fn run(mut self, busy_flag: Arc<AtomicBool>) {
        loop {
            match self.run_once(&busy_flag).await {
                State::RetryNow => debug!("Retrying immediately."),
                State::RetryDelay => {
                    info!(
                        timeout_secs = self.retry_delay.as_secs_f64(),
                        "Retrying after timeout."
                    );
                    tokio::time::sleep(self.retry_delay).await;
                }
                State::Shutdown => break,
            }
        }
    }

    async fn run_once(&mut self, busy_flag: &Arc<AtomicBool>) -> State {
        if self.auth.awaiting_token() {
            // The credentials were refused at startup: pulling unauthenticated would only fail.
            // The token regenerator keeps retrying (and logging); pull once it gets a token.
            info!("Waiting for a GCP authentication token before pulling.");
            tokio::select! {
                _ = &mut self.shutdown => return State::Shutdown,
                changed = self.token_generator.changed() => if changed.is_err() {
                    return State::RetryDelay;
                },
            }
        }

        let connection = match self.endpoint.connect().await {
            Ok(connection) => connection,
            Err(error) => {
                emit!(GcpPubsubConnectError { error });
                return State::RetryDelay;
            }
        };

        let mut client = proto::subscriber_client::SubscriberClient::with_interceptor(
            connection,
            |mut req: Request<()>| {
                if let Some(token) = self.auth.make_token() {
                    let authorization = MetadataValue::try_from(&token).map_err(|_| {
                        Status::new(
                            Code::FailedPrecondition,
                            "Invalid token text returned by GCP",
                        )
                    })?;
                    req.metadata_mut().insert("authorization", authorization);
                }
                Ok(req)
            },
        )
        // Tonic added a default of 4MB in 0.9. This replaces the old behavior.
        .max_decoding_message_size(usize::MAX);

        let (ack_ids_sender, ack_ids_receiver) = mpsc::channel::<StreamUpdate>(ACK_QUEUE_SIZE);

        // Handle shutdown during startup, the streaming pull doesn't
        // start if there is no data in the subscription.
        let request_stream = self.request_stream(ack_ids_receiver);
        debug!("Starting streaming pull.");
        let stream = tokio::select! {
            _ = &mut self.shutdown => return State::Shutdown,
            result = client.streaming_pull(request_stream) => match result {
                Ok(stream) => stream,
                Err(error) => {
                    emit!(GcpPubsubStreamingPullError { error });
                    return State::RetryDelay;
                }
            }
        };
        let mut stream = stream.into_inner();

        // No shutdown signal here: on shutdown the loop below keeps reading acknowledgements
        // until every message sent downstream is settled (or the drain times out).
        let (finalizer, mut ack_stream) = Finalizer::maybe_new(self.acknowledgements, None);
        let mut in_flight = InFlight::default();
        let mut drain_deadline: Option<tokio::time::Instant> = None;
        let mut released_on_receive = 0;

        let drained = loop {
            tokio::select! {
                biased;
                receipts = ack_stream.next() => if let Some((status, receipts)) = receipts {
                    in_flight.settle(&receipts);
                    if status == BatchStatus::Delivered {
                        ack_ids_sender
                            .send(StreamUpdate::Ack(receipts))
                            .await
                            .unwrap_or_else(|_| unreachable!("request stream never closes"));
                    }
                    if drain_deadline.is_some() && in_flight.is_empty() {
                        break Drained::Done;
                    }
                },
                _ = &mut self.shutdown, if drain_deadline.is_none() => {
                    if in_flight.is_empty() {
                        break Drained::Done;
                    }
                    debug!(
                        message = "Shutting down; waiting for acknowledgements.",
                        in_flight = in_flight.len(),
                    );
                    drain_deadline = Some(tokio::time::Instant::now() + SHUTDOWN_ACK_TIMEOUT);
                },
                _ = tokio::time::sleep_until(drain_deadline.unwrap_or_else(tokio::time::Instant::now)),
                    if drain_deadline.is_some() => break Drained::TimedOut,
                response = stream.next() => match response {
                    // Shutting down: hand new messages straight back instead of processing them.
                    Some(Ok(response)) if drain_deadline.is_some() => {
                        let ids: Vec<String> = response
                            .received_messages
                            .into_iter()
                            .map(|message| message.ack_id)
                            .collect();
                        released_on_receive += ids.len();
                        send_release(&ack_ids_sender, ids).await;
                    }
                    Some(Ok(response)) => {
                        self.handle_response(
                            response,
                            &finalizer,
                            &ack_ids_sender,
                            &mut in_flight,
                            busy_flag,
                        ).await;
                    }
                    Some(Err(_)) | None if drain_deadline.is_some() => break Drained::StreamEnded,
                    Some(Err(error)) => return translate_error(error),
                    None => return State::RetryNow,
                },
                _ = self.token_generator.changed(), if drain_deadline.is_none() => {
                    debug!("New authentication token generated, restarting stream.");
                    return State::RetryNow;
                },
                _ = tokio::time::sleep(self.keepalive) => {
                    if in_flight.is_empty() && drain_deadline.is_none() {
                        // No pending acks, and no new data, so drop
                        // this stream if we aren't the only active
                        // one.
                        if self.concurrency.load(Ordering::Relaxed) > 1 {
                            info!("Shutting down inactive stream.");
                            return State::Shutdown;
                        }
                        // Otherwise, mark this stream as idle.
                        busy_flag.store(false, Ordering::Relaxed);
                    }
                    // GCP Pub/Sub likes to time out connections after
                    // about 75 seconds of inactivity. To forestall
                    // the resulting error, send an empty array of
                    // acknowledgement IDs to the request stream if no
                    // other activity has happened. This will result
                    // in a new request with empty fields, effectively
                    // a keepalive.
                    ack_ids_sender
                        .send(StreamUpdate::Ack(Vec::new()))
                        .await
                        .unwrap_or_else(|_| unreachable!("request stream never closes"));
                }
            }
        };

        // Shutdown. Stop reading acknowledgements first: from here on, no ID is acknowledged, so
        // releasing the unsettled ones cannot race an acknowledgement.
        drop(ack_stream);
        drop(finalizer);
        let unsettled = in_flight.take();
        let unsettled_count = unsettled.len();
        let stream_ended = matches!(drained, Drained::StreamEnded);
        let mut late = 0;
        if !stream_ended {
            // Send the releases, then close the request side: the server ends the stream once it
            // has processed the acknowledgements and releases sent so far. All within one budget.
            let finish = async {
                send_release(&ack_ids_sender, unsettled).await;
                drop(ack_ids_sender);
                while let Some(Ok(response)) = stream.next().await {
                    late += response.received_messages.len();
                }
            };
            if tokio::time::timeout(SHUTDOWN_FLUSH_TIMEOUT, finish)
                .await
                .is_err()
            {
                debug!("Timed out releasing messages or waiting for the streaming pull to close.");
            }
        }
        let (released, not_released) = if stream_ended {
            (released_on_receive, unsettled_count + late)
        } else {
            (released_on_receive + unsettled_count, late)
        };
        if released + not_released > 0 {
            emit!(SourceMessagesReleased {
                component_type: "gcp_pubsub",
                released,
                not_released,
                timed_out: matches!(drained, Drained::TimedOut),
            });
        }
        State::Shutdown
    }

    fn request_stream(
        &self,
        ack_ids: mpsc::Receiver<StreamUpdate>,
    ) -> impl Stream<Item = proto::StreamingPullRequest> + 'static + use<> {
        let subscription = self.subscription.clone();
        let client_id = CLIENT_ID.clone();
        let stream_ack_deadline_seconds = self.ack_deadline_secs.as_secs() as i32;
        let ack_ids = ReceiverStream::new(ack_ids).ready_chunks(ACK_QUEUE_SIZE);

        stream::once(async move {
            // These fields are only valid on the first request in the
            // stream, and so must not be repeated below.
            proto::StreamingPullRequest {
                subscription,
                client_id,
                stream_ack_deadline_seconds,
                ..Default::default()
            }
        })
        // These "requests" serve only to send updates about
        // acknowledgements (and, on shutdown, releases) to the server.
        // None of the above fields need to be repeated and, in fact,
        // will cause an stream error and cancellation if they are
        // present.
        .chain(ack_ids.map(update_request))
    }

    async fn handle_response(
        &mut self,
        response: proto::StreamingPullResponse,
        finalizer: &Option<Finalizer>,
        ack_ids: &mpsc::Sender<StreamUpdate>,
        in_flight: &mut InFlight,
        busy_flag: &Arc<AtomicBool>,
    ) {
        if response.received_messages.len() >= self.full_response_size {
            busy_flag.store(true, Ordering::Relaxed);
        }
        self.bytes_received.emit(ByteSize(response.size_of()));

        let (batch, notifier) = BatchNotifier::maybe_new_with_receiver(self.acknowledgements);
        let (events, ids) = self.parse_messages(response.received_messages, batch);

        let count = events.len();
        match self.out.send_batch(events).await {
            Err(_) => emit!(StreamClosedError { count }),
            Ok(()) => match notifier {
                None => ack_ids
                    .send(StreamUpdate::Ack(ids))
                    .await
                    .unwrap_or_else(|_| unreachable!("request stream never closes")),
                Some(notifier) => {
                    in_flight.add(&ids);
                    finalizer
                        .as_ref()
                        .expect("Finalizer must have been set up for acknowledgements")
                        .add(ids, notifier);
                }
            },
        }
    }

    fn parse_messages(
        &self,
        response: Vec<proto::ReceivedMessage>,
        batch: Option<BatchNotifier>,
    ) -> (Vec<Event>, Vec<String>) {
        let mut ack_ids = Vec::with_capacity(response.len());
        let events = response
            .into_iter()
            .flat_map(|received| {
                ack_ids.push(received.ack_id);
                received
                    .message
                    .map(|message| self.parse_message(message, &batch))
            })
            .flatten()
            .collect();
        (events, ack_ids)
    }

    fn parse_message<'a>(
        &'a self,
        message: proto::PubsubMessage,
        batch: &'a Option<BatchNotifier>,
    ) -> impl Iterator<Item = Event> + 'a {
        let attributes = Value::Object(
            message
                .attributes
                .into_iter()
                .map(|(key, value)| (key.into(), Value::Bytes(value.into())))
                .collect(),
        );
        let log_namespace = self.log_namespace;
        util::decode_message(
            self.decoder.clone(),
            "gcp_pubsub",
            &message.data,
            message.publish_time.map(|dt| {
                DateTime::from_timestamp(dt.seconds, dt.nanos as u32).expect("invalid timestamp")
            }),
            batch,
            log_namespace,
            &self.events_received,
        )
        .map(move |mut event| {
            if let Some(log) = event.maybe_as_log_mut() {
                log_namespace.insert_source_metadata(
                    PubsubConfig::NAME,
                    log,
                    Some(LegacyKey::Overwrite(path!("message_id"))),
                    path!("message_id"),
                    message.message_id.clone(),
                );
                log_namespace.insert_source_metadata(
                    PubsubConfig::NAME,
                    log,
                    Some(LegacyKey::Overwrite(path!("attributes"))),
                    path!("attributes"),
                    attributes.clone(),
                )
            }
            event
        })
    }
}

/// Sends a release for `ids`, in chunks. Best effort: if the request stream is gone, the messages
/// are redelivered when their ack deadline expires, as before.
async fn send_release(sender: &mpsc::Sender<StreamUpdate>, ids: Vec<String>) {
    for chunk in ids.chunks(RELEASE_CHUNK_SIZE) {
        if sender
            .send(StreamUpdate::Release(chunk.to_vec()))
            .await
            .is_err()
        {
            break;
        }
    }
}

fn translate_error(error: tonic::Status) -> State {
    // GCP occasionally issues a connection reset
    // in the middle of the streaming pull. This
    // reset is not technically an error, so we
    // want to retry immediately, but it is
    // reported to us as an error from the
    // underlying library (`tonic`).
    if is_reset(&error) {
        debug!("Stream reset by server.");
        State::RetryNow
    } else {
        emit!(GcpPubsubReceiveError { error });
        State::RetryDelay
    }
}

fn is_reset(error: &Status) -> bool {
    error
        .source()
        .and_then(|source| source.downcast_ref::<hyper::Error>())
        .and_then(|error| error.source())
        .and_then(|source| source.downcast_ref::<h2::Error>())
        .is_some_and(|error| error.is_remote() && error.is_reset())
}

#[pin_project::pin_project]
struct Task {
    task: tokio::task::JoinHandle<()>,
    busy_flag: Arc<AtomicBool>,
}

impl Future for Task {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        self.task.poll_unpin(ctx)
    }
}

#[cfg(test)]
mod tests {
    use vector_lib::{lookup::OwnedTargetPath, schema::Definition};

    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<PubsubConfig>();
    }

    #[test]
    fn output_schema_definition_vector_namespace() {
        let config = PubsubConfig {
            log_namespace: Some(true),
            ..Default::default()
        };

        let definitions = config
            .outputs(LogNamespace::Vector)
            .remove(0)
            .schema_definition(true);

        let expected_definition =
            Definition::new_with_default_metadata(Kind::bytes(), [LogNamespace::Vector])
                .with_meaning(OwnedTargetPath::event_root(), "message")
                .with_metadata_field(
                    &owned_value_path!("vector", "source_type"),
                    Kind::bytes(),
                    None,
                )
                .with_metadata_field(
                    &owned_value_path!("vector", "ingest_timestamp"),
                    Kind::timestamp(),
                    None,
                )
                .with_metadata_field(
                    &owned_value_path!("gcp_pubsub", "timestamp"),
                    Kind::timestamp().or_undefined(),
                    Some("timestamp"),
                )
                .with_metadata_field(
                    &owned_value_path!("gcp_pubsub", "attributes"),
                    Kind::object(Collection::empty().with_unknown(Kind::bytes())),
                    None,
                )
                .with_metadata_field(
                    &owned_value_path!("gcp_pubsub", "message_id"),
                    Kind::bytes(),
                    None,
                );

        assert_eq!(definitions, Some(expected_definition));
    }

    #[test]
    fn output_schema_definition_legacy_namespace() {
        let config = PubsubConfig::default();

        let definitions = config
            .outputs(LogNamespace::Legacy)
            .remove(0)
            .schema_definition(true);

        let expected_definition = Definition::new_with_default_metadata(
            Kind::object(Collection::empty()),
            [LogNamespace::Legacy],
        )
        .with_event_field(
            &owned_value_path!("message"),
            Kind::bytes(),
            Some("message"),
        )
        .with_event_field(
            &owned_value_path!("timestamp"),
            Kind::timestamp().or_undefined(),
            Some("timestamp"),
        )
        .with_event_field(&owned_value_path!("source_type"), Kind::bytes(), None)
        .with_event_field(
            &owned_value_path!("attributes"),
            Kind::object(Collection::empty().with_unknown(Kind::bytes())),
            None,
        )
        .with_event_field(&owned_value_path!("message_id"), Kind::bytes(), None);

        assert_eq!(definitions, Some(expected_definition));
    }
}

/// Shutdown with messages received but not acknowledged, against a fake Pub/Sub server that
/// records every acknowledgement and deadline modification it receives.
#[cfg(test)]
mod shutdown_release_tests {
    use std::{collections::HashMap, sync::Mutex};

    use tokio::time::Instant;
    use tonic::transport::Server;
    use vector_lib::{event::EventStatus, finalization::Finalizable};

    use super::{
        proto::{
            PubsubMessage, ReceivedMessage, StreamingPullRequest, StreamingPullResponse,
            subscriber_server::{Subscriber, SubscriberServer},
        },
        *,
    };
    use crate::{
        config::ComponentKey, shutdown::SourceShutdownCoordinator, test_util::addr::next_addr,
    };

    #[derive(Default)]
    struct Recorded {
        acks: Vec<String>,
        /// (ack id, new deadline)
        modacks: Vec<(String, i32)>,
        /// The open stream's sender, for responses pushed by the test.
        outbound: Option<mpsc::Sender<Result<StreamingPullResponse, Status>>>,
        /// The `authorization` metadata of every streaming pull.
        authorizations: Vec<Option<String>>,
    }

    #[derive(Clone, Default)]
    struct FakePubsub(Arc<Mutex<Recorded>>);

    #[tonic::async_trait]
    impl Subscriber for FakePubsub {
        type StreamingPullStream = ReceiverStream<Result<StreamingPullResponse, Status>>;

        async fn streaming_pull(
            &self,
            request: tonic::Request<tonic::Streaming<StreamingPullRequest>>,
        ) -> Result<tonic::Response<Self::StreamingPullStream>, Status> {
            let (tx, rx) = mpsc::channel(16);
            let authorization = request
                .metadata()
                .get("authorization")
                .map(|value| value.to_str().unwrap().to_string());
            {
                let mut recorded = self.0.lock().unwrap();
                recorded.outbound = Some(tx);
                recorded.authorizations.push(authorization);
            }
            let recorded = Arc::clone(&self.0);
            let mut inbound = request.into_inner();
            tokio::spawn(async move {
                while let Some(Ok(request)) = inbound.next().await {
                    let mut recorded = recorded.lock().unwrap();
                    recorded.acks.extend(request.ack_ids);
                    assert_eq!(
                        request.modify_deadline_ack_ids.len(),
                        request.modify_deadline_seconds.len()
                    );
                    recorded.modacks.extend(
                        request
                            .modify_deadline_ack_ids
                            .into_iter()
                            .zip(request.modify_deadline_seconds),
                    );
                }
                // The client closed its side: end the stream, as Pub/Sub does.
                recorded.lock().unwrap().outbound = None;
            });
            Ok(tonic::Response::new(ReceiverStream::new(rx)))
        }
    }

    impl FakePubsub {
        async fn send(&self, ids: &[&str]) {
            let response = StreamingPullResponse {
                received_messages: ids
                    .iter()
                    .map(|id| ReceivedMessage {
                        ack_id: id.to_string(),
                        message: Some(PubsubMessage {
                            data: id.as_bytes().to_vec().into(),
                            message_id: id.to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };
            let sender = self
                .0
                .lock()
                .unwrap()
                .outbound
                .clone()
                .expect("stream open");
            sender.send(Ok(response)).await.unwrap();
        }

        fn acks(&self) -> Vec<String> {
            self.0.lock().unwrap().acks.clone()
        }

        fn modacks(&self) -> Vec<(String, i32)> {
            self.0.lock().unwrap().modacks.clone()
        }

        async fn wait_for(&self, what: &str, condition: impl Fn(&Recorded) -> bool) {
            let deadline = Instant::now() + Duration::from_secs(10);
            while !condition(&self.0.lock().unwrap()) {
                assert!(Instant::now() < deadline, "timed out waiting for {what}");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    struct Harness {
        _port: crate::test_util::addr::PortGuard,
        server: FakePubsub,
        events: SourceSenderStream,
        shutdown: SourceShutdownCoordinator,
        component: ComponentKey,
        source: tokio::task::JoinHandle<()>,
    }

    type SourceSenderStream = std::pin::Pin<Box<dyn Stream<Item = Event> + Send>>;

    async fn start() -> Harness {
        let server = FakePubsub::default();
        let (port, addr) = next_addr();
        let service = SubscriberServer::new(server.clone());
        tokio::spawn(Server::builder().add_service(service).serve(addr));

        let (tx, rx) = SourceSender::new_test();
        let component = ComponentKey::from("gcp_pubsub");
        // Parsed, so that every option has its real default (ack deadline 600 s etc.).
        let config: PubsubConfig = toml::from_str(&format!(
            r#"
            project = "project"
            subscription = "subscription"
            endpoint = "http://{addr}"
            skip_authentication = true
            "#
        ))
        .unwrap();
        let (mut cx, shutdown) = SourceContext::new_shutdown(&component, tx);
        cx.acknowledgements = true;
        // The server may take a moment to listen; the source retries connecting.
        let source = config.build(cx).await.expect("build source");
        let source = tokio::spawn(async move { source.await.expect("source failed") });
        server
            .wait_for("the streaming pull", |r| r.outbound.is_some())
            .await;
        Harness {
            _port: port,
            server,
            events: Box::pin(rx),
            shutdown,
            component,
            source,
        }
    }

    impl Harness {
        /// Receives `count` events, keyed by message ID.
        async fn receive(&mut self, count: usize) -> HashMap<String, Event> {
            let mut events = HashMap::new();
            for _ in 0..count {
                let event = tokio::time::timeout(Duration::from_secs(10), self.events.next())
                    .await
                    .expect("timed out receiving")
                    .expect("stream ended");
                let id = event
                    .as_log()
                    .get_message()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                events.insert(id, event);
            }
            events
        }

        /// Shuts the source down and waits for it to finish.
        async fn shut_down(mut self) -> FakePubsub {
            let deadline = Instant::now() + Duration::from_secs(30);
            assert!(
                self.shutdown
                    .shutdown_source(&self.component, deadline)
                    .await,
                "source did not shut down in time"
            );
            self.source.await.unwrap();
            self.server
        }
    }

    fn deliver(event: Event) {
        let mut event = event;
        event
            .take_finalizers()
            .update_status(EventStatus::Delivered);
    }

    fn sorted(mut ids: Vec<String>) -> Vec<String> {
        ids.sort();
        ids
    }

    /// Refused credentials (here: the metadata server refusing the first token) do not fail the
    /// build. The source starts without pulling, and once a token is granted it pulls, with that
    /// token, without being restarted.
    #[tokio::test]
    async fn refused_credentials_start_the_source_and_it_pulls_once_they_work() {
        let metadata = crate::gcp::test_util::FakeMetadata::start(1);
        let server = FakePubsub::default();
        let (_port, addr) = next_addr();
        let service = SubscriberServer::new(server.clone());
        tokio::spawn(Server::builder().add_service(service).serve(addr));

        let (tx, rx) = SourceSender::new_test();
        let mut events: SourceSenderStream = Box::pin(rx);
        let component = ComponentKey::from("gcp_pubsub");
        let config: PubsubConfig = toml::from_str(&format!(
            r#"
            project = "project"
            subscription = "subscription"
            endpoint = "http://{addr}"
            "#
        ))
        .unwrap();
        let (mut cx, mut shutdown) = SourceContext::new_shutdown(&component, tx);
        cx.acknowledgements = true;
        // What `build` does, with the metadata server pointed at the fake.
        let auth = GcpAuthenticator::new_implicit_deferred(metadata.url.clone())
            .await
            .expect("refused credentials must not fail the build");
        let source = config.build_with_auth(cx, auth).expect("build source");
        let source = tokio::spawn(async move { source.await.expect("source failed") });

        // Refused: the source runs, but does not pull without a token.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(!source.is_finished());
        assert!(server.0.lock().unwrap().authorizations.is_empty());
        assert_eq!(metadata.requests(), 1);

        // The token regenerator retries after 2 s and gets a token: the source pulls with it.
        server
            .wait_for("the streaming pull", |r| r.outbound.is_some())
            .await;
        assert_eq!(
            server.0.lock().unwrap().authorizations,
            vec![Some("Bearer token-2".to_string())]
        );
        server.send(&["a"]).await;
        let event = tokio::time::timeout(Duration::from_secs(10), events.next())
            .await
            .expect("timed out receiving")
            .expect("stream ended");
        deliver(event);
        server
            .wait_for("the acknowledgement", |r| r.acks == ["a"])
            .await;

        let deadline = Instant::now() + Duration::from_secs(30);
        assert!(shutdown.shutdown_source(&component, deadline).await);
        source.await.unwrap();
    }

    /// Malformed configuration still fails the build.
    #[tokio::test]
    async fn bad_configuration_still_fails_the_build() {
        for (case, auth) in [
            ("bad API key", r#"api_key = "abc%xyz""#),
            (
                "missing credentials file",
                r#"credentials_path = "/nonexistent/credentials.json""#,
            ),
        ] {
            let config: PubsubConfig = toml::from_str(&format!(
                r#"
                project = "project"
                subscription = "subscription"
                {auth}
                "#
            ))
            .unwrap();
            let (tx, _rx) = SourceSender::new_test();
            let component = ComponentKey::from("gcp_pubsub");
            let (cx, _shutdown) = SourceContext::new_shutdown(&component, tx);
            assert!(
                config.build(cx).await.is_err(),
                "{case} must fail the build"
            );
        }
    }

    /// Delivered messages are acknowledged; the ones still in flight at shutdown are released
    /// with a zero deadline, each exactly once, and never acknowledged.
    #[tokio::test]
    async fn shutdown_releases_in_flight_messages_and_acks_delivered_ones() {
        let mut harness = start().await;
        harness.server.send(&["a0", "a1", "a2"]).await;
        harness.server.send(&["b0", "b1"]).await;
        let mut events = harness.receive(5).await;
        for id in ["a0", "a1", "a2"] {
            deliver(events.remove(id).unwrap());
        }
        harness
            .server
            .wait_for("the acks", |r| r.acks.len() == 3)
            .await;

        // b0 and b1 are still held downstream when the source shuts down.
        let started = Instant::now();
        let server = harness.server.clone();
        let shutdown = tokio::spawn(harness.shut_down());
        // The release goes out right at the end of the acknowledgement wait...
        server
            .wait_for("the release", |r| r.modacks.len() == 2)
            .await;
        let released_after = started.elapsed();
        assert!(released_after >= SHUTDOWN_ACK_TIMEOUT, "{released_after:?}");
        assert!(
            released_after < SHUTDOWN_ACK_TIMEOUT + Duration::from_secs(1),
            "{released_after:?}"
        );
        let server = shutdown.await.unwrap();
        drop(events);

        assert_eq!(sorted(server.acks()), vec!["a0", "a1", "a2"]);
        let modacks = server.modacks();
        assert_eq!(
            sorted(modacks.iter().map(|(id, _)| id.clone()).collect()),
            vec!["b0", "b1"]
        );
        assert!(
            modacks.iter().all(|(_, deadline)| *deadline == 0),
            "{modacks:?}"
        );
        // ...and the whole shutdown stays within the acknowledgement wait plus the flush.
        assert!(
            started.elapsed() < SHUTDOWN_ACK_TIMEOUT + SHUTDOWN_FLUSH_TIMEOUT,
            "{:?}",
            started.elapsed()
        );
    }

    /// An acknowledgement that comes back during the drain is still sent as an ack, and that
    /// message is not released.
    #[tokio::test]
    async fn acknowledgements_during_the_drain_are_acked_not_released() {
        let mut harness = start().await;
        // Separate responses: a response is one acknowledgement batch.
        harness.server.send(&["c0"]).await;
        harness.server.send(&["c1"]).await;
        let mut events = harness.receive(2).await;
        let c0 = events.remove("c0").unwrap();
        let c1 = events.remove("c1").unwrap();
        let shutdown = tokio::spawn(harness.shut_down());
        tokio::time::sleep(Duration::from_millis(200)).await;
        deliver(c0);
        let server = shutdown.await.unwrap();
        drop(c1);

        assert_eq!(server.acks(), vec!["c0"]);
        assert_eq!(server.modacks(), vec![("c1".to_string(), 0)]);
    }

    /// Messages that arrive after shutdown started are handed straight back, not processed.
    #[tokio::test]
    async fn messages_received_while_draining_are_released_unprocessed() {
        let mut harness = start().await;
        harness.server.send(&["d0"]).await;
        let held = harness.receive(1).await;
        let server = harness.server.clone();
        let mut events = std::mem::replace(&mut harness.events, Box::pin(futures::stream::empty()));
        let shutdown = tokio::spawn(harness.shut_down());
        tokio::time::sleep(Duration::from_millis(200)).await;
        server.send(&["e0", "e1"]).await;
        let server = shutdown.await.unwrap();
        drop(held);

        assert!(server.acks().is_empty());
        assert_eq!(
            sorted(server.modacks().into_iter().map(|(id, _)| id).collect()),
            vec!["d0", "e0", "e1"]
        );
        // e0 and e1 never reached the pipeline.
        assert!(events.next().now_or_never().flatten().is_none());
    }

    /// With everything acknowledged, shutdown releases nothing and does not wait.
    #[tokio::test]
    async fn shutdown_with_nothing_in_flight_releases_nothing() {
        let mut harness = start().await;
        harness.server.send(&["f0"]).await;
        let mut events = harness.receive(1).await;
        deliver(events.remove("f0").unwrap());
        harness
            .server
            .wait_for("the ack", |r| r.acks.len() == 1)
            .await;
        let started = Instant::now();
        let server = harness.shut_down().await;
        assert!(started.elapsed() < SHUTDOWN_ACK_TIMEOUT);
        assert_eq!(server.acks(), vec!["f0"]);
        assert!(server.modacks().is_empty());
    }

    /// The worst case (acknowledgements that never come, a slow release or close) fits in the
    /// graceful shutdown limit (`VECTOR_GRACEFUL_SHUTDOWN_LIMIT_SECS` in nanosiem's
    /// `pull-container.js`), so the release is always sent.
    #[test]
    fn shutdown_fits_the_graceful_shutdown_limit() {
        // The release starts by 8 s; with the flush, everything ends by 13 s.
        assert_eq!(SHUTDOWN_ACK_WAIT, Duration::from_secs(8));
        assert_eq!(
            SHUTDOWN_ACK_WAIT + SHUTDOWN_FLUSH_TIMEOUT,
            Duration::from_secs(13)
        );
        assert!(SHUTDOWN_ACK_WAIT + SHUTDOWN_FLUSH_TIMEOUT < GRACEFUL_SHUTDOWN_LIMIT);
    }

    #[test]
    fn in_flight_settles_and_takes() {
        let mut in_flight = InFlight::default();
        in_flight.add(&["a".into(), "b".into(), "c".into()]);
        in_flight.settle(&["b".into()]);
        assert_eq!(in_flight.len(), 2);
        let mut taken = in_flight.take();
        taken.sort();
        assert_eq!(taken, vec!["a", "c"]);
        assert!(in_flight.is_empty());
    }

    #[test]
    fn update_request_combines_acks_and_releases() {
        let request = update_request(vec![
            StreamUpdate::Ack(vec!["a".into()]),
            StreamUpdate::Release(vec!["r1".into(), "r2".into()]),
            StreamUpdate::Ack(vec!["b".into()]),
        ]);
        assert_eq!(request.ack_ids, vec!["a", "b"]);
        assert_eq!(request.modify_deadline_ack_ids, vec!["r1", "r2"]);
        assert_eq!(request.modify_deadline_seconds, vec![0, 0]);
        assert!(request.subscription.is_empty());
        assert_eq!(request.stream_ack_deadline_seconds, 0);
    }
}

#[cfg(all(test, feature = "gcp-integration-tests"))]
mod integration_tests {
    use std::{
        collections::{BTreeMap, HashSet},
        sync::LazyLock,
    };

    use base64::prelude::{BASE64_STANDARD, Engine as _};
    use chrono::{DateTime, Utc};
    use futures::{Stream, StreamExt};
    use http::method::Method;
    use hyper::{Request, StatusCode};
    use serde_json::{Value, json};
    use tokio::time::{Duration, Instant};
    use vrl::{btreemap, event_path};

    use super::*;
    use crate::{
        SourceSender,
        config::{ComponentKey, ProxyConfig},
        event::EventStatus,
        gcp,
        http::HttpClient,
        shutdown,
        test_util::{
            self, components,
            components::{SOURCE_TAGS, assert_source_compliance},
            random_string,
        },
    };

    const PROJECT: &str = "sourceproject";
    static PROJECT_URI: LazyLock<String> =
        LazyLock::new(|| format!("{}/v1/projects/{PROJECT}", *gcp::PUBSUB_ADDRESS));
    static ACK_DEADLINE: LazyLock<Duration> = LazyLock::new(|| Duration::from_secs(10)); // Minimum custom deadline allowed by Pub/Sub

    #[ignore = "https://github.com/vectordotdev/vector/issues/24133"]
    #[tokio::test]
    async fn oneshot() {
        assert_source_compliance(&SOURCE_TAGS, async move {
            let (tester, mut rx, shutdown) = setup(EventStatus::Delivered).await;
            let test_data = tester.send_test_events(99, BTreeMap::new()).await;
            receive_events(&mut rx, test_data).await;
            tester.shutdown_check(shutdown).await;
        })
        .await;
    }

    #[ignore = "https://github.com/vectordotdev/vector/issues/24133"]
    #[tokio::test]
    async fn shuts_down_before_data_received() {
        let (tester, mut rx, shutdown) = setup(EventStatus::Delivered).await;

        tester.shutdown(shutdown).await; // Not shutdown_check because this emits nothing

        assert!(rx.next().await.is_none());
        tester.send_test_events(1, BTreeMap::new()).await;
        assert!(rx.next().await.is_none());
        assert_eq!(tester.pull_count(1).await, 1);
    }

    #[ignore = "https://github.com/vectordotdev/vector/issues/24133"]
    #[tokio::test]
    async fn shuts_down_after_data_received() {
        assert_source_compliance(&SOURCE_TAGS, async move {
            let (tester, mut rx, shutdown) = setup(EventStatus::Delivered).await;

            let test_data = tester.send_test_events(1, BTreeMap::new()).await;
            receive_events(&mut rx, test_data).await;

            tester.shutdown_check(shutdown).await;

            assert!(rx.next().await.is_none());
            tester.send_test_events(1, BTreeMap::new()).await;
            assert!(rx.next().await.is_none());
            // The following assert is there to test that the source isn't
            // pulling anything out of the subscription after it reports
            // shutdown. It works when there wasn't anything previously in
            // the topic, but does not work here despite evidence that the
            // entire tokio task has exited.
            // assert_eq!(tester.pull_count(1).await, 1);
        })
        .await;
    }

    #[ignore = "https://github.com/vectordotdev/vector/issues/24133"]
    #[tokio::test]
    async fn streams_data() {
        assert_source_compliance(&SOURCE_TAGS, async move {
            let (tester, mut rx, shutdown) = setup(EventStatus::Delivered).await;
            for _ in 0..10 {
                let test_data = tester.send_test_events(9, BTreeMap::new()).await;
                receive_events(&mut rx, test_data).await;
            }
            tester.shutdown_check(shutdown).await;
        })
        .await;
    }

    #[ignore = "https://github.com/vectordotdev/vector/issues/24133"]
    #[tokio::test]
    async fn sends_attributes() {
        assert_source_compliance(&SOURCE_TAGS, async move {
            let (tester, mut rx, shutdown) = setup(EventStatus::Delivered).await;
            let attributes = btreemap![
                random_string(8) => random_string(88),
                random_string(8) => random_string(88),
                random_string(8) => random_string(88),
            ];
            let test_data = tester.send_test_events(1, attributes).await;
            receive_events(&mut rx, test_data).await;
            tester.shutdown_check(shutdown).await;
        })
        .await;
    }

    #[ignore = "https://github.com/vectordotdev/vector/issues/24133"]
    #[tokio::test]
    async fn acks_received() {
        assert_source_compliance(&SOURCE_TAGS, async move {
            let (tester, mut rx, shutdown) = setup(EventStatus::Delivered).await;

            let test_data = tester.send_test_events(1, BTreeMap::new()).await;
            receive_events(&mut rx, test_data).await;

            tester.shutdown_check(shutdown).await;

            // Make sure there are no messages left in the queue
            assert_eq!(tester.pull_count(10).await, 0);

            // Wait for the acknowledgement deadline to expire
            tokio::time::sleep(*ACK_DEADLINE + Duration::from_secs(1)).await;

            // All messages are still acknowledged
            assert_eq!(tester.pull_count(10).await, 0);
        })
        .await;
    }

    #[tokio::test]
    // I have verified manually that the streaming code above omits the
    // acknowledgements when events are rejected, but have been unable
    // to verify the events are not acknowledged through the emulator.
    #[ignore]
    async fn does_not_ack_rejected() {
        assert_source_compliance(&SOURCE_TAGS, async {
            let (tester, mut rx, shutdown) = setup(EventStatus::Rejected).await;

            let test_data = tester.send_test_events(1, BTreeMap::new()).await;
            receive_events(&mut rx, test_data).await;

            tester.shutdown(shutdown).await;

            // Make sure there are no messages left in the queue
            assert_eq!(tester.pull_count(10).await, 0);

            // Wait for the acknowledgement deadline to expire
            tokio::time::sleep(*ACK_DEADLINE + Duration::from_secs(1)).await;

            // All messages are still in the queue
            assert_eq!(tester.pull_count(10).await, 1);
        })
        .await;
    }

    async fn setup(
        status: EventStatus,
    ) -> (
        Tester,
        impl Stream<Item = Event> + Unpin,
        shutdown::SourceShutdownCoordinator,
    ) {
        components::init_test();

        let tls_settings = TlsSettings::from_options(None).unwrap();
        let client = HttpClient::new(tls_settings, &ProxyConfig::default()).unwrap();
        let tester = Tester::new(client).await;

        let (rx, shutdown) = tester.spawn_source(status).await;

        (tester, rx, shutdown)
    }

    fn now_trunc() -> DateTime<Utc> {
        let start = Utc::now().timestamp();
        // Truncate the milliseconds portion, the hard way.
        DateTime::from_timestamp(start, 0).expect("invalid timestamp")
    }

    struct Tester {
        client: HttpClient,
        topic: String,
        subscription: String,
        component: ComponentKey,
    }

    struct TestData {
        lines: Vec<String>,
        start: DateTime<Utc>,
        attributes: BTreeMap<String, String>,
    }

    impl Tester {
        async fn new(client: HttpClient) -> Self {
            let this = Self {
                client,
                topic: format!("topic-{}", random_string(10).to_lowercase()),
                subscription: format!("sub-{}", random_string(10).to_lowercase()),
                component: ComponentKey::from("gcp_pubsub"),
            };

            this.request(Method::PUT, "topics/{topic}", json!({})).await;

            let body = json!({
                "topic": format!("projects/{PROJECT}/topics/{}", this.topic),
                "ackDeadlineSeconds": *ACK_DEADLINE,
            });
            this.request(Method::PUT, "subscriptions/{sub}", body).await;

            this
        }

        async fn spawn_source(
            &self,
            status: EventStatus,
        ) -> (
            impl Stream<Item = Event> + Unpin + use<>,
            shutdown::SourceShutdownCoordinator,
        ) {
            let (tx, rx) = SourceSender::new_test_finalize(status);
            let config = PubsubConfig {
                project: PROJECT.into(),
                subscription: self.subscription.clone(),
                endpoint: gcp::PUBSUB_ADDRESS.clone(),
                auth: GcpAuthConfig {
                    skip_authentication: true,
                    ..Default::default()
                },
                ack_deadline_secs: *ACK_DEADLINE,
                ..Default::default()
            };
            let (mut ctx, shutdown) = SourceContext::new_shutdown(&self.component, tx);
            ctx.acknowledgements = true;
            let source = config.build(ctx).await.expect("Failed to build source");
            tokio::spawn(async move { source.await.expect("Failed to run source") });

            (rx, shutdown)
        }

        async fn send_test_events(
            &self,
            count: usize,
            attributes: BTreeMap<String, String>,
        ) -> TestData {
            let start = now_trunc();
            let lines: Vec<_> = test_util::random_lines(44).take(count).collect();
            let messages: Vec<_> = lines
                .iter()
                .map(|input| BASE64_STANDARD.encode(input))
                .map(|data| json!({ "data": data, "attributes": attributes.clone() }))
                .collect();
            let body = json!({ "messages": messages });
            self.request(Method::POST, "topics/{topic}:publish", body)
                .await;

            TestData {
                lines,
                start,
                attributes,
            }
        }

        async fn pull_count(&self, count: usize) -> usize {
            let response = self
                .request(
                    Method::POST,
                    "subscriptions/{sub}:pull",
                    json!({ "maxMessages": count, "returnImmediately": true }),
                )
                .await;
            response
                .get("receivedMessages")
                .map(|rm| rm.as_array().unwrap().len())
                .unwrap_or(0)
        }

        async fn request(&self, method: Method, base: &str, body: Value) -> Value {
            let path = base
                .replace("{topic}", &self.topic)
                .replace("{sub}", &self.subscription);
            let uri = [PROJECT_URI.as_str(), &path].join("/");
            let body = crate::serde::json::to_bytes(&body).unwrap().freeze();
            let request = Request::builder()
                .method(method)
                .uri(uri)
                .body(body.into())
                .unwrap();
            let response = self.client.send(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = http_body::Body::collect(response.into_body())
                .await
                .unwrap()
                .to_bytes();
            serde_json::from_str(core::str::from_utf8(&body).unwrap()).unwrap()
        }

        async fn shutdown_check(&self, shutdown: shutdown::SourceShutdownCoordinator) {
            self.shutdown(shutdown).await;
            components::SOURCE_TESTS.assert(&components::HTTP_PULL_SOURCE_TAGS);
        }

        async fn shutdown(&self, mut shutdown: shutdown::SourceShutdownCoordinator) {
            let deadline = Instant::now() + Duration::from_secs(1);
            let shutdown = shutdown.shutdown_source(&self.component, deadline);
            assert!(shutdown.await);
        }
    }

    async fn receive_events(rx: &mut (impl Stream<Item = Event> + Unpin), test_data: TestData) {
        let TestData {
            start,
            lines,
            attributes,
        } = test_data;

        let events: Vec<Event> = tokio::time::timeout(
            Duration::from_secs(1),
            test_util::collect_n_stream(rx, lines.len()),
        )
        .await
        .unwrap();

        let end = Utc::now();
        let mut message_ids = HashSet::new();

        assert_eq!(events.len(), lines.len());
        for (message, event) in lines.into_iter().zip(events) {
            let log = event.into_log();
            assert_eq!(log.get(event_path!("message")), Some(&message.into()));
            assert_eq!(
                log.get(event_path!("source_type")),
                Some(&"gcp_pubsub".into())
            );
            assert!(
                log.get(event_path!("timestamp"))
                    .unwrap()
                    .as_timestamp()
                    .unwrap()
                    >= &start
            );
            assert!(
                log.get(event_path!("timestamp"))
                    .unwrap()
                    .as_timestamp()
                    .unwrap()
                    <= &end
            );
            assert!(
                message_ids.insert(
                    log.get(event_path!("message_id"))
                        .unwrap()
                        .clone()
                        .to_string()
                ),
                "Message contained duplicate message_id"
            );
            let logattr = log
                .get(event_path!("attributes"))
                .expect("missing attributes")
                .as_object()
                .unwrap()
                .clone();
            assert_eq!(logattr.len(), attributes.len());
            for (a, b) in logattr.into_iter().zip(&attributes) {
                assert_eq!(&a.0, b.0.as_str());
                assert_eq!(a.1, b.1.clone().into());
            }
        }
    }
}
