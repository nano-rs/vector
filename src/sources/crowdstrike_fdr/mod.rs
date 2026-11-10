//! The `crowdstrike_fdr` source: CrowdStrike Falcon Data Replicator feeds.
//!
//! FDR publishes one SQS message per batch. The body is a manifest listing gzipped
//! newline-delimited JSON objects in S3 (often behind an access-point alias), not an S3 event
//! notification, so the `aws_s3` source cannot read it. This source long-polls the queue, fetches
//! every object a manifest lists, emits one event per line, and deletes the message only after
//! every event of every object has been acknowledged by the sinks. While it waits it heartbeats
//! the message's visibility; failures release the message for re-delivery.

use std::{path::PathBuf, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use snafu::Snafu;
use vector_lib::{
    codecs::decoding::{DeserializerConfig, FramingConfig},
    config::{LegacyKey, LogNamespace},
    configurable::configurable_component,
    lookup::owned_value_path,
};
use vrl::value::Kind;

use crate::{
    aws::{AwsTimeout, RegionOrEndpoint, auth::AwsAuthentication, create_client},
    codecs::DecodingConfig,
    common::{s3::S3ClientBuilder, sqs::SqsClientBuilder},
    config::{
        GenerateConfig, SourceAcknowledgementsConfig, SourceConfig, SourceContext, SourceOutput,
    },
    serde::{bool_or_struct, default_decoding},
    tls::TlsConfig,
};

mod aws;
mod cursor;
mod lines;
mod manifest;
mod source;

#[cfg(all(test, feature = "crowdstrike-fdr-integration-tests"))]
mod integration_tests;
#[cfg(test)]
mod tests;

pub use manifest::ChecksumMode;

/// Named output that carries lookup snapshots (`fdrv2-*` objects) when `route_lookups` is on.
pub const LOOKUPS_OUTPUT: &str = "lookups";

/// Configuration for the `crowdstrike_fdr` source.
#[configurable_component(source(
    "crowdstrike_fdr",
    "Collect CrowdStrike Falcon Data Replicator (FDR) events from SQS batch manifests and S3."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "independent on/off options, as in the aws_s3 source"
)]
pub struct CrowdstrikeFdrConfig {
    #[serde(flatten)]
    region: RegionOrEndpoint,

    /// The URL of the FDR SQS queue that carries batch manifests.
    #[configurable(metadata(
        docs::examples = "https://sqs.us-west-2.amazonaws.com/123456789012/cs-mav-cannon-queue"
    ))]
    #[configurable(validation(format = "uri"))]
    queue_url: String,

    #[serde(default)]
    auth: AwsAuthentication,

    /// How long to long-poll the queue for messages, in seconds (0-20).
    #[serde(default = "default_poll_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    poll_secs: u32,

    /// Maximum number of messages to receive per poll (1-10).
    #[serde(default = "default_max_number_of_messages")]
    #[configurable(metadata(docs::human_name = "Max Messages"))]
    max_number_of_messages: u32,

    /// The visibility timeout to request when receiving, and to renew while a manifest is held.
    #[serde(default = "default_visibility_timeout_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[configurable(metadata(docs::human_name = "Visibility Timeout"))]
    visibility_timeout_secs: u32,

    /// How often to extend a held message's visibility, in seconds.
    ///
    /// Defaults to half of `visibility_timeout_secs`, and may not exceed that half.
    #[configurable(metadata(docs::type_unit = "seconds"))]
    heartbeat_interval_secs: Option<u32>,

    /// The longest a manifest is held, in seconds.
    ///
    /// After this, visibility is no longer extended and the source stops waiting for the
    /// manifest's acknowledgements, so the message re-delivers. At most 43200 (12 hours), the
    /// longest SQS lets a received message stay invisible.
    #[serde(default = "default_max_hold_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    max_hold_secs: u64,

    /// Number of objects downloaded at once, across all manifests.
    ///
    /// Each download is buffered in memory while it is verified (FDR objects are about 30-35 MB).
    #[serde(default = "default_file_concurrency")]
    file_concurrency: usize,

    /// Maximum number of manifests held (received but not yet deleted or released) at once.
    ///
    /// The source stops polling while this many are in flight.
    #[serde(default = "default_max_inflight_manifests")]
    max_inflight_manifests: usize,

    /// Maximum total compressed size (per the manifests) of held manifests, in bytes.
    ///
    /// A manifest larger than the whole budget is still processed, alone. Unlimited when unset.
    #[configurable(metadata(docs::type_unit = "bytes"))]
    max_inflight_bytes: Option<u64>,

    /// How many deliveries of a message to process before leaving it alone.
    ///
    /// A message received more than this many times (per `ApproximateReceiveCount`) is not
    /// processed, deleted or released; configure a redrive policy (dead-letter queue) on the
    /// queue to move it aside.
    #[serde(default = "default_max_tries")]
    max_tries: u32,

    /// After a failure, how long the message stays invisible before it re-delivers, in seconds.
    #[serde(default = "default_retry_delay_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    retry_delay_secs: u32,

    /// How to verify the per-object checksum in the manifest (32 hex digits = MD5, 64 = SHA-256).
    #[serde(default)]
    checksum: ChecksumMode,

    /// Which manifests to process, by batch time.
    ///
    /// - `beginning`: every manifest on the queue.
    /// - `now`: only batches newer than the time the source started.
    /// - `cursor`: only batches newer than the newest batch this source fully delivered before
    ///   (persisted in `data_dir`), minus `cursor_grace_secs`. Every manifest is processed on
    ///   the first run.
    /// - An RFC 3339 timestamp: only batches newer than it.
    ///
    /// Older manifests are deleted without downloading and counted as skipped.
    #[serde(default = "default_start_from")]
    #[configurable(metadata(docs::examples = "beginning"))]
    #[configurable(metadata(docs::examples = "cursor"))]
    #[configurable(metadata(docs::examples = "2026-11-01T00:00:00Z"))]
    start_from: String,

    /// With `start_from = cursor`, how far before the stored cursor still counts as new, in
    /// seconds. Covers batches that arrive out of order or were released before a restart.
    #[serde(default = "default_cursor_grace_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    cursor_grace_secs: u64,

    /// The directory for the `start_from = cursor` state.
    ///
    /// Defaults to the global `data_dir` option.
    #[configurable(metadata(docs::examples = "/var/lib/vector"))]
    data_dir: Option<PathBuf>,

    /// Send lookup snapshots (`fdrv2-*` objects such as aidmaster and managedassets) to the
    /// `lookups` output instead of the default output.
    #[serde(default = "default_true")]
    route_lookups: bool,

    /// Lines longer than this many bytes are dropped (and counted as errors).
    #[serde(default = "default_max_line_bytes")]
    #[configurable(metadata(docs::type_unit = "bytes"))]
    max_line_bytes: usize,

    /// An object larger than this many (compressed) bytes fails instead of being buffered.
    ///
    /// Objects are held in memory while they are verified, so this bounds memory use to about
    /// `file_concurrency` times this value.
    #[serde(default = "default_max_object_bytes")]
    #[configurable(metadata(docs::type_unit = "bytes"))]
    max_object_bytes: usize,

    /// Whether to delete a manifest's message once it is delivered.
    ///
    /// Set to `false` only for debugging: every message re-delivers.
    #[serde(default = "default_true")]
    delete_message: bool,

    /// Whether to delete a message whose events a sink rejected permanently.
    ///
    /// When `false` (the default), the message re-delivers until `max_tries`.
    #[serde(default)]
    delete_failed_message: bool,

    /// Timeout for establishing a connection to SQS or S3, in seconds.
    #[serde(default = "default_connect_timeout_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    connect_timeout_secs: u64,

    /// Timeout for one SQS or S3 operation, in seconds.
    ///
    /// For a GET this covers the response headers; the body is bounded by
    /// `stream_idle_timeout_secs`. Must exceed `poll_secs`.
    #[serde(default = "default_operation_timeout_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    operation_timeout_secs: u64,

    /// Timeout for the first byte of a response, in seconds. Must exceed `poll_secs`.
    #[serde(default = "default_read_timeout_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    read_timeout_secs: u64,

    /// A GET whose body sends no data for this many seconds fails (and its manifest is released).
    #[serde(default = "default_stream_idle_timeout_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    stream_idle_timeout_secs: u64,

    /// How often to read the queue depth with `GetQueueAttributes`, in seconds; `0` disables it.
    ///
    /// If the credentials may not call `GetQueueAttributes`, the gauge is disabled at the first
    /// denial.
    #[serde(default = "default_queue_depth_interval_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    queue_depth_interval_secs: u64,

    /// Whether to address S3 path-style (`https://s3.<region>.amazonaws.com/<bucket>/<key>`).
    ///
    /// Off by default: FDR buckets are read through S3 access-point aliases, which AWS serves
    /// virtual-hosted (the SDK default). Turn it on for S3-compatible endpoints such as LocalStack.
    #[serde(default)]
    force_path_style: bool,

    tls: Option<TlsConfig>,

    #[serde(default = "default_decoding")]
    decoding: DeserializerConfig,

    #[serde(default, deserialize_with = "bool_or_struct")]
    acknowledgements: SourceAcknowledgementsConfig,

    /// The namespace to use for logs. This overrides the global setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    log_namespace: Option<bool>,
}

/// SQS keeps a received message invisible for at most 12 hours in total, however often its
/// visibility is extended.
const SQS_MAX_VISIBILITY_SECS: u64 = 43_200;

const fn default_poll_secs() -> u32 {
    20
}
const fn default_max_number_of_messages() -> u32 {
    10
}
const fn default_visibility_timeout_secs() -> u32 {
    300
}
const fn default_max_hold_secs() -> u64 {
    36_000
}
const fn default_file_concurrency() -> usize {
    4
}
const fn default_max_inflight_manifests() -> usize {
    64
}
const fn default_max_tries() -> u32 {
    3
}
const fn default_retry_delay_secs() -> u32 {
    30
}
fn default_start_from() -> String {
    "beginning".to_owned()
}
const fn default_cursor_grace_secs() -> u64 {
    3600
}
const fn default_true() -> bool {
    true
}
const fn default_max_line_bytes() -> usize {
    8 * 1024 * 1024
}
const fn default_max_object_bytes() -> usize {
    512 * 1024 * 1024
}
const fn default_connect_timeout_secs() -> u64 {
    20
}
const fn default_operation_timeout_secs() -> u64 {
    90
}
const fn default_read_timeout_secs() -> u64 {
    60
}
const fn default_stream_idle_timeout_secs() -> u64 {
    120
}
const fn default_queue_depth_interval_secs() -> u64 {
    60
}

impl GenerateConfig for CrowdstrikeFdrConfig {
    fn generate_config() -> serde_json::Value {
        serde_json::json!({
            "region": "us-west-2",
            "queue_url": "https://sqs.us-west-2.amazonaws.com/123456789012/cs-mav-cannon-queue",
        })
    }
}

/// Where `start_from` puts the cutoff.
#[derive(Clone, Debug, PartialEq, Eq)]
enum StartFrom {
    Beginning,
    Now,
    Cursor,
    Timestamp(DateTime<Utc>),
}

#[derive(Debug, Snafu)]
enum ConfigError {
    #[snafu(display(
        "`start_from` must be `beginning`, `now`, `cursor` or an RFC 3339 timestamp, not {value:?}"
    ))]
    StartFrom { value: String },
    #[snafu(display("`max_number_of_messages` must be between 1 and 10, not {value}"))]
    MaxMessages { value: u32 },
    #[snafu(display("`poll_secs` must be between 0 and 20, not {value}"))]
    PollSecs { value: u32 },
    #[snafu(display("`visibility_timeout_secs` must be between 1 and 43200, not {value}"))]
    Visibility { value: u32 },
    #[snafu(display(
        "`heartbeat_interval_secs` ({value}) must be between 1 and half of `visibility_timeout_secs` ({visibility})"
    ))]
    Heartbeat { value: u32, visibility: u32 },
    #[snafu(display(
        "`operation_timeout_secs` and `read_timeout_secs` must exceed `poll_secs` ({poll})"
    ))]
    Timeouts { poll: u32 },
    #[snafu(display("`{field}` must be at least 1"))]
    Zero { field: &'static str },
    #[snafu(display(
        "`max_hold_secs` must not exceed 43200 (SQS caps a message's total visibility at 12 hours), not {value}"
    ))]
    MaxHold { value: u64 },
}

/// Without end-to-end acknowledgements a message is deleted once its events enter the pipeline,
/// so a sink that later fails loses them. Say so at startup.
fn warn_unless_acknowledged(acknowledgements: bool) {
    if !acknowledgements {
        warn!(
            message = "End-to-end acknowledgements are off: FDR messages are deleted once their events enter the pipeline, not once sinks deliver them. Enable `acknowledgements` to hold each message until delivery."
        );
    }
}

fn parse_start_from(value: &str) -> Result<StartFrom, ConfigError> {
    match value.trim() {
        "beginning" => Ok(StartFrom::Beginning),
        "now" => Ok(StartFrom::Now),
        "cursor" => Ok(StartFrom::Cursor),
        other => DateTime::parse_from_rfc3339(other)
            .map(|time| StartFrom::Timestamp(time.with_timezone(&Utc)))
            .map_err(|_| ConfigError::StartFrom {
                value: value.to_owned(),
            }),
    }
}

impl CrowdstrikeFdrConfig {
    /// Checks the numeric settings and returns the heartbeat interval.
    fn validate(&self) -> Result<(StartFrom, u32), ConfigError> {
        let start_from = parse_start_from(&self.start_from)?;
        if !(1..=10).contains(&self.max_number_of_messages) {
            return Err(ConfigError::MaxMessages {
                value: self.max_number_of_messages,
            });
        }
        if self.poll_secs > 20 {
            return Err(ConfigError::PollSecs {
                value: self.poll_secs,
            });
        }
        if !(1..=43_200).contains(&self.visibility_timeout_secs) {
            return Err(ConfigError::Visibility {
                value: self.visibility_timeout_secs,
            });
        }
        let half = (self.visibility_timeout_secs / 2).max(1);
        let heartbeat = self.heartbeat_interval_secs.unwrap_or(half);
        if heartbeat == 0 || heartbeat > half {
            return Err(ConfigError::Heartbeat {
                value: heartbeat,
                visibility: self.visibility_timeout_secs,
            });
        }
        let poll = u64::from(self.poll_secs);
        if self.operation_timeout_secs <= poll || self.read_timeout_secs <= poll {
            return Err(ConfigError::Timeouts {
                poll: self.poll_secs,
            });
        }
        for (field, value) in [
            ("file_concurrency", self.file_concurrency),
            ("max_inflight_manifests", self.max_inflight_manifests),
            ("max_line_bytes", self.max_line_bytes),
            ("max_object_bytes", self.max_object_bytes),
        ] {
            if value == 0 {
                return Err(ConfigError::Zero { field });
            }
        }
        if self.max_hold_secs == 0 {
            return Err(ConfigError::Zero {
                field: "max_hold_secs",
            });
        }
        if self.max_hold_secs > SQS_MAX_VISIBILITY_SECS {
            return Err(ConfigError::MaxHold {
                value: self.max_hold_secs,
            });
        }
        if self.stream_idle_timeout_secs == 0 {
            return Err(ConfigError::Zero {
                field: "stream_idle_timeout_secs",
            });
        }
        Ok((start_from, heartbeat))
    }

    const fn aws_timeout(&self) -> AwsTimeout {
        AwsTimeout::new(
            Some(self.connect_timeout_secs),
            Some(self.operation_timeout_secs),
            Some(self.read_timeout_secs),
        )
    }

    fn schema_definition(&self, log_namespace: LogNamespace) -> vector_lib::schema::Definition {
        let field = |name: &'static str| owned_value_path!("fdr", name);
        self.decoding
            .schema_definition(log_namespace)
            .with_standard_vector_source_metadata()
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::InsertIfEmpty(field("file"))),
                &owned_value_path!("file"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::InsertIfEmpty(field("line"))),
                &owned_value_path!("line"),
                Kind::integer(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::InsertIfEmpty(field("cid"))),
                &owned_value_path!("cid"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::InsertIfEmpty(field("bucket"))),
                &owned_value_path!("bucket"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::InsertIfEmpty(field("kind"))),
                &owned_value_path!("kind"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::InsertIfEmpty(field("batch_timestamp"))),
                &owned_value_path!("batch_timestamp"),
                Kind::timestamp().or_undefined(),
                None,
            )
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "crowdstrike_fdr")]
impl SourceConfig for CrowdstrikeFdrConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let (start_from, heartbeat_secs) = self.validate()?;
        let log_namespace = cx.log_namespace(self.log_namespace);
        let acknowledgements = cx.do_acknowledgements(self.acknowledgements);
        warn_unless_acknowledged(acknowledgements);

        let cursor = match start_from {
            StartFrom::Cursor => {
                let dir = cx
                    .globals
                    .resolve_and_make_data_subdir(self.data_dir.as_ref(), cx.key.id())?;
                Some(Arc::new(cursor::Cursor::open(&dir)?))
            }
            _ => None,
        };
        let cutoff = match &start_from {
            StartFrom::Beginning => None,
            StartFrom::Now => Some(Utc::now()),
            StartFrom::Timestamp(time) => Some(*time),
            StartFrom::Cursor => match &cursor {
                Some(cursor) => cursor.get().await.and_then(|ms| {
                    let grace = i64::try_from(self.cursor_grace_secs.saturating_mul(1000))
                        .unwrap_or(i64::MAX);
                    DateTime::from_timestamp_millis(ms.saturating_sub(grace))
                }),
                None => None,
            },
        };
        if let Some(cutoff) = cutoff {
            info!(message = "Skipping FDR batches older than the start_from cutoff.", %cutoff);
        }

        let region = self.region.region();
        let endpoint = self.region.endpoint();
        let timeout = self.aws_timeout();
        let sqs_client = create_client::<SqsClientBuilder>(
            &SqsClientBuilder {},
            &self.auth,
            region.clone(),
            endpoint.clone(),
            &cx.proxy,
            self.tls.as_ref(),
            Some(&timeout),
        )
        .await?;
        let s3_client = create_client::<S3ClientBuilder>(
            &S3ClientBuilder {
                force_path_style: Some(self.force_path_style),
            },
            &self.auth,
            region,
            endpoint,
            &cx.proxy,
            self.tls.as_ref(),
            Some(&timeout),
        )
        .await?;

        let decoder =
            DecodingConfig::new(FramingConfig::Bytes, self.decoding.clone(), log_namespace)
                .build()?;

        #[allow(clippy::cast_possible_wrap, reason = "validated ranges fit i32")]
        let settings = source::Settings {
            max_number_of_messages: self.max_number_of_messages as i32,
            visibility_timeout_secs: self.visibility_timeout_secs as i32,
            heartbeat_interval: Duration::from_secs(u64::from(heartbeat_secs)),
            max_hold: Duration::from_secs(self.max_hold_secs),
            file_concurrency: self.file_concurrency,
            max_inflight_manifests: self.max_inflight_manifests,
            max_inflight_kib: self.max_inflight_bytes.map(|bytes| {
                u32::try_from(bytes.div_ceil(1024))
                    .unwrap_or(u32::MAX)
                    .max(1)
            }),
            max_tries: self.max_tries,
            retry_delay_secs: i32::try_from(self.retry_delay_secs.min(43_200)).unwrap_or(30),
            checksum: self.checksum,
            cutoff,
            route_lookups: self.route_lookups,
            max_line_bytes: self.max_line_bytes,
            max_object_bytes: self.max_object_bytes,
            delete_message: self.delete_message,
            delete_failed_message: self.delete_failed_message,
            call_timeout: Duration::from_secs(self.operation_timeout_secs + 5),
            stream_idle_timeout: Duration::from_secs(self.stream_idle_timeout_secs),
            queue_depth_interval: (self.queue_depth_interval_secs > 0)
                .then(|| Duration::from_secs(self.queue_depth_interval_secs)),
            acknowledgements,
            log_namespace,
        };

        let engine = source::Engine::new(
            Arc::new(aws::SqsQueue {
                client: sqs_client,
                queue_url: self.queue_url.clone(),
                wait_time_secs: i32::try_from(self.poll_secs).unwrap_or(20),
            }),
            Arc::new(aws::S3Store { client: s3_client }),
            settings,
            decoder,
            cursor,
        );
        Ok(crate::sources::opaque_source(
            engine.run(cx.out, cx.shutdown),
        ))
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);
        let schema = self.schema_definition(log_namespace);
        let mut outputs = vec![SourceOutput::new_maybe_logs(
            self.decoding.output_type(),
            schema.clone(),
        )];
        if self.route_lookups {
            outputs.push(
                SourceOutput::new_maybe_logs(self.decoding.output_type(), schema)
                    .with_port(LOOKUPS_OUTPUT),
            );
        }
        outputs
    }

    fn can_acknowledge(&self) -> bool {
        true
    }
}
