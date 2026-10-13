//! `azure_event_hubs` source.
//!
//! Consumes events from every partition of an [Azure Event Hub][event_hubs] over AMQP 1.0, using
//! Microsoft's `azure_messaging_eventhubs` SDK.
//!
//! Delivery is at-least-once: a partition's checkpoint only advances past an event once every log
//! decoded from it has been acknowledged end to end (when acknowledgements are enabled), and it is
//! persisted in a [`checkpoint::CheckpointStore`]. On restart, each partition resumes right after
//! its checkpoint.
//!
//! With `start_position = "latest"` and a checkpoint store, a partition without a checkpoint gets
//! one before it is read: the position of its last enqueued event at that moment, written
//! create-only. A restart before the partition's first event then resumes after that position
//! instead of at `latest` again, which would skip what was enqueued while the source was down.
//!
//! [event_hubs]: https://learn.microsoft.com/azure/event-hubs/event-hubs-about

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use azure_core::credentials::TokenCredential;
use azure_messaging_eventhubs::{ConsumerClient, StartLocation, StartPosition};
use futures::{FutureExt, future::join_all};
use serde_with::serde_as;
use vector_lib::{
    codecs::{
        DecodingConfig,
        decoding::{DeserializerConfig, FramingConfig},
    },
    config::{LegacyKey, LogNamespace, SourceAcknowledgementsConfig, SourceOutput},
    configurable::configurable_component,
    sensitive_string::SensitiveString,
};
use vrl::{owned_value_path, value::Kind};

use crate::{
    config::{SourceConfig, SourceContext},
    internal_events::{AzureEventHubsStarted, AzureEventHubsStartupError},
    serde::{bool_or_struct, default_decoding, default_framing_message_based},
    shutdown::ShutdownSignal,
    sinks::azure_common::config::AzureAuthentication,
};

mod checkpoint;
mod sas;
mod source;

#[cfg(all(test, feature = "azure-event-hubs-integration-tests"))]
mod integration_tests;

pub use checkpoint::CheckpointStoreConfig;
pub(crate) use checkpoint::InitialCheckpoint;
use checkpoint::{Checkpointer, Position};
use sas::{EventHubsConnectionString, SasTokenCredential};
use source::PartitionReader;

/// How long one startup step (reading the checkpoints, connecting, listing the partitions) may
/// take before the attempt is retried.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

/// Where to start reading a partition that has no checkpoint.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StartPositionConfig {
    /// Start with the oldest event retained in the partition.
    Earliest,
    /// Start with events enqueued after the source connects.
    #[default]
    Latest,
}

impl StartPositionConfig {
    fn start_position(self) -> StartPosition {
        StartPosition {
            location: match self {
                Self::Earliest => StartLocation::Earliest,
                Self::Latest => StartLocation::Latest,
            },
            inclusive: false,
        }
    }
}

/// Configuration for the `azure_event_hubs` source.
#[serde_as]
#[configurable_component(source("azure_event_hubs", "Collect events from Azure Event Hubs."))]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub struct AzureEventHubsSourceConfig {
    /// An Event Hubs connection string with Shared Access Signature (SAS) credentials.
    ///
    /// The connection string must carry either `SharedAccessKeyName` and `SharedAccessKey`, in
    /// which case short-lived SAS tokens are signed and renewed locally, or a pre-formed
    /// `SharedAccessSignature`, which is used as-is until it expires. If it contains an
    /// `EntityPath`, that is the Event Hub to consume.
    ///
    /// Mutually exclusive with `namespace` and `auth`.
    #[configurable(metadata(
        docs::examples = "Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey=...;EntityPath=my-event-hub"
    ))]
    #[configurable(metadata(docs::examples = "${EVENTHUBS_CONNECTION_STRING}"))]
    #[serde(default)]
    pub connection_string: Option<SensitiveString>,

    /// The fully qualified Event Hubs namespace, for authentication with `auth`.
    #[configurable(metadata(docs::examples = "mynamespace.servicebus.windows.net"))]
    #[serde(default)]
    pub namespace: Option<String>,

    /// Microsoft Entra ID credentials, for authentication against `namespace`.
    ///
    /// The identity needs the *Azure Event Hubs Data Receiver* role on the Event Hub.
    #[serde(default)]
    pub auth: Option<AzureAuthentication>,

    /// The name of the Event Hub to consume. Required unless the connection string has an
    /// `EntityPath`.
    #[configurable(metadata(docs::examples = "my-event-hub"))]
    #[serde(default)]
    pub event_hub_name: Option<String>,

    /// The consumer group to read with.
    #[configurable(metadata(docs::examples = "vector"))]
    #[serde(default = "default_consumer_group")]
    #[derivative(Default(value = "default_consumer_group()"))]
    pub consumer_group: String,

    /// The IDs of the partitions to consume. By default, all partitions are consumed.
    #[configurable(metadata(docs::examples = "partition_ids_example()"))]
    #[serde(default)]
    pub partition_ids: Vec<String>,

    /// Where to start reading a partition that has no checkpoint.
    ///
    /// With `latest` and a `checkpoint_store`, the partition's current end is written as its
    /// first checkpoint before reading starts, so a restart never skips events enqueued while the
    /// source was down, even if the partition received none before.
    #[serde(default)]
    pub start_position: StartPositionConfig,

    /// Where checkpoints are stored.
    ///
    /// Without a checkpoint store, each start begins at `start_position`.
    #[serde(default)]
    pub checkpoint_store: Option<CheckpointStoreConfig>,

    /// How often acknowledged positions are written to the checkpoint store.
    ///
    /// Positions are also written on shutdown. A shorter interval reduces the number of events
    /// redelivered after a crash, at the cost of more checkpoint writes.
    #[serde(default = "default_checkpoint_interval")]
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    #[configurable(metadata(docs::human_name = "Checkpoint Interval"))]
    #[derivative(Default(value = "default_checkpoint_interval()"))]
    pub checkpoint_interval_secs: Duration,

    /// The number of events each partition receiver prefetches.
    #[configurable(metadata(docs::advanced))]
    #[serde(default)]
    pub prefetch: Option<u32>,

    /// The owner level (epoch) of the partition receivers.
    ///
    /// When set, the service disconnects the other receivers on the same partition and consumer
    /// group that have no owner level or a lower or equal one, which keeps a single active reader
    /// per partition.
    #[configurable(metadata(docs::advanced))]
    #[serde(default)]
    pub owner_level: Option<i64>,

    /// A custom AMQP endpoint to connect to instead of the namespace, for example a proxy or the
    /// Event Hubs emulator (`amqp://localhost:5672`).
    #[configurable(metadata(docs::advanced))]
    #[configurable(metadata(docs::examples = "amqp://localhost:5672"))]
    #[serde(default)]
    pub custom_endpoint: Option<String>,

    #[serde(default = "default_framing_message_based")]
    #[derivative(Default(value = "default_framing_message_based()"))]
    pub framing: FramingConfig,

    #[serde(default = "default_decoding")]
    #[derivative(Default(value = "default_decoding()"))]
    pub decoding: DeserializerConfig,

    #[serde(default, deserialize_with = "bool_or_struct")]
    pub acknowledgements: SourceAcknowledgementsConfig,

    /// The namespace to use for logs. This overrides the global setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    pub log_namespace: Option<bool>,
}

/// Formats an error with its chain of sources. The Azure SDKs wrap the cause that matters (an
/// Entra `AADSTS` code, an AMQP `401`) in generic outer errors.
fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let cause_message = cause.to_string();
        if !message.contains(&cause_message) {
            message.push_str(": ");
            message.push_str(&cause_message);
        }
        source = cause.source();
    }
    message
}

fn default_consumer_group() -> String {
    "$Default".to_string()
}

const fn default_checkpoint_interval() -> Duration {
    Duration::from_secs(5)
}

fn partition_ids_example() -> Vec<String> {
    vec!["0".into(), "1".into()]
}

impl_generate_config_from_default!(AzureEventHubsSourceConfig);

/// What the source needs from the service before its partitions can be read: the checkpoints,
/// a connection, and the partition list.
struct Startup {
    connection: Connection,
    consumer_group: String,
    /// The configured partitions; empty for all of them.
    partition_ids: Vec<String>,
    checkpointer: Option<Checkpointer>,
}

/// A successful [`Startup`].
struct Started {
    client: ConsumerClient,
    partition_ids: Vec<String>,
    positions: HashMap<String, Position>,
}

impl Startup {
    /// Starts, retrying with backoff until it succeeds. Returns `None` on shutdown.
    async fn run(&self, shutdown: &mut ShutdownSignal) -> Option<Started> {
        retry_startup(|| self.attempt(), shutdown).await
    }

    /// One startup attempt. Reads the checkpoints before anything else, so that no partition is
    /// read without knowing where it stopped.
    async fn attempt(&self) -> Result<Started, StartupError> {
        let positions = match &self.checkpointer {
            Some(checkpointer) => tokio::time::timeout(STARTUP_TIMEOUT, checkpointer.load())
                .await
                .map_err(|_| StartupError::timed_out(StartupStage::ReadCheckpoints))?
                .map_err(|e| StartupError::new(StartupStage::ReadCheckpoints, &e))?,
            None => Default::default(),
        };

        let mut builder = ConsumerClient::builder()
            .with_consumer_group(self.consumer_group.clone())
            .with_application_id(format!("vector/{}", crate::get_version()));
        if let Some(endpoint) = &self.connection.custom_endpoint {
            builder = builder.with_custom_endpoint(endpoint.clone());
        }
        let client = tokio::time::timeout(
            STARTUP_TIMEOUT,
            builder.open(
                &self.connection.namespace,
                self.connection.event_hub_name.clone(),
                Arc::clone(&self.connection.credential),
            ),
        )
        .await
        .map_err(|_| StartupError::timed_out(StartupStage::Connect))?
        .map_err(|e| StartupError::new(StartupStage::Connect, &e))?;

        // Fetching the partition list also verifies the credentials.
        let partition_ids =
            match tokio::time::timeout(STARTUP_TIMEOUT, client.get_eventhub_properties()).await {
                Ok(Ok(properties)) => self.resolve_partitions(properties.partition_ids),
                Ok(Err(error)) => Err(StartupError::new(StartupStage::ReadProperties, &error)),
                Err(_) => Err(StartupError::timed_out(StartupStage::ReadProperties)),
            };
        match partition_ids {
            Ok(partition_ids) => Ok(Started {
                client,
                partition_ids,
                positions,
            }),
            Err(error) => {
                if let Err(close_error) = client.close().await {
                    debug!(message = "Error closing the Event Hubs client.", error = %close_error);
                }
                Err(error)
            }
        }
    }

    /// The partitions to read: the configured ones, which must all exist, or all of them.
    fn resolve_partitions(&self, existing: Vec<String>) -> Result<Vec<String>, StartupError> {
        if self.partition_ids.is_empty() {
            return Ok(existing);
        }
        match self.partition_ids.iter().find(|id| !existing.contains(id)) {
            Some(unknown) => Err(StartupError {
                stage: StartupStage::ReadProperties,
                message: format!(
                    "partition `{unknown}` does not exist in Event Hub `{}`",
                    self.connection.event_hub_name
                ),
            }),
            None => Ok(self.partition_ids.clone()),
        }
    }
}

/// A startup step that can fail until the service accepts the source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StartupStage {
    ReadCheckpoints,
    Connect,
    ReadProperties,
}

impl StartupStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ReadCheckpoints => "reading checkpoints from the checkpoint store",
            Self::Connect => "connecting to Event Hubs",
            Self::ReadProperties => {
                "reading the Event Hub's partitions (check the credentials and that the identity \
                 may listen on it)"
            }
        }
    }
}

/// Why one startup attempt failed.
#[derive(Debug)]
struct StartupError {
    stage: StartupStage,
    message: String,
}

impl StartupError {
    fn new(stage: StartupStage, error: &(dyn std::error::Error + 'static)) -> Self {
        Self {
            stage,
            message: error_chain(error),
        }
    }

    fn timed_out(stage: StartupStage) -> Self {
        Self {
            stage,
            message: format!("timed out after {} s", STARTUP_TIMEOUT.as_secs()),
        }
    }
}

/// Runs `attempt` until it succeeds, reporting each failure and backing off between attempts
/// (as a partition does after a receive failure). Returns `None` on shutdown.
async fn retry_startup<T, F, Fut>(mut attempt: F, shutdown: &mut ShutdownSignal) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StartupError>>,
{
    let mut backoff = source::BACKOFF_INITIAL;
    let mut failures = 0_usize;
    loop {
        let result = tokio::select! {
            biased;
            _ = &mut *shutdown => return None,
            result = attempt() => result,
        };
        match result {
            Ok(started) => {
                if failures > 0 {
                    emit!(AzureEventHubsStarted { failures });
                }
                return Some(started);
            }
            Err(error) => {
                failures += 1;
                emit!(AzureEventHubsStartupError {
                    stage: error.stage.as_str(),
                    error: &error.message,
                    retry_in: backoff,
                });
                if !source::sleep_or_shutdown(&mut backoff, shutdown).await {
                    return None;
                }
            }
        }
    }
}

/// The resolved connection settings.
struct Connection {
    namespace: String,
    event_hub_name: String,
    credential: Arc<dyn TokenCredential>,
    custom_endpoint: Option<String>,
}

impl AzureEventHubsSourceConfig {
    fn connection(&self) -> crate::Result<Connection> {
        let (namespace, event_hub_name, credential, emulator_endpoint): (
            String,
            String,
            Arc<dyn TokenCredential>,
            Option<String>,
        ) = match (&self.connection_string, &self.namespace, &self.auth) {
            (Some(connection_string), None, None) => {
                let parsed = EventHubsConnectionString::parse(connection_string.inner())
                    .map_err(|e| format!("invalid `connection_string`: {e}"))?;
                let event_hub_name = match (&parsed.entity_path, &self.event_hub_name) {
                    (Some(entity), Some(name)) if !entity.eq_ignore_ascii_case(name) => {
                        return Err(format!(
                            "`event_hub_name` ({name}) does not match the connection string's \
                             `EntityPath` ({entity})"
                        )
                        .into());
                    }
                    (Some(entity), _) => entity.clone(),
                    (None, Some(name)) => name.clone(),
                    (None, None) => {
                        return Err("`event_hub_name` is required when the connection string \
                                    has no `EntityPath`"
                            .into());
                    }
                };
                let namespace = parsed.namespace_host().to_string();
                let credential = Arc::new(SasTokenCredential::new(
                    &namespace,
                    &event_hub_name,
                    parsed.credentials.clone(),
                ));
                (
                    namespace,
                    event_hub_name,
                    credential,
                    parsed.emulator_endpoint(),
                )
            }
            (None, Some(namespace), Some(auth)) => {
                let event_hub_name = self
                    .event_hub_name
                    .clone()
                    .ok_or("`event_hub_name` is required when using `namespace`")?;
                let namespace = namespace
                    .trim()
                    .trim_start_matches("sb://")
                    .trim_end_matches('/')
                    .to_ascii_lowercase();
                let credential = auth
                    .credential()
                    .map_err(|e| format!("failed to configure Azure authentication: {e}"))?;
                (namespace, event_hub_name, credential, None)
            }
            (Some(_), _, _) => {
                return Err(
                    "`connection_string` cannot be combined with `namespace` or `auth`".into(),
                );
            }
            (None, None, _) => {
                return Err(
                    "either `connection_string`, or `namespace` and `auth`, must be set".into(),
                );
            }
            (None, Some(_), None) => {
                return Err("`auth` is required when using `namespace`".into());
            }
        };

        if self.consumer_group.trim().is_empty() {
            return Err("`consumer_group` must not be empty".into());
        }

        Ok(Connection {
            namespace,
            event_hub_name,
            credential,
            custom_endpoint: self.custom_endpoint.clone().or(emulator_endpoint),
        })
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "azure_event_hubs")]
impl SourceConfig for AzureEventHubsSourceConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let log_namespace = cx.log_namespace(self.log_namespace);
        let acknowledgements = cx.do_acknowledgements(self.acknowledgements);
        let decoder =
            DecodingConfig::new(self.framing.clone(), self.decoding.clone(), log_namespace)
                .build()?;
        let connection = self.connection()?;

        if self.checkpoint_interval_secs.is_zero() {
            return Err("`checkpoint_interval_secs` must be greater than zero".into());
        }

        // Only the configuration is checked here. Everything that needs the service (reading the
        // checkpoints, connecting, listing the partitions) happens when the source runs, and is
        // retried until it succeeds: a refused credential, a missing permission or a missing
        // entity must not fail the configuration, which would stop every other component of
        // this Vector too. They are reported as errors meanwhile, and the source starts reading
        // once they are resolved, without a restart.
        let checkpointer = match &self.checkpoint_store {
            Some(store) => Some(Checkpointer::new(
                store.build(&cx.proxy)?,
                &connection.namespace,
                &connection.event_hub_name,
                &self.consumer_group,
            )),
            None => {
                warn!(
                    message =
                        "No `checkpoint_store` configured; every start begins at `start_position`."
                );
                None
            }
        };

        let startup = Startup {
            connection,
            consumer_group: self.consumer_group.clone(),
            partition_ids: self.partition_ids.clone(),
            checkpointer: checkpointer.clone(),
        };
        let start_position = self.start_position.clone();
        let prefetch = self.prefetch;
        let owner_level = self.owner_level;
        let checkpoint_interval = self.checkpoint_interval_secs;
        let out = cx.out;
        let mut shutdown = cx.shutdown;

        Ok(async move {
            let Some(Started {
                client,
                partition_ids,
                positions,
            }) = startup.run(&mut shutdown).await
            else {
                // Shut down before the source could start: nothing was read.
                return Ok(());
            };
            info!(
                message = "Consuming Event Hub.",
                namespace = %startup.connection.namespace,
                event_hub = %startup.connection.event_hub_name,
                consumer_group = %startup.consumer_group,
                partitions = ?partition_ids,
                checkpointed_partitions = positions.len(),
            );

            let client = Arc::new(client);
            let readers: Vec<_> = partition_ids
                .into_iter()
                .map(|partition_id| {
                    PartitionReader {
                        client: Arc::clone(&client),
                        initial_position: positions.get(&partition_id).cloned(),
                        partition_id,
                        event_hub_name: startup.connection.event_hub_name.clone(),
                        checkpointer: checkpointer.clone(),
                        default_start: start_position.start_position(),
                        // `earliest` needs no initial checkpoint: restarting at the earliest
                        // retained event re-reads, but never skips.
                        initial_checkpoint: checkpointer.is_some()
                            && start_position == StartPositionConfig::Latest,
                        prefetch,
                        owner_level,
                        checkpoint_interval,
                        decoder: decoder.clone(),
                        out: out.clone(),
                        acknowledgements,
                        log_namespace,
                        shutdown: shutdown.clone(),
                    }
                    .run()
                })
                .collect();

            let results = join_all(readers).await;
            match Arc::try_unwrap(client) {
                Ok(client) => {
                    if let Err(error) = client.close().await {
                        debug!(message = "Error closing the Event Hubs client.", %error);
                    }
                }
                Err(_) => debug!(message = "Event Hubs client still shared at shutdown."),
            }
            if results.iter().any(Result::is_err) {
                Err(())
            } else {
                Ok(())
            }
        }
        .boxed())
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);
        let schema_definition = self
            .decoding
            .schema_definition(log_namespace)
            .with_standard_vector_source_metadata()
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::InsertIfEmpty(owned_value_path!(
                    "event_hub_name"
                ))),
                &owned_value_path!("event_hub_name"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::InsertIfEmpty(owned_value_path!("partition_id"))),
                &owned_value_path!("partition_id"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::InsertIfEmpty(owned_value_path!("offset"))),
                &owned_value_path!("offset"),
                Kind::bytes().or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::InsertIfEmpty(owned_value_path!(
                    "sequence_number"
                ))),
                &owned_value_path!("sequence_number"),
                Kind::integer().or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::InsertIfEmpty(owned_value_path!("enqueued_time"))),
                &owned_value_path!("enqueued_time"),
                Kind::timestamp().or_undefined(),
                Some("timestamp"),
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::InsertIfEmpty(owned_value_path!("partition_key"))),
                &owned_value_path!("partition_key"),
                Kind::bytes().or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::InsertIfEmpty(owned_value_path!("properties"))),
                &owned_value_path!("properties"),
                Kind::object(vrl::value::kind::Collection::empty().with_unknown(Kind::any()))
                    .or_undefined(),
                None,
            );

        vec![SourceOutput::new_maybe_logs(
            self.decoding.output_type(),
            schema_definition,
        )]
    }

    fn can_acknowledge(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<AzureEventHubsSourceConfig>();
    }

    fn parse(toml: &str) -> AzureEventHubsSourceConfig {
        toml::from_str(toml).unwrap()
    }

    #[test]
    fn connection_string_config() {
        let config = parse(
            r#"
            connection_string = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=l;SharedAccessKey=k;EntityPath=hub"
            consumer_group = "vector"
            start_position = "earliest"
            checkpoint_interval_secs = 2.5
            "#,
        );
        assert_eq!(config.start_position, StartPositionConfig::Earliest);
        assert_eq!(config.checkpoint_interval_secs, Duration::from_millis(2500));
        let connection = config.connection().unwrap();
        assert_eq!(connection.namespace, "ns.servicebus.windows.net");
        assert_eq!(connection.event_hub_name, "hub");
        assert_eq!(connection.custom_endpoint, None);
    }

    #[test]
    fn defaults() {
        let config = parse(
            r#"connection_string = "Endpoint=sb://ns/;SharedAccessKeyName=l;SharedAccessKey=k"
               event_hub_name = "hub""#,
        );
        assert_eq!(config.consumer_group, "$Default");
        assert_eq!(config.start_position, StartPositionConfig::Latest);
        assert_eq!(config.checkpoint_interval_secs, Duration::from_secs(5));
        assert!(config.checkpoint_store.is_none());
    }

    #[test]
    fn entra_config() {
        let config = parse(
            r#"
            namespace = "sb://NS.servicebus.windows.net/"
            event_hub_name = "hub"
            auth.azure_credential_kind = "client_secret_credential"
            auth.azure_tenant_id = "00000000-0000-0000-0000-000000000000"
            auth.azure_client_id = "00000000-0000-0000-0000-000000000000"
            auth.azure_client_secret = "secret"

            [checkpoint_store]
            type = "azure_blob"
            account_name = "mystorage"
            container_name = "checkpoints"
            auth.azure_credential_kind = "workload_identity"
            auth.token_file_path = "/var/run/secrets/token"
            "#,
        );
        let connection = config.connection().unwrap();
        assert_eq!(connection.namespace, "ns.servicebus.windows.net");
        assert!(matches!(
            config.checkpoint_store,
            Some(CheckpointStoreConfig::AzureBlob { .. })
        ));
    }

    #[test]
    fn invalid_connection_settings() {
        let err = |toml: &str| parse(toml).connection().err().unwrap().to_string();
        assert!(err(r#"event_hub_name = "hub""#).contains("must be set"));
        assert!(
            err(r#"namespace = "ns"
                        event_hub_name = "hub""#)
            .contains("`auth` is required")
        );
        assert!(
            err(
                r#"connection_string = "Endpoint=sb://ns/;SharedAccessKeyName=l;SharedAccessKey=k""#
            )
            .contains("`event_hub_name` is required")
        );
        assert!(err(
            r#"connection_string = "Endpoint=sb://ns/;SharedAccessKeyName=l;SharedAccessKey=k;EntityPath=a"
               event_hub_name = "b""#
        )
        .contains("does not match"));
        assert!(err(
            r#"connection_string = "Endpoint=sb://ns/;SharedAccessKeyName=l;SharedAccessKey=k;EntityPath=a"
               consumer_group = " ""#
        )
        .contains("consumer_group"));
        assert!(err(r#"connection_string = "Endpoint=sb://ns/""#).contains("invalid"));
    }

    #[test]
    fn emulator_connection_string_sets_endpoint() {
        let config = parse(
            r#"connection_string = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;EntityPath=eh1""#,
        );
        let connection = config.connection().unwrap();
        assert_eq!(connection.namespace, "localhost");
        assert_eq!(
            connection.custom_endpoint.as_deref(),
            Some("amqp://localhost:5672")
        );
    }

    /// A refused credential (or any failure that needs the service) does not fail the
    /// configuration: the source builds, keeps retrying, and stops cleanly on shutdown.
    #[tokio::test]
    async fn build_does_not_contact_the_service() {
        // Nothing listens on this endpoint, and the storage account does not exist: building
        // still succeeds, quickly, because neither is contacted.
        let config = parse(
            r#"connection_string = "Endpoint=sb://localhost:1;SharedAccessKeyName=l;SharedAccessKey=k;EntityPath=hub;UseDevelopmentEmulator=true"
               checkpoint_store.type = "azure_blob"
               checkpoint_store.connection_string = "DefaultEndpointsProtocol=http;AccountName=a;AccountKey=a2V5;BlobEndpoint=http://127.0.0.1:1/a;"
               checkpoint_store.container_name = "checkpoints""#,
        );
        let (trigger, shutdown, done) = ShutdownSignal::new_wired();
        let (tx, _rx) = crate::SourceSender::new_test();
        let mut cx = SourceContext::new_test(tx, None);
        cx.shutdown = shutdown;
        let source = tokio::time::timeout(Duration::from_secs(5), config.build(cx))
            .await
            .expect("build contacted the service")
            .expect("build failed");
        let source = tokio::spawn(source);
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(trigger);
        done.await;
        assert_eq!(source.await.unwrap(), Ok(()));
    }

    /// Bad configuration still fails the build.
    #[tokio::test]
    async fn build_fails_on_bad_configuration() {
        let build = |toml: &str| {
            let config = parse(toml);
            async move {
                let (tx, _rx) = crate::SourceSender::new_test();
                config
                    .build(SourceContext::new_test(tx, None))
                    .await
                    .err()
                    .map(|e| e.to_string())
            }
        };
        let valid = r#"connection_string = "Endpoint=sb://ns/;SharedAccessKeyName=l;SharedAccessKey=k;EntityPath=hub""#;
        assert!(
            build(r#"connection_string = "Endpoint=sb://ns/""#)
                .await
                .unwrap()
                .contains("invalid")
        );
        assert!(
            build(&format!("{valid}\ncheckpoint_interval_secs = 0"))
                .await
                .unwrap()
                .contains("checkpoint_interval_secs")
        );
        assert!(
            build(&format!(
                "{valid}\ncheckpoint_store.type = \"azure_blob\"\ncheckpoint_store.container_name = \"c\""
            ))
            .await
            .is_some()
        );
    }

    fn refused(stage: StartupStage) -> StartupError {
        StartupError {
            stage,
            message: "AADSTS70021: No matching federated identity record found".into(),
        }
    }

    /// Startup failures are retried with backoff until the service accepts the source.
    #[tokio::test(start_paused = true)]
    async fn startup_retries_until_it_succeeds() {
        let (_trigger, mut shutdown, _done) = ShutdownSignal::new_wired();
        let mut attempts = 0;
        let started = tokio::time::Instant::now();
        let result = retry_startup(
            || {
                attempts += 1;
                let attempt = attempts;
                async move {
                    match attempt {
                        1 => Err(refused(StartupStage::ReadCheckpoints)),
                        2 | 3 => Err(refused(StartupStage::ReadProperties)),
                        _ => Ok(attempt),
                    }
                }
            },
            &mut shutdown,
        )
        .await;
        assert_eq!(result, Some(4));
        // Backed off 1 + 2 + 4 s between the four attempts.
        assert_eq!(started.elapsed(), Duration::from_secs(7));
    }

    /// Shutdown while startup keeps failing ends the source cleanly.
    #[tokio::test(start_paused = true)]
    async fn startup_stops_on_shutdown() {
        let (trigger, mut shutdown, _done) = ShutdownSignal::new_wired();
        let stop = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(100)).await;
            drop(trigger);
        });
        let result: Option<()> = retry_startup(
            || async { Err(refused(StartupStage::Connect)) },
            &mut shutdown,
        )
        .await;
        assert_eq!(result, None);
        stop.await.unwrap();
    }

    #[test]
    fn error_chain_includes_causes() {
        let inner = std::io::Error::other("AADSTS7000215: Invalid client secret provided.");
        let outer = azure_core::Error::with_error(
            azure_core::error::ErrorKind::Credential,
            inner,
            "non-transport error",
        );
        let message = error_chain(&outer);
        assert!(message.starts_with("non-transport error"), "{message}");
        assert!(message.contains("AADSTS7000215"), "{message}");
    }

    #[test]
    fn rejects_unknown_fields() {
        assert!(toml::from_str::<AzureEventHubsSourceConfig>(r#"bogus = 1"#).is_err());
    }
}
