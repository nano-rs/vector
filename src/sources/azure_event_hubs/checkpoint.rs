//! Checkpoint persistence for the `azure_event_hubs` source.
//!
//! Checkpoints go through the [`CheckpointStore`] trait of `azure_messaging_eventhubs`, so any
//! implementation of that trait can back the source. The bundled implementation stores them in
//! Azure Blob Storage using the same layout as the Azure SDKs' blob checkpoint stores
//! (`<namespace>/<event hub>/<consumer group>/checkpoint/<partition id>`), which keeps
//! checkpoints interchangeable with other Event Hubs processors.
//!
//! The SDK trait can only overwrite a checkpoint. [`InitialCheckpointStore`] adds the one
//! conditional write the source needs: creating a partition's first checkpoint only if it has
//! none (see [`Checkpointer::initialize`]).

use std::{collections::HashMap, sync::Arc};

use azure_core::{
    Bytes,
    http::{Etag, NoFormat, RequestContent, StatusCode, Url},
};
use azure_messaging_eventhubs::{
    CheckpointStore, StartLocation, StartPosition,
    models::{Checkpoint, EventHubPartitionProperties, Ownership},
};
use azure_messaging_eventhubs_checkpointstore_blob::BlobCheckpointStore;
use azure_storage_blob::{BlobContainerClient, models::BlockBlobClientUploadOptions};
use vector_lib::{configurable::configurable_component, sensitive_string::SensitiveString};

use crate::{
    config::ProxyConfig,
    sinks::{
        azure_blob::config::build_client,
        azure_common::{
            config::AzureAuthentication,
            connection_string::{Auth, ParsedConnectionString},
        },
    },
};

/// Checkpoint store configuration.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[configurable(metadata(docs::enum_tag_description = "The kind of checkpoint store to use."))]
pub enum CheckpointStoreConfig {
    /// Store checkpoints in an Azure Blob Storage container.
    ///
    /// The blob layout matches the blob checkpoint stores of the Azure SDKs, so the checkpoints
    /// are interchangeable with other Event Hubs consumers that use the same consumer group.
    AzureBlob {
        /// The Azure Blob Storage connection string.
        ///
        /// Either `connection_string`, `account_name`, or `blob_endpoint` must be set. A connection
        /// string carrying an account key or a SAS token cannot be combined with `auth`.
        #[configurable(metadata(
            docs::examples = "DefaultEndpointsProtocol=https;AccountName=mystorage;AccountKey=...;EndpointSuffix=core.windows.net"
        ))]
        #[serde(default)]
        connection_string: Option<SensitiveString>,

        /// The Azure Blob Storage account name. Requires `auth`.
        #[configurable(metadata(docs::examples = "mystorage"))]
        #[serde(default)]
        account_name: Option<String>,

        /// The Azure Blob Storage endpoint. Requires `auth`.
        #[configurable(metadata(docs::examples = "https://mystorage.blob.core.windows.net/"))]
        #[serde(default)]
        blob_endpoint: Option<String>,

        /// The name of the container that holds the checkpoints. It must already exist.
        #[configurable(metadata(docs::examples = "eventhubs-checkpoints"))]
        container_name: String,

        /// Azure credentials for the storage account, used with `account_name` or `blob_endpoint`.
        #[serde(default)]
        auth: Option<AzureAuthentication>,
    },
}

impl CheckpointStoreConfig {
    /// Builds the configured checkpoint store.
    pub fn build(&self, proxy: &ProxyConfig) -> crate::Result<Arc<dyn InitialCheckpointStore>> {
        match self {
            Self::AzureBlob {
                connection_string,
                account_name,
                blob_endpoint,
                container_name,
                auth,
            } => {
                let connection_string: String =
                    match (connection_string, account_name, blob_endpoint) {
                        (Some(connection_string), None, None) => connection_string.inner().into(),
                        (None, Some(account_name), None) => format!("AccountName={account_name}"),
                        (None, None, Some(blob_endpoint)) => {
                            format!("BlobEndpoint={}/", blob_endpoint.trim_end_matches('/'))
                        }
                        _ => {
                            return Err("`checkpoint_store`: exactly one of `connection_string`, \
                                        `account_name`, or `blob_endpoint` must be set"
                                .into());
                        }
                    };
                let parsed = ParsedConnectionString::parse(&connection_string).map_err(|e| {
                    format!("`checkpoint_store`: invalid storage connection string: {e}")
                })?;
                match (parsed.auth(), auth) {
                    (Auth::None, None) => {
                        return Err(
                            "`checkpoint_store`: no storage credentials; use a connection \
                                    string with an account key or SAS token, or set `auth`"
                                .into(),
                        );
                    }
                    (Auth::Sas { .. } | Auth::SharedKey { .. }, Some(_)) => {
                        return Err("`checkpoint_store`: a connection string with credentials \
                                    cannot be combined with `auth`"
                            .into());
                    }
                    _ => {}
                }
                let container_url = parsed
                    .container_url(container_name)
                    .map_err(|e| format!("`checkpoint_store`: invalid container URL: {e}"))?;
                let container_url = Url::parse(&container_url)
                    .map_err(|e| format!("`checkpoint_store`: invalid container URL: {e}"))?;
                // The SDK store takes ownership of its client and does not expose it, so the
                // conditional create (`AzureBlobCheckpointStore::create_checkpoint`) gets its own
                // client for the same container.
                let client = build_client(
                    auth.clone(),
                    parsed.clone(),
                    container_url.clone(),
                    proxy,
                    None,
                )?;
                let client = Arc::try_unwrap(client)
                    .map_err(|_| "`checkpoint_store`: storage client unexpectedly shared")?;
                let container = build_client(auth.clone(), parsed, container_url, proxy, None)?;
                Ok(Arc::new(AzureBlobCheckpointStore {
                    store: BlobCheckpointStore::new(client),
                    container,
                }))
            }
        }
    }
}

/// A [`CheckpointStore`] that can also write a partition's first checkpoint without ever
/// replacing an existing one.
#[async_trait::async_trait]
pub trait InitialCheckpointStore: CheckpointStore {
    /// Writes `checkpoint` only if its partition has no checkpoint yet, atomically with respect to
    /// other writers. Returns `true` if it was written and `false` if one already existed (which
    /// is left untouched).
    async fn create_checkpoint(&self, checkpoint: Checkpoint) -> azure_core::Result<bool>;
}

/// The blob checkpoint store of the Azure SDK, plus a create-only checkpoint write.
struct AzureBlobCheckpointStore {
    store: Arc<BlobCheckpointStore>,
    container: Arc<BlobContainerClient>,
}

/// Metadata keys of a checkpoint blob, as written by `BlobCheckpointStore` (and the other Azure
/// SDKs' blob checkpoint stores).
const BLOB_METADATA_OFFSET: &str = "offset";
const BLOB_METADATA_SEQUENCE_NUMBER: &str = "sequencenumber";

#[async_trait::async_trait]
impl CheckpointStore for AzureBlobCheckpointStore {
    async fn claim_ownership(
        &self,
        ownerships: &[Ownership],
    ) -> azure_core::Result<Vec<Ownership>> {
        self.store.claim_ownership(ownerships).await
    }

    async fn list_checkpoints(
        &self,
        namespace: &str,
        event_hub_name: &str,
        consumer_group: &str,
    ) -> azure_core::Result<Vec<Checkpoint>> {
        self.store
            .list_checkpoints(namespace, event_hub_name, consumer_group)
            .await
    }

    async fn list_ownerships(
        &self,
        namespace: &str,
        event_hub_name: &str,
        consumer_group: &str,
    ) -> azure_core::Result<Vec<Ownership>> {
        self.store
            .list_ownerships(namespace, event_hub_name, consumer_group)
            .await
    }

    async fn update_checkpoint(&self, checkpoint: Checkpoint) -> azure_core::Result<()> {
        self.store.update_checkpoint(checkpoint).await
    }
}

#[async_trait::async_trait]
impl InitialCheckpointStore for AzureBlobCheckpointStore {
    /// Uploads the checkpoint blob with `If-None-Match: *`: Blob Storage refuses the upload if the
    /// blob exists, so an existing checkpoint can never be replaced, whoever wrote it and when.
    async fn create_checkpoint(&self, checkpoint: Checkpoint) -> azure_core::Result<bool> {
        let blob_name = Checkpoint::get_checkpoint_blob_name(
            &checkpoint.fully_qualified_namespace,
            &checkpoint.event_hub_name,
            &checkpoint.consumer_group,
            &checkpoint.partition_id,
        )?;
        let mut metadata = HashMap::new();
        if let Some(sequence_number) = checkpoint.sequence_number {
            metadata.insert(
                BLOB_METADATA_SEQUENCE_NUMBER.to_string(),
                sequence_number.to_string(),
            );
        }
        if let Some(offset) = checkpoint.offset {
            metadata.insert(BLOB_METADATA_OFFSET.to_string(), offset);
        }
        let options = BlockBlobClientUploadOptions {
            metadata: Some(metadata),
            if_none_match: Some(Etag::from("*")),
            ..Default::default()
        };
        let result = self
            .container
            .blob_client(&blob_name)
            .upload(
                RequestContent::<Bytes, NoFormat>::from(Vec::new()),
                Some(options),
            )
            .await;
        match result {
            Ok(_) => Ok(true),
            // `409 BlobAlreadyExists` (or `412` from some emulators): a checkpoint is there.
            Err(error)
                if matches!(
                    error.http_status(),
                    Some(StatusCode::Conflict | StatusCode::PreconditionFailed)
                ) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }
}

/// A position in a partition: the identity of the last event that was processed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Position {
    pub offset: Option<String>,
    pub sequence_number: Option<i64>,
}

impl Position {
    /// Returns `None` when neither field is set: such a position names nothing, and writing it as
    /// a checkpoint would erase a valid one.
    pub fn new(offset: Option<String>, sequence_number: Option<i64>) -> Option<Self> {
        (offset.is_some() || sequence_number.is_some()).then_some(Self {
            offset,
            sequence_number,
        })
    }

    fn location(&self) -> StartLocation {
        // The offset is preferred, as the Azure SDK event processors do.
        match (&self.offset, self.sequence_number) {
            (Some(offset), _) => StartLocation::Offset(offset.clone()),
            (None, Some(sequence_number)) => StartLocation::SequenceNumber(sequence_number),
            (None, None) => unreachable!("a position always has an offset or a sequence number"),
        }
    }

    /// Start right after this position.
    pub fn after(&self) -> StartPosition {
        StartPosition {
            location: self.location(),
            inclusive: false,
        }
    }

    /// Start at this position, including it.
    pub fn at(&self) -> StartPosition {
        StartPosition {
            location: self.location(),
            inclusive: true,
        }
    }

    /// The position of the last event enqueued in a partition: what `latest` means at the time
    /// the properties were read. Starting after it reads exactly the events enqueued since.
    ///
    /// For a partition that never had an event the service reports offset `-1` and sequence
    /// number `-1`; starting after that is the start of the partition, which is correct.
    pub fn last_enqueued(properties: &EventHubPartitionProperties) -> Self {
        Self {
            offset: Some(properties.last_enqueued_offset.clone()).filter(|o| !o.is_empty()),
            sequence_number: Some(properties.last_enqueued_sequence_number),
        }
    }
}

/// The outcome of [`Checkpointer::initialize`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitialCheckpoint {
    /// The partition had no checkpoint; this position was written as its first one.
    Created(Position),
    /// A checkpoint appeared since the source loaded them (another instance wrote it); it was
    /// kept and the partition resumes from it.
    Existing(Position),
    /// The store holds a checkpoint record for the partition that names no position (for
    /// example an empty blob written by another tool). It was kept; the partition starts after
    /// the resolved position, and the first checkpoint committed for an event replaces the
    /// record.
    Unreadable(Position),
}

impl InitialCheckpoint {
    /// The position the partition resumes after.
    pub fn position(&self) -> &Position {
        match self {
            Self::Created(position) | Self::Existing(position) | Self::Unreadable(position) => {
                position
            }
        }
    }
}

/// Reads and writes the checkpoints of one Event Hub and consumer group.
#[derive(Clone)]
pub struct Checkpointer {
    store: Arc<dyn InitialCheckpointStore>,
    namespace: String,
    event_hub_name: String,
    consumer_group: String,
}

impl Checkpointer {
    pub fn new(
        store: Arc<dyn InitialCheckpointStore>,
        namespace: &str,
        event_hub_name: &str,
        consumer_group: &str,
    ) -> Self {
        // The Azure SDKs key checkpoints by the lower-cased identifiers.
        Self {
            store,
            namespace: namespace.to_ascii_lowercase(),
            event_hub_name: event_hub_name.to_ascii_lowercase(),
            consumer_group: consumer_group.to_ascii_lowercase(),
        }
    }

    /// Loads the stored positions, keyed by partition ID.
    pub async fn load(&self) -> azure_core::Result<HashMap<String, Position>> {
        let checkpoints = self
            .store
            .list_checkpoints(&self.namespace, &self.event_hub_name, &self.consumer_group)
            .await?;
        Ok(checkpoints
            .into_iter()
            .filter_map(|checkpoint| {
                Position::new(checkpoint.offset, checkpoint.sequence_number)
                    .map(|position| (checkpoint.partition_id, position))
            })
            .collect())
    }

    fn checkpoint(&self, partition_id: &str, position: &Position) -> Checkpoint {
        Checkpoint {
            fully_qualified_namespace: self.namespace.clone(),
            event_hub_name: self.event_hub_name.clone(),
            consumer_group: self.consumer_group.clone(),
            partition_id: partition_id.to_string(),
            offset: position.offset.clone(),
            sequence_number: position.sequence_number,
        }
    }

    pub async fn commit(&self, partition_id: &str, position: &Position) -> azure_core::Result<()> {
        self.store
            .update_checkpoint(self.checkpoint(partition_id, position))
            .await
    }

    /// Records `latest` as the first checkpoint of a partition that had none when the source
    /// loaded the checkpoints, so that a restart resumes after it instead of at `latest` again.
    ///
    /// The write is create-only ([`InitialCheckpointStore::create_checkpoint`]): if a checkpoint
    /// exists by now, it is kept and returned instead.
    pub async fn initialize(
        &self,
        partition_id: &str,
        latest: Position,
    ) -> azure_core::Result<InitialCheckpoint> {
        if self
            .store
            .create_checkpoint(self.checkpoint(partition_id, &latest))
            .await?
        {
            return Ok(InitialCheckpoint::Created(latest));
        }
        Ok(match self.load().await?.remove(partition_id) {
            Some(existing) => InitialCheckpoint::Existing(existing),
            None => InitialCheckpoint::Unreadable(latest),
        })
    }
}

/// An in-memory [`CheckpointStore`], for tests.
#[cfg(test)]
#[derive(Default)]
pub struct MemoryCheckpointStore {
    pub checkpoints: std::sync::Mutex<HashMap<String, Checkpoint>>,
    pub fail_updates: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
#[async_trait::async_trait]
impl CheckpointStore for MemoryCheckpointStore {
    async fn claim_ownership(
        &self,
        ownerships: &[Ownership],
    ) -> azure_core::Result<Vec<Ownership>> {
        Ok(ownerships.to_vec())
    }

    async fn list_checkpoints(
        &self,
        namespace: &str,
        event_hub_name: &str,
        consumer_group: &str,
    ) -> azure_core::Result<Vec<Checkpoint>> {
        Ok(self
            .checkpoints
            .lock()
            .unwrap()
            .values()
            .filter(|c| {
                c.fully_qualified_namespace == namespace
                    && c.event_hub_name == event_hub_name
                    && c.consumer_group == consumer_group
            })
            .cloned()
            .collect())
    }

    async fn list_ownerships(
        &self,
        _namespace: &str,
        _event_hub_name: &str,
        _consumer_group: &str,
    ) -> azure_core::Result<Vec<Ownership>> {
        Ok(Vec::new())
    }

    async fn update_checkpoint(&self, checkpoint: Checkpoint) -> azure_core::Result<()> {
        if self.fail_updates.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(azure_core::Error::with_message(
                azure_core::error::ErrorKind::Io,
                "injected checkpoint failure",
            ));
        }
        self.checkpoints
            .lock()
            .unwrap()
            .insert(checkpoint.partition_id.clone(), checkpoint);
        Ok(())
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl InitialCheckpointStore for MemoryCheckpointStore {
    async fn create_checkpoint(&self, checkpoint: Checkpoint) -> azure_core::Result<bool> {
        if self.fail_updates.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(azure_core::Error::with_message(
                azure_core::error::ErrorKind::Io,
                "injected checkpoint failure",
            ));
        }
        // Like the blob store's `If-None-Match: *` upload: atomic, and never replaces a record.
        match self
            .checkpoints
            .lock()
            .unwrap()
            .entry(checkpoint.partition_id.clone())
        {
            std::collections::hash_map::Entry::Occupied(_) => Ok(false),
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(checkpoint);
                Ok(true)
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    #[test]
    fn position_requires_a_field() {
        assert_eq!(Position::new(None, None), None);
        let p = Position::new(Some("96".into()), Some(1)).unwrap();
        assert!(matches!(p.after().location, StartLocation::Offset(ref o) if o == "96"));
        assert!(!p.after().inclusive);
        assert!(p.at().inclusive);
        let p = Position::new(None, Some(7)).unwrap();
        assert!(matches!(
            p.after().location,
            StartLocation::SequenceNumber(7)
        ));
    }

    #[tokio::test]
    async fn checkpointer_round_trip_is_case_insensitive() {
        let store = Arc::new(MemoryCheckpointStore::default());
        let writer = Checkpointer::new(
            store.clone(),
            "NS.servicebus.windows.net",
            "Hub",
            "$Default",
        );
        writer
            .commit("0", &Position::new(Some("10".into()), Some(3)).unwrap())
            .await
            .unwrap();
        let reader = Checkpointer::new(store, "ns.servicebus.windows.net", "hub", "$default");
        let loaded = reader.load().await.unwrap();
        assert_eq!(
            loaded.get("0"),
            Some(&Position::new(Some("10".into()), Some(3)).unwrap())
        );
    }

    pub(crate) fn partition_properties(
        last_sequence_number: i64,
        last_offset: &str,
    ) -> EventHubPartitionProperties {
        EventHubPartitionProperties {
            id: "0".into(),
            eventhub: "hub".into(),
            beginning_sequence_number: 0,
            last_enqueued_sequence_number: last_sequence_number,
            last_enqueued_offset: last_offset.into(),
            last_enqueued_time_utc: None,
            is_empty: last_sequence_number < 0,
        }
    }

    #[test]
    fn last_enqueued_position() {
        let p = Position::last_enqueued(&partition_properties(41, "4100"));
        assert_eq!(p, Position::new(Some("4100".into()), Some(41)).unwrap());
        assert!(matches!(p.after().location, StartLocation::Offset(ref o) if o == "4100"));
        assert!(!p.after().inclusive);

        // A partition that never had an event: after offset -1 is the start of the partition
        // (the SDK's own `Earliest` is `x-opt-offset > '-1'`).
        let p = Position::last_enqueued(&partition_properties(-1, "-1"));
        assert!(matches!(p.after().location, StartLocation::Offset(ref o) if o == "-1"));

        // No offset reported: fall back to the sequence number.
        let p = Position::last_enqueued(&partition_properties(7, ""));
        assert_eq!(p, Position::new(None, Some(7)).unwrap());
    }

    fn checkpointer(store: &Arc<MemoryCheckpointStore>) -> Checkpointer {
        Checkpointer::new(
            store.clone(),
            "ns.servicebus.windows.net",
            "hub",
            "$Default",
        )
    }

    fn stored(
        store: &MemoryCheckpointStore,
        partition_id: &str,
    ) -> Option<(Option<String>, Option<i64>)> {
        store
            .checkpoints
            .lock()
            .unwrap()
            .get(partition_id)
            .map(|c| (c.offset.clone(), c.sequence_number))
    }

    #[tokio::test]
    async fn initialize_writes_the_first_checkpoint() {
        let store = Arc::new(MemoryCheckpointStore::default());
        let checkpointer = checkpointer(&store);
        let latest = Position::last_enqueued(&partition_properties(9, "900"));
        let outcome = checkpointer.initialize("0", latest.clone()).await.unwrap();
        assert_eq!(outcome, InitialCheckpoint::Created(latest.clone()));
        assert_eq!(stored(&store, "0"), Some((Some("900".into()), Some(9))));
        // It is an ordinary checkpoint: the next load returns it.
        assert_eq!(checkpointer.load().await.unwrap().get("0"), Some(&latest));
    }

    #[tokio::test]
    async fn initialize_never_overwrites_an_existing_checkpoint() {
        let store = Arc::new(MemoryCheckpointStore::default());
        let checkpointer = checkpointer(&store);
        // Written after the source loaded the checkpoints (e.g. by another instance).
        let existing = Position::new(Some("500".into()), Some(5)).unwrap();
        checkpointer.commit("0", &existing).await.unwrap();

        let latest = Position::last_enqueued(&partition_properties(9, "900"));
        let outcome = checkpointer.initialize("0", latest).await.unwrap();
        assert_eq!(outcome, InitialCheckpoint::Existing(existing.clone()));
        assert_eq!(outcome.position(), &existing);
        assert_eq!(stored(&store, "0"), Some((Some("500".into()), Some(5))));
        // Other partitions are unaffected.
        assert_eq!(stored(&store, "1"), None);
    }

    #[tokio::test]
    async fn initialize_keeps_a_checkpoint_without_a_position() {
        let store = Arc::new(MemoryCheckpointStore::default());
        let checkpointer = checkpointer(&store);
        store.checkpoints.lock().unwrap().insert(
            "0".into(),
            Checkpoint {
                fully_qualified_namespace: "ns.servicebus.windows.net".into(),
                event_hub_name: "hub".into(),
                consumer_group: "$default".into(),
                partition_id: "0".into(),
                offset: None,
                sequence_number: None,
            },
        );
        let latest = Position::last_enqueued(&partition_properties(9, "900"));
        let outcome = checkpointer.initialize("0", latest.clone()).await.unwrap();
        assert_eq!(outcome, InitialCheckpoint::Unreadable(latest));
        assert_eq!(stored(&store, "0"), Some((None, None)));
    }

    #[tokio::test]
    async fn initialize_reports_write_failures() {
        let store = Arc::new(MemoryCheckpointStore::default());
        store
            .fail_updates
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let latest = Position::last_enqueued(&partition_properties(9, "900"));
        assert!(checkpointer(&store).initialize("0", latest).await.is_err());
        assert_eq!(stored(&store, "0"), None);
    }

    #[test]
    fn blob_store_config_validation() {
        let proxy = ProxyConfig::default();
        let cfg = |cs: Option<&str>, account: Option<&str>, auth: Option<AzureAuthentication>| {
            CheckpointStoreConfig::AzureBlob {
                connection_string: cs.map(|s| s.to_string().into()),
                account_name: account.map(str::to_string),
                blob_endpoint: None,
                container_name: "checkpoints".into(),
                auth,
            }
        };
        // No source of storage location.
        assert!(cfg(None, None, None).build(&proxy).is_err());
        // An account name without credentials.
        assert!(cfg(None, Some("acct"), None).build(&proxy).is_err());
        // Key-bearing connection string plus `auth`.
        let keyed = "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=a2V5;EndpointSuffix=core.windows.net";
        assert!(
            cfg(Some(keyed), None, Some(AzureAuthentication::default()))
                .build(&proxy)
                .is_err()
        );
        // Valid shared key connection string.
        assert!(cfg(Some(keyed), None, None).build(&proxy).is_ok());
    }
}
