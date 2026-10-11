//! Checkpoint persistence for the `azure_event_hubs` source.
//!
//! Checkpoints go through the [`CheckpointStore`] trait of `azure_messaging_eventhubs`, so any
//! implementation of that trait can back the source. The bundled implementation stores them in
//! Azure Blob Storage using the same layout as the Azure SDKs' blob checkpoint stores
//! (`<namespace>/<event hub>/<consumer group>/checkpoint/<partition id>`), which keeps
//! checkpoints interchangeable with other Event Hubs processors.

use std::{collections::HashMap, sync::Arc};

use azure_core::http::Url;
use azure_messaging_eventhubs::{
    CheckpointStore, StartLocation, StartPosition,
    models::Checkpoint,
};
use azure_messaging_eventhubs_checkpointstore_blob::BlobCheckpointStore;
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
    pub fn build(&self, proxy: &ProxyConfig) -> crate::Result<Arc<dyn CheckpointStore>> {
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
                        return Err("`checkpoint_store`: no storage credentials; use a connection \
                                    string with an account key or SAS token, or set `auth`"
                            .into());
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
                let client = build_client(auth.clone(), parsed, container_url, proxy, None)?;
                let client = Arc::try_unwrap(client)
                    .map_err(|_| "`checkpoint_store`: storage client unexpectedly shared")?;
                Ok(BlobCheckpointStore::new(client))
            }
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
}

/// Reads and writes the checkpoints of one Event Hub and consumer group.
#[derive(Clone)]
pub struct Checkpointer {
    store: Arc<dyn CheckpointStore>,
    namespace: String,
    event_hub_name: String,
    consumer_group: String,
}

impl Checkpointer {
    pub fn new(
        store: Arc<dyn CheckpointStore>,
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

    pub async fn commit(&self, partition_id: &str, position: &Position) -> azure_core::Result<()> {
        self.store
            .update_checkpoint(Checkpoint {
                fully_qualified_namespace: self.namespace.clone(),
                event_hub_name: self.event_hub_name.clone(),
                consumer_group: self.consumer_group.clone(),
                partition_id: partition_id.to_string(),
                offset: position.offset.clone(),
                sequence_number: position.sequence_number,
            })
            .await
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
use azure_messaging_eventhubs::models::Ownership;

#[cfg(test)]
#[async_trait::async_trait]
impl CheckpointStore for MemoryCheckpointStore {
    async fn claim_ownership(&self, ownerships: &[Ownership]) -> azure_core::Result<Vec<Ownership>> {
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
        if self
            .fail_updates
            .load(std::sync::atomic::Ordering::SeqCst)
        {
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
mod tests {
    use super::*;

    #[test]
    fn position_requires_a_field() {
        assert_eq!(Position::new(None, None), None);
        let p = Position::new(Some("96".into()), Some(1)).unwrap();
        assert!(matches!(p.after().location, StartLocation::Offset(ref o) if o == "96"));
        assert!(!p.after().inclusive);
        assert!(p.at().inclusive);
        let p = Position::new(None, Some(7)).unwrap();
        assert!(matches!(p.after().location, StartLocation::SequenceNumber(7)));
    }

    #[tokio::test]
    async fn checkpointer_round_trip_is_case_insensitive() {
        let store = Arc::new(MemoryCheckpointStore::default());
        let writer = Checkpointer::new(store.clone(), "NS.servicebus.windows.net", "Hub", "$Default");
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
