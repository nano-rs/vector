//! Integration tests against the Azure Event Hubs emulator, with Azurite holding the checkpoints.
//!
//! The emulator and Azurite run from `tests/integration/azure/config/compose.yaml`. To run the
//! tests directly against locally published containers:
//!
//! ```sh
//! EVENTHUBS_EMULATOR_ADDRESS=localhost AZURITE_ADDRESS=localhost \
//!   cargo test --no-default-features --features azure-event-hubs-integration-tests \
//!   --lib sources::azure_event_hubs::integration_tests
//! ```

use std::{collections::HashMap, sync::Arc, time::Duration};

use azure_core::http::Url;
use azure_messaging_eventhubs::{EventDataBatchOptions, ProducerClient, models::EventData};
use futures::{Stream, StreamExt};
use vector_lib::{config::LogNamespace, event::EventStatus, shutdown::ShutdownSignal};

use super::{
    AzureEventHubsSourceConfig, CheckpointStoreConfig, StartPositionConfig,
    checkpoint::{Checkpointer, Position},
    sas::{EventHubsConnectionString, SasTokenCredential},
};
use crate::{
    SourceSender,
    config::{SourceConfig, SourceContext},
    event::Event,
    sinks::azure_common::connection_string::ParsedConnectionString,
    test_util::{
        components::{SOURCE_TAGS, assert_source_compliance},
        random_string,
    },
};

/// The emulator's fixed development credentials (not secrets).
const EMULATOR_KEY: &str = "SAS_KEY_VALUE";
const AZURITE_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const CONSUMER_GROUP: &str = "cg1";

fn emulator_address() -> String {
    std::env::var("EVENTHUBS_EMULATOR_ADDRESS").unwrap_or_else(|_| "localhost".into())
}

fn azurite_address() -> String {
    std::env::var("AZURITE_ADDRESS").unwrap_or_else(|_| "localhost".into())
}

fn connection_string(hub: &str) -> String {
    format!(
        "Endpoint=sb://{};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey={EMULATOR_KEY};UseDevelopmentEmulator=true;EntityPath={hub}",
        emulator_address()
    )
}

fn storage_connection_string() -> String {
    format!(
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey={AZURITE_KEY};BlobEndpoint=http://{}:10000/devstoreaccount1;",
        azurite_address()
    )
}

async fn create_container(name: &str) {
    let parsed = ParsedConnectionString::parse(&storage_connection_string()).unwrap();
    let url = Url::parse(&parsed.container_url(name).unwrap()).unwrap();
    let client = crate::sinks::azure_blob::config::build_client(
        None,
        parsed,
        url,
        &Default::default(),
        None,
    )
    .unwrap();
    client
        .create(None)
        .await
        .expect("create checkpoint container");
}

fn config(hub: &str, container: &str) -> AzureEventHubsSourceConfig {
    AzureEventHubsSourceConfig {
        connection_string: Some(connection_string(hub).into()),
        consumer_group: CONSUMER_GROUP.into(),
        start_position: StartPositionConfig::Earliest,
        checkpoint_store: Some(CheckpointStoreConfig::AzureBlob {
            connection_string: Some(storage_connection_string().into()),
            account_name: None,
            blob_endpoint: None,
            container_name: container.into(),
            auth: None,
        }),
        checkpoint_interval_secs: Duration::from_millis(200),
        ..Default::default()
    }
}

/// Sends `count` events tagged with `run`, spread over the partitions. Returns the bodies.
async fn produce(hub: &str, run: &str, count: usize) -> Vec<String> {
    let parsed = EventHubsConnectionString::parse(&connection_string(hub)).unwrap();
    let credential = Arc::new(SasTokenCredential::new(
        parsed.namespace_host(),
        hub,
        parsed.credentials.clone(),
    ));
    let producer = ProducerClient::builder()
        .with_custom_endpoint(parsed.emulator_endpoint().unwrap())
        .open(parsed.namespace_host(), hub, credential)
        .await
        .expect("open producer");
    let partitions = producer
        .get_eventhub_properties()
        .await
        .unwrap()
        .partition_ids;
    let mut bodies = Vec::with_capacity(count);
    for (chunk_index, chunk) in (0..count).collect::<Vec<_>>().chunks(50).enumerate() {
        let batch = producer
            .create_batch(Some(EventDataBatchOptions {
                partition_id: Some(partitions[chunk_index % partitions.len()].clone()),
                ..Default::default()
            }))
            .await
            .unwrap();
        for i in chunk {
            let body = format!(r#"{{"run":"{run}","i":{i}}}"#);
            let event = EventData::builder()
                .with_body(body.clone().into_bytes())
                .add_property("run".into(), run.to_string())
                .build();
            assert!(batch.try_add_event_data(event, None).unwrap());
            bodies.push(body);
        }
        producer.send_batch(batch, None).await.unwrap();
    }
    producer.close().await.unwrap();
    bodies
}

fn message(event: &Event) -> String {
    event
        .as_log()
        .get_message()
        .map(|m| m.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Runs the source until `stop` returns true for the events of `run` received so far, then shuts it
/// down gracefully. Returns the messages of `run`.
async fn run_source(
    config: &AzureEventHubsSourceConfig,
    out: SourceSender,
    rx: impl Stream<Item = Event> + Unpin,
    run: &str,
    stop: impl Fn(usize) -> bool,
) -> Vec<String> {
    let (trigger, shutdown, done) = ShutdownSignal::new_wired();
    let mut cx = SourceContext::new_test(out, None);
    cx.shutdown = shutdown;
    cx.acknowledgements = true;
    let source = config.build(cx).await.expect("build source");
    let handle = tokio::spawn(source);

    let mut rx = rx;
    let mut messages = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    while !stop(messages.len()) {
        match tokio::time::timeout_at(deadline, rx.next()).await {
            Ok(Some(event)) => {
                let message = message(&event);
                if message.contains(&format!(r#""run":"{run}""#)) {
                    messages.push(message);
                }
            }
            Ok(None) => break,
            Err(_) => panic!(
                "timed out after receiving {} events of run {run}",
                messages.len()
            ),
        }
    }
    // Let the finalizer observe the acknowledgements before shutting down.
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(trigger);
    done.await;
    handle.await.unwrap().expect("source exited cleanly");
    messages
}

fn assert_exactly_once(expected: &[String], received: &[String]) {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for message in received {
        *counts.entry(message.as_str()).or_default() += 1;
    }
    for body in expected {
        assert!(counts.contains_key(body.as_str()), "missing event {body}");
    }
    assert_eq!(counts.len(), expected.len(), "unexpected events received");
}

#[tokio::test]
async fn azure_event_hubs_delivers_all_events_and_resumes_from_checkpoints() {
    let hub = "eh-resume";
    let container = format!("checkpoints-{}", random_string(8).to_lowercase());
    create_container(&container).await;
    let config = config(hub, &container);
    let run = random_string(10);

    // First pass: everything produced so far is delivered exactly once.
    let first = produce(hub, &run, 200).await;
    let received = assert_source_compliance(&SOURCE_TAGS, async {
        let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
        run_source(&config, tx, rx, &run, |n| n >= first.len()).await
    })
    .await;
    assert_eq!(received.len(), first.len());
    assert_exactly_once(&first, &received);

    // Second pass: after a restart, only the new events are read: the checkpoints hold.
    let second = produce(hub, &run, 100).await;
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let received = run_source(&config, tx, rx, &run, |n| n >= second.len()).await;
    assert_exactly_once(&second, &received);
}

#[tokio::test]
async fn azure_event_hubs_replays_after_failed_delivery() {
    let hub = "eh-replay";
    let container = format!("checkpoints-{}", random_string(8).to_lowercase());
    create_container(&container).await;
    let config = config(hub, &container);
    let run = random_string(10);
    let produced = produce(hub, &run, 100).await;

    // The 10th batch fails downstream: its partition is replayed from the last acknowledged event,
    // so every event still arrives at least once.
    let (tx, rx) = SourceSender::new_test_errors(|i| i == 10);
    let received = run_source(&config, tx, rx, &run, |n| n >= produced.len() + 1).await;
    let distinct: std::collections::HashSet<_> = received.iter().collect();
    assert_eq!(distinct.len(), produced.len());

    // After a restart nothing is lost and nothing old is re-read beyond what was in flight.
    let more = produce(hub, &run, 20).await;
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let received = run_source(&config, tx, rx, &run, |n| n >= more.len()).await;
    for body in &more {
        assert!(received.contains(body), "missing event {body}");
    }
}

fn checkpointer(config: &AzureEventHubsSourceConfig, hub: &str) -> Checkpointer {
    let store = config
        .checkpoint_store
        .as_ref()
        .unwrap()
        .build(&Default::default())
        .unwrap();
    Checkpointer::new(
        store,
        &config.connection().unwrap().namespace,
        hub,
        &config.consumer_group,
    )
}

/// Starts the source with no traffic, waits until every partition has a checkpoint, and shuts it
/// down. Returns the checkpoints.
async fn start_idle(
    config: &AzureEventHubsSourceConfig,
    checkpointer: &Checkpointer,
    partitions: usize,
) -> HashMap<String, Position> {
    let (trigger, shutdown, done) = ShutdownSignal::new_wired();
    let (tx, _rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let mut cx = SourceContext::new_test(tx, None);
    cx.shutdown = shutdown;
    cx.acknowledgements = true;
    let handle = tokio::spawn(config.build(cx).await.expect("build source"));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let checkpoints = loop {
        let checkpoints = checkpointer.load().await.unwrap();
        if checkpoints.len() >= partitions {
            break checkpoints;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for initial checkpoints: {checkpoints:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    drop(trigger);
    done.await;
    handle.await.unwrap().expect("source exited cleanly");
    checkpoints
}

/// `start_position = "latest"`: the source starts while the partitions are idle and stops before
/// any event arrives. It wrote an initial checkpoint per partition, so after a restart it reads
/// every event enqueued while it was down, and none from before its first start.
#[tokio::test]
async fn azure_event_hubs_latest_restart_while_idle_skips_nothing() {
    let hub = "eh-idle";
    let container = format!("checkpoints-{}", random_string(8).to_lowercase());
    create_container(&container).await;
    let mut config = config(hub, &container);
    config.start_position = StartPositionConfig::Latest;
    let checkpointer = checkpointer(&config, hub);
    let run = random_string(10);

    // History from before the source's first start, tagged with its own run so that its bodies
    // differ from the later ones: `latest` never reads it.
    let history_run = format!("{run}-history");
    let history = produce(hub, &history_run, 20).await;
    let initial = start_idle(&config, &checkpointer, 2).await;
    assert_eq!(initial.len(), 2, "{initial:?}");

    let while_down = produce(hub, &run, 60).await;
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let (history_tx, history_rx) = futures::channel::mpsc::unbounded();
    // Everything the restarted source reads goes to `rx`; copy the history events aside.
    let rx = rx.inspect(move |event| {
        let message = message(event);
        if message.contains(&format!(r#""run":"{history_run}""#)) {
            history_tx.unbounded_send(message).unwrap();
        }
    });
    let received = run_source(&config, tx, rx, &run, |n| n >= while_down.len()).await;
    assert_exactly_once(&while_down, &received);
    let history_read: Vec<String> = history_rx.collect().await;
    assert!(
        history_read.is_empty(),
        "read {} of {} history events: {history_read:?}",
        history_read.len(),
        history.len()
    );

    // An existing checkpoint is never replaced by an initial one: an idle start leaves the
    // checkpoints exactly as the previous run committed them.
    let committed = checkpointer.load().await.unwrap();
    assert_ne!(committed, initial);
    assert_eq!(start_idle(&config, &checkpointer, 2).await, committed);
    assert_eq!(checkpointer.load().await.unwrap(), committed);
}

#[tokio::test]
async fn azure_event_hubs_rejects_unknown_event_hub() {
    let container = format!("checkpoints-{}", random_string(8).to_lowercase());
    create_container(&container).await;
    let config = config("does-not-exist", &container);
    let (tx, _rx) = SourceSender::new_test();
    let error = match config.build(SourceContext::new_test(tx, None)).await {
        Ok(_) => panic!("building a source for a missing Event Hub must fail"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("does-not-exist"), "{error}");
}

#[test]
fn log_namespace_is_respected() {
    // The schema of both namespaces must be derivable.
    let config = config("eh-resume", "unused");
    assert!(!config.outputs(LogNamespace::Vector).is_empty());
    assert!(!config.outputs(LogNamespace::Legacy).is_empty());
}
