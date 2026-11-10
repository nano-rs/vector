//! LocalStack (SQS + S3) tests. Run with `cargo vdev int test aws`, or against any LocalStack:
//! `S3_ADDRESS=http://127.0.0.1:4566 cargo test --features crowdstrike-fdr-integration-tests --lib sources::crowdstrike_fdr::integration_tests`.

use std::{
    io::Write as _,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use aws_sdk_sqs::{Client as SqsClient, types::QueueAttributeName};
use futures::{Stream, StreamExt};
use tokio::{io::AsyncWriteExt, net::TcpListener};
use vector_lib::{
    codecs::decoding::{DeserializerConfig, FramingConfig, JsonDeserializerConfig},
    config::LogNamespace,
    event::{EventContainer, EventStatus, Finalizable},
    shutdown::ShutdownSignal,
    source_sender::SourceSenderItem,
};
use vrl::value::Value;

use super::{
    CrowdstrikeFdrConfig, LOOKUPS_OUTPUT,
    aws::{S3Store, SqsQueue},
    completion::Completions,
    manifest::{ChecksumMode, md5_hex},
    source::{Engine, Settings},
};
use crate::{
    SourceSender,
    aws::{AwsAuthentication, create_client},
    codecs::DecodingConfig,
    common::{s3::S3ClientBuilder, sqs::SqsClientBuilder},
    config::{ProxyConfig, SourceConfig, SourceContext},
    event::Event,
};

fn address() -> String {
    std::env::var("S3_ADDRESS").unwrap_or_else(|_| "http://localhost:4566".into())
}

async fn s3_client(endpoint: String) -> S3Client {
    create_client::<S3ClientBuilder>(
        &S3ClientBuilder {
            force_path_style: Some(true),
        },
        &AwsAuthentication::test_auth(),
        Some(aws_types::region::Region::new("us-east-1")),
        Some(endpoint),
        &ProxyConfig::default(),
        None,
        None,
    )
    .await
    .unwrap()
}

async fn sqs_client() -> SqsClient {
    create_client::<SqsClientBuilder>(
        &SqsClientBuilder {},
        &AwsAuthentication::test_auth(),
        Some(aws_types::region::Region::new("us-east-1")),
        Some(address()),
        &ProxyConfig::default(),
        None,
        None,
    )
    .await
    .unwrap()
}

struct Fixture {
    sqs: SqsClient,
    s3: S3Client,
    queue_url: String,
    bucket: String,
}

impl Fixture {
    async fn new() -> Self {
        let sqs = sqs_client().await;
        let s3 = s3_client(address()).await;
        let queue_url = sqs
            .create_queue()
            .queue_name(uuid::Uuid::new_v4().to_string())
            .attributes(QueueAttributeName::VisibilityTimeout, "30")
            .send()
            .await
            .unwrap()
            .queue_url
            .unwrap();
        let bucket = uuid::Uuid::new_v4().to_string();
        s3.create_bucket().bucket(&bucket).send().await.unwrap();
        Self {
            sqs,
            s3,
            queue_url,
            bucket,
        }
    }

    /// Uploads gzipped NDJSON and returns its manifest entry.
    async fn put(&self, key: &str, lines: &[&str]) -> String {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        for line in lines {
            encoder.write_all(line.as_bytes()).unwrap();
            encoder.write_all(b"\n").unwrap();
        }
        let data = encoder.finish().unwrap();
        let entry = format!(
            r#"{{"path":"{key}","size":{},"checksum":"{}"}}"#,
            data.len(),
            md5_hex(&data)
        );
        self.s3
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(data))
            .send()
            .await
            .unwrap();
        entry
    }

    async fn send_manifest(&self, entries: &[String]) {
        // Batch time as a string, as FDR sometimes sends it.
        let body = format!(
            r#"{{"cid":"cid0","timestamp":"{}","fileCount":{},"totalSize":1,"bucket":"{}","pathPrefix":"cid0/data/u","files":[{}]}}"#,
            chrono::Utc::now().timestamp_millis(),
            entries.len(),
            self.bucket,
            entries.join(",")
        );
        self.sqs
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(body)
            .send()
            .await
            .unwrap();
    }

    /// (visible, in flight) message counts.
    async fn depth(&self) -> (u64, u64) {
        let output = self
            .sqs
            .get_queue_attributes()
            .queue_url(&self.queue_url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
            .send()
            .await
            .unwrap();
        let attributes = output.attributes.unwrap_or_default();
        let read = |name| {
            attributes
                .get(&name)
                .and_then(|value| value.parse().ok())
                .unwrap_or(0)
        };
        (
            read(QueueAttributeName::ApproximateNumberOfMessages),
            read(QueueAttributeName::ApproximateNumberOfMessagesNotVisible),
        )
    }

    async fn wait_for_depth(&self, want: (u64, u64)) {
        for _ in 0..120 {
            if self.depth().await == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!(
            "queue depth never reached {want:?}; now {:?}",
            self.depth().await
        );
    }

    fn config(&self, extra: &str) -> CrowdstrikeFdrConfig {
        toml::from_str(&format!(
            r#"
region = "us-east-1"
endpoint = "{}"
queue_url = "{}"
poll_secs = 1
retry_delay_secs = 1
queue_depth_interval_secs = 0
force_path_style = true
auth.access_key_id = "dummy"
auth.secret_access_key = "dummy"
decoding.codec = "json"
acknowledgements = true
{extra}
"#,
            address(),
            self.queue_url
        ))
        .unwrap()
    }
}

type Items = std::pin::Pin<Box<dyn Stream<Item = SourceSenderItem> + Send>>;

async fn take(items: &mut Items, n: usize) -> Vec<Event> {
    let mut events = Vec::new();
    while events.len() < n {
        let item = tokio::time::timeout(Duration::from_mins(1), items.next())
            .await
            .expect("events arrive")
            .expect("stream open");
        events.extend(item.events.into_events());
    }
    events
}

fn finalize(events: Vec<Event>, status: EventStatus) {
    for mut event in events {
        event.take_finalizers().update_status(status);
    }
}

fn line_of(event: &Event) -> i64 {
    event
        .as_log()
        .parse_path_and_get_value("fdr.line")
        .unwrap()
        .and_then(Value::as_integer)
        .unwrap()
}

#[tokio::test]
async fn aws_crowdstrike_fdr_deletes_only_after_acknowledgement() {
    let fixture = Fixture::new().await;
    let a = fixture
        .put("cid0/data/u/part-00000.gz", &[r#"{"n":1}"#, r#"{"n":2}"#])
        .await;
    let b = fixture
        .put("cid0/data/u/part-00001.gz", &[r#"{"n":3}"#])
        .await;
    fixture.send_manifest(&[a, b]).await;

    let (tx, rx) = SourceSender::new_test_sender_with_options(1000, None);
    let mut items: Items = rx.into_stream();
    let source = fixture
        .config("")
        .build(SourceContext::new_test(tx, None))
        .await
        .unwrap();
    let task = tokio::spawn(source);

    let events = take(&mut items, 3).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(fixture.depth().await, (0, 1), "held, not deleted");

    finalize(events, EventStatus::Delivered);
    fixture.wait_for_depth((0, 0)).await;
    task.abort();
}

#[tokio::test]
async fn aws_crowdstrike_fdr_sink_outage_redelivers_without_loss() {
    let fixture = Fixture::new().await;
    let a = fixture
        .put("cid0/data/u/part-00000.gz", &[r#"{"n":1}"#, r#"{"n":2}"#])
        .await;
    fixture.send_manifest(&[a]).await;

    let (tx, rx) = SourceSender::new_test_sender_with_options(1000, None);
    let mut items: Items = rx.into_stream();
    let source = fixture
        .config("")
        .build(SourceContext::new_test(tx, None))
        .await
        .unwrap();
    let task = tokio::spawn(source);

    let first = take(&mut items, 2).await;
    finalize(first, EventStatus::Errored);

    // Released with retry_delay_secs = 1, then re-delivered: same lines again.
    let second = take(&mut items, 2).await;
    let mut lines: Vec<_> = second.iter().map(line_of).collect();
    lines.sort_unstable();
    assert_eq!(lines, vec![0, 1]);
    finalize(second, EventStatus::Delivered);
    fixture.wait_for_depth((0, 0)).await;
    task.abort();
}

#[tokio::test]
async fn aws_crowdstrike_fdr_routes_lookup_snapshots() {
    let fixture = Fixture::new().await;
    let data = fixture
        .put("cid0/data/u/part-00000.gz", &[r#"{"n":1}"#])
        .await;
    let lookup = fixture
        .put(
            "cid0/fdrv2-aidmaster-2026/part-00000.gz",
            &[r#"{"aid":"a1"}"#],
        )
        .await;
    fixture.send_manifest(&[data, lookup]).await;

    let (mut tx, rx) = SourceSender::new_test_sender_with_options(1000, None);
    let mut lookups: Items =
        Box::pin(tx.add_outputs(EventStatus::Delivered, LOOKUPS_OUTPUT.into()));
    let mut items: Items = rx.into_stream();
    let source = fixture
        .config("")
        .build(SourceContext::new_test(tx, None))
        .await
        .unwrap();
    let task = tokio::spawn(source);

    let events = take(&mut items, 1).await;
    let lookup_events = take(&mut lookups, 1).await;
    assert_eq!(
        lookup_events[0]
            .as_log()
            .parse_path_and_get_value("fdr.kind")
            .unwrap(),
        Some(&Value::from("lookup"))
    );
    assert_eq!(
        lookup_events[0]
            .as_log()
            .parse_path_and_get_value("aid")
            .unwrap(),
        Some(&Value::from("a1"))
    );
    drop(lookup_events);
    finalize(events, EventStatus::Delivered);
    fixture.wait_for_depth((0, 0)).await;
    task.abort();
}

/// An S3 endpoint that sends headers and a few body bytes, then goes silent.
async fn stalling_s3(gets: Arc<AtomicUsize>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            gets.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                drop(tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await);
                drop(
                    socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\nContent-Type: application/gzip\r\n\r\n\x1f\x8b",
                        )
                        .await,
                );
                tokio::time::sleep(Duration::from_hours(1)).await;
            });
        }
    });
    format!("http://{address}")
}

#[tokio::test]
async fn aws_crowdstrike_fdr_stalled_stream_releases() {
    let fixture = Fixture::new().await;
    let entry = format!(
        r#"{{"path":"cid0/data/u/part-00000.gz","size":100000,"checksum":"{}"}}"#,
        md5_hex(b"never")
    );
    fixture.send_manifest(&[entry]).await;

    let settings = Settings {
        max_number_of_messages: 10,
        visibility_timeout_secs: 30,
        heartbeat_interval: Duration::from_secs(15),
        max_hold: Duration::from_mins(10),
        file_concurrency: 4,
        max_inflight_manifests: 4,
        max_inflight_kib: None,
        max_tries: 3,
        retry_delay_secs: 1,
        fetch_max_tries: 1,
        fetch_retry_backoff: Duration::from_secs(1),
        spool_threshold_bytes: 64 * 1024 * 1024,
        spool_dir: std::env::temp_dir().join("vector-crowdstrike-fdr-int-tests"),
        checksum: ChecksumMode::Verify,
        cutoff: None,
        route_lookups: false,
        max_line_bytes: 1024,
        max_object_bytes: 1024 * 1024,
        delete_message: true,
        delete_failed_message: false,
        call_timeout: Duration::from_secs(30),
        stream_idle_timeout: Duration::from_secs(2),
        queue_depth_interval: None,
        acknowledgements: true,
        log_namespace: LogNamespace::Legacy,
    };
    let decoder = DecodingConfig::new(
        FramingConfig::Bytes,
        DeserializerConfig::Json(JsonDeserializerConfig::default()),
        LogNamespace::Legacy,
    )
    .build()
    .unwrap();
    let gets = Arc::new(AtomicUsize::new(0));
    let engine = Engine::new(
        Arc::new(SqsQueue {
            client: sqs_client().await,
            queue_url: fixture.queue_url.clone(),
            wait_time_secs: 1,
        }),
        Arc::new(S3Store {
            client: s3_client(stalling_s3(Arc::clone(&gets)).await).await,
        }),
        settings,
        decoder,
        None,
        Arc::new(Completions::in_memory()),
    );
    let (tx, _rx) = SourceSender::new_test_sender_with_options(1000, None);
    let started = std::time::Instant::now();
    let task = tokio::spawn(engine.run(tx, ShutdownSignal::noop()));

    // The stall is detected after ~2 s and the message released (visible again after 1 s), so
    // it is fetched again long before its 30 s visibility timeout would lapse on its own.
    for _ in 0..60 {
        if gets.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        gets.load(Ordering::SeqCst) >= 2,
        "stalled GET never released the message"
    );
    assert!(started.elapsed() < Duration::from_secs(20));
    task.abort();
}

/// NAN-5004 class against real SQS semantics: a rejected batch is released, re-delivered and
/// never deleted, until a delivery is acknowledged.
#[tokio::test]
async fn aws_crowdstrike_fdr_rejection_never_deletes() {
    let fixture = Fixture::new().await;
    let a = fixture
        .put("cid0/data/u/part-00000.gz", &[r#"{"n":1}"#, r#"{"n":2}"#])
        .await;
    fixture.send_manifest(&[a]).await;

    let (tx, rx) = SourceSender::new_test_sender_with_options(1000, None);
    let mut items: Items = rx.into_stream();
    let source = fixture
        .config("max_tries = 10")
        .build(SourceContext::new_test(tx, None))
        .await
        .unwrap();
    let task = tokio::spawn(source);

    for _ in 0..2 {
        let events = take(&mut items, 2).await;
        finalize(events, EventStatus::Rejected);
    }
    // Still on the queue (visible or held), never deleted.
    let (visible, held) = fixture.depth().await;
    assert_eq!(visible + held, 1, "a rejected batch must stay on the queue");

    let third = take(&mut items, 2).await;
    finalize(third, EventStatus::Delivered);
    fixture.wait_for_depth((0, 0)).await;
    task.abort();
}

/// The `start_from` cutoff must not delete a message an earlier run accepted. Run 1 (`now`) takes
/// a batch and dies before acknowledging it; run 2 starts later with a `now` past the batch
/// (a fresh data dir, so nothing is pinned). SQS re-delivers with `ApproximateReceiveCount` 2:
/// run 2 delivers it instead of skipping it.
#[tokio::test]
async fn aws_crowdstrike_fdr_restart_does_not_skip_a_redelivered_batch() {
    let fixture = Fixture::new().await;
    let run1_dir = tempfile::tempdir().unwrap();
    let run2_dir = tempfile::tempdir().unwrap();
    let config = |dir: &tempfile::TempDir| {
        fixture.config(&format!(
            "start_from = \"now\"\nvisibility_timeout_secs = 2\ndata_dir = {:?}",
            dir.path().to_str().unwrap()
        ))
    };

    let (tx, rx) = SourceSender::new_test_sender_with_options(1000, None);
    let mut items: Items = rx.into_stream();
    let run1 = tokio::spawn(
        config(&run1_dir)
            .build(SourceContext::new_test(tx, None))
            .await
            .unwrap(),
    );
    let a = fixture
        .put("cid0/data/u/part-00000.gz", &[r#"{"n":1}"#, r#"{"n":2}"#])
        .await;
    fixture.send_manifest(&[a]).await;
    let unacknowledged = take(&mut items, 2).await;
    run1.abort(); // crash: no release, no delete
    drop(unacknowledged);
    drop(items);

    tokio::time::sleep(Duration::from_secs(1)).await;
    let (tx, rx) = SourceSender::new_test_sender_with_options(1000, None);
    let mut items: Items = rx.into_stream();
    let run2 = tokio::spawn(
        config(&run2_dir)
            .build(SourceContext::new_test(tx, None))
            .await
            .unwrap(),
    );
    let redelivered = take(&mut items, 2).await;
    let mut lines: Vec<_> = redelivered.iter().map(line_of).collect();
    lines.sort_unstable();
    assert_eq!(lines, vec![0, 1]);
    let (visible, held) = fixture.depth().await;
    assert_eq!(visible + held, 1, "still on the queue until acknowledged");
    finalize(redelivered, EventStatus::Delivered);
    fixture.wait_for_depth((0, 0)).await;
    run2.abort();
}

/// The field failure (NAN-5005 follow-up) against real SQS semantics: file 16 of 20 is cut
/// mid-body on every try of the first delivery, so the batch is released; SQS re-delivers it
/// and only the files not yet delivered are fetched again. No event is emitted twice.
#[tokio::test]
async fn aws_crowdstrike_fdr_cut_download_redelivers_without_duplicates() {
    use std::collections::HashMap;

    let fixture = Fixture::new().await;
    let objects: Vec<(String, bytes::Bytes)> = (0..20)
        .map(|file| {
            let lines: Vec<String> = (0..3)
                .map(|line| format!(r#"{{"f":{file},"l":{line}}}"#))
                .collect();
            let refs: Vec<&str> = lines.iter().map(String::as_str).collect();
            (
                format!("cid0/data/u/part-{file:05}.gz"),
                super::tests::gzip(&refs),
            )
        })
        .collect();
    let entries: Vec<String> = objects
        .iter()
        .map(|(key, data)| super::tests::file_entry(key, data))
        .collect();
    fixture.send_manifest(&entries).await;
    let s3 = super::fetch_tests::cutting_s3(
        objects.iter().cloned().collect(),
        HashMap::from([(objects[16].0.clone(), 3)]),
    )
    .await;

    let mut settings = super::tests::settings();
    settings.visibility_timeout_secs = 30;
    settings.heartbeat_interval = Duration::from_secs(15);
    settings.retry_delay_secs = 1;
    settings.route_lookups = false;
    settings.file_concurrency = 1;
    settings.fetch_max_tries = 3;
    settings.fetch_retry_backoff = Duration::from_millis(100);
    settings.call_timeout = Duration::from_secs(30);
    settings.stream_idle_timeout = Duration::from_secs(5);
    let decoder = DecodingConfig::new(
        FramingConfig::Bytes,
        DeserializerConfig::Json(JsonDeserializerConfig::default()),
        LogNamespace::Legacy,
    )
    .build()
    .unwrap();
    let engine = Engine::new(
        Arc::new(SqsQueue {
            client: sqs_client().await,
            queue_url: fixture.queue_url.clone(),
            wait_time_secs: 1,
        }),
        Arc::new(super::fetch_tests::s3_store(s3.endpoint.clone()).await),
        settings,
        decoder,
        None,
        Arc::new(Completions::in_memory()),
    );
    let (tx, rx) = SourceSender::new_test_sender_with_options(1000, None);
    let mut items: Items = rx.into_stream();
    let task = tokio::spawn(engine.run(tx, ShutdownSignal::noop()));

    let mut seen: HashMap<(i64, i64), usize> = HashMap::new();
    while seen.len() < 60 {
        let events = take(&mut items, 1).await;
        for event in &events {
            let int = |path| {
                event
                    .as_log()
                    .parse_path_and_get_value(path)
                    .unwrap()
                    .and_then(Value::as_integer)
                    .unwrap()
            };
            *seen.entry((int("f"), int("l"))).or_default() += 1;
        }
        finalize(events, EventStatus::Delivered);
    }
    fixture.wait_for_depth((0, 0)).await;
    assert!(
        seen.values().all(|count| *count == 1),
        "duplicated: {seen:?}"
    );
    let gets = s3.gets.lock().unwrap().clone();
    for (index, (key, _)) in objects.iter().enumerate() {
        let count = gets.iter().filter(|got| *got == key).count();
        assert_eq!(count, if index == 16 { 4 } else { 1 }, "GETs of {key}");
    }
    task.abort();
}
