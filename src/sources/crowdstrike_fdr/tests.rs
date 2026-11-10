use std::{
    collections::{HashMap, VecDeque},
    io::Write as _,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::{Stream, StreamExt, stream};
use tokio::time::Instant;
use tokio_util::codec::Decoder as _;
use vector_lib::{
    codecs::decoding::{DeserializerConfig, FramingConfig, JsonDeserializerConfig},
    config::LogNamespace,
    event::{EventContainer, EventStatus, Finalizable},
    shutdown::ShutdownSignal,
    source_sender::SourceSenderItem,
};
use vrl::value::Value;

use super::{
    CrowdstrikeFdrConfig, LOOKUPS_OUTPUT, StartFrom,
    completion::Completions,
    cursor::Cursor,
    lines::{Line, LineCodec},
    manifest::{
        ChecksumMode, Expected, Hasher, ObjectKind, classify, md5_hex, parse_checksum,
        parse_manifest, sha256_hex,
    },
    parse_start_from,
    source::{
        BoxError, ByteChunks, DepthError, Engine, ObjectStore, Queue, QueueDepth, QueueMessage,
        Settings,
    },
};
use crate::{SourceSender, codecs::DecodingConfig, config::SourceConfig, event::Event};

// ---------------------------------------------------------------------------------------------
// Manifest parsing and checksums

#[test]
fn parses_numeric_manifest() {
    let manifest = parse_manifest(
        r#"{"cid":"0123456789abcdef0123456789abcdef","timestamp":1794100000000,"fileCount":2,
            "totalSize":30,"bucket":"cs-mav-cannon-s3alias","pathPrefix":"cid/data/uuid",
            "files":[{"path":"cid/data/uuid/part-00000.gz","size":10,"checksum":"0123456789abcdef0123456789abcdef"},
                     {"path":"cid/data/uuid/part-00001.gz","size":20,"checksum":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}]}"#,
    )
    .unwrap();
    assert_eq!(manifest.cid, "0123456789abcdef0123456789abcdef");
    assert_eq!(manifest.timestamp_ms, 1_794_100_000_000);
    assert_eq!(manifest.bucket, "cs-mav-cannon-s3alias");
    assert_eq!(manifest.total_size, 30);
    assert_eq!(manifest.files.len(), 2);
    assert_eq!(manifest.files[1].size, Some(20));
    assert!(
        manifest
            .files
            .iter()
            .all(|file| file.kind == ObjectKind::Data)
    );
    assert_eq!(
        manifest.batch_time(),
        Utc.timestamp_millis_opt(1_794_100_000_000).single()
    );
}

#[test]
fn parses_string_numbers() {
    let manifest = parse_manifest(
        r#"{"cid":"c","timestamp":"1794100000000","totalSize":"5","bucket":"b",
            "files":[{"path":"c/data/u/part-0.gz","size":"5"}]}"#,
    )
    .unwrap();
    assert_eq!(manifest.timestamp_ms, 1_794_100_000_000);
    assert_eq!(manifest.total_size, 5);
    assert_eq!(manifest.files[0].size, Some(5));
    assert_eq!(manifest.files[0].checksum, None);
}

#[test]
fn total_size_falls_back_to_listed_sizes() {
    let manifest = parse_manifest(
        r#"{"timestamp":1,"bucket":"b","files":[{"path":"a","size":3},{"path":"b","size":4}]}"#,
    )
    .unwrap();
    assert_eq!(manifest.total_size, 7);
}

#[test]
fn rejects_bad_manifests() {
    assert!(parse_manifest("not json").is_err());
    assert!(
        parse_manifest(r#"{"timestamp":1,"files":[]}"#).is_err(),
        "no bucket"
    );
    assert!(
        parse_manifest(r#"{"bucket":"b","files":[]}"#).is_err(),
        "no timestamp"
    );
    assert!(parse_manifest(r#"{"bucket":"b","timestamp":"soon","files":[]}"#).is_err());
    assert!(parse_manifest(r#"{"bucket":"b","timestamp":-5,"files":[]}"#).is_err());
    assert!(parse_manifest(r#"{"bucket":"b","timestamp":1,"files":[{"size":1}]}"#).is_err());
}

#[test]
fn classifies_lookup_snapshots() {
    assert_eq!(classify("cid/data/uuid/part-00000.gz"), ObjectKind::Data);
    assert_eq!(
        classify("cid/fdrv2-aidmaster-2026-11-08/part-00000.gz"),
        ObjectKind::Lookup
    );
    assert_eq!(
        classify("cid/fdrv2/managedassets/part-0.gz"),
        ObjectKind::Lookup
    );
    let manifest = parse_manifest(
        r#"{"timestamp":1,"bucket":"b","pathPrefix":"cid/fdrv2-managedassets-x","files":[{"path":"cid/x/part-0.gz"}]}"#,
    )
    .unwrap();
    assert_eq!(manifest.files[0].kind, ObjectKind::Lookup, "prefix decides");
}

#[test]
fn checksum_shapes() {
    let md5 = md5_hex(b"hello");
    let sha = sha256_hex(b"hello");
    assert!(matches!(parse_checksum(Some(&md5)), Ok(Expected::Md5(_))));
    assert!(matches!(
        parse_checksum(Some(&sha)),
        Ok(Expected::Sha256(_))
    ));
    assert!(parse_checksum(None).is_err());
    assert!(parse_checksum(Some("abc")).is_err());
    assert!(parse_checksum(Some(&"z".repeat(32))).is_err(), "not hex");
    assert!(
        parse_checksum(Some(&"a".repeat(40))).is_err(),
        "sha1 shape is unknown"
    );
}

#[test]
fn hasher_verifies_md5_and_sha256() {
    for checksum in [md5_hex(b"hello world"), sha256_hex(b"hello world")] {
        let expected = parse_checksum(Some(&checksum)).unwrap();
        let mut hasher = Hasher::for_expected(&expected);
        hasher.update(b"hello ");
        hasher.update(b"world");
        hasher.verify(&expected).unwrap();

        let mut hasher = Hasher::for_expected(&expected);
        hasher.update(b"hello there");
        assert!(hasher.verify(&expected).unwrap_err().contains("mismatch"));
    }
}

// ---------------------------------------------------------------------------------------------
// Line framing

fn frame_all(input: &[u8], max: usize) -> Vec<Line> {
    let mut codec = LineCodec::new(max);
    let mut buf = bytes::BytesMut::new();
    let mut lines = Vec::new();
    // Feed in small pieces to exercise partial reads.
    for piece in input.chunks(3) {
        buf.extend_from_slice(piece);
        while let Some(line) = codec.decode(&mut buf).unwrap() {
            lines.push(line);
        }
    }
    while let Some(line) = codec.decode_eof(&mut buf).unwrap() {
        lines.push(line);
    }
    lines
}

#[test]
fn lines_are_numbered_and_trimmed() {
    let lines = frame_all(b"a\r\nbb\n\nccc", 10);
    let got: Vec<_> = lines
        .iter()
        .map(|line| (line.number, line.bytes.clone()))
        .collect();
    assert_eq!(
        got,
        vec![
            (0, Some(Bytes::from("a"))),
            (1, Some(Bytes::from("bb"))),
            (2, Some(Bytes::new())),
            (3, Some(Bytes::from("ccc"))),
        ]
    );
}

#[test]
fn oversized_lines_are_dropped_but_counted() {
    let lines = frame_all(b"ok\n0123456789abcdef\nnext\n0123456789abcdef", 8);
    let got: Vec<_> = lines
        .iter()
        .map(|line| (line.number, line.bytes.clone()))
        .collect();
    assert_eq!(
        got,
        vec![
            (0, Some(Bytes::from("ok"))),
            (1, None),
            (2, Some(Bytes::from("next"))),
            (3, None),
        ]
    );
}

// ---------------------------------------------------------------------------------------------
// Config

#[test]
fn generate_config() {
    crate::test_util::test_generate_config::<CrowdstrikeFdrConfig>();
}

#[test]
fn start_from_values() {
    assert_eq!(parse_start_from("beginning").unwrap(), StartFrom::Beginning);
    assert_eq!(parse_start_from("now").unwrap(), StartFrom::Now);
    assert_eq!(parse_start_from("cursor").unwrap(), StartFrom::Cursor);
    assert_eq!(
        parse_start_from("2026-11-01T00:00:00Z").unwrap(),
        StartFrom::Timestamp(Utc.with_ymd_and_hms(2026, 11, 1, 0, 0, 0).unwrap())
    );
    assert!(parse_start_from("yesterday").is_err());
}

fn config(extra: &str) -> CrowdstrikeFdrConfig {
    toml::from_str(&format!(
        "queue_url = \"https://sqs.us-west-2.amazonaws.com/1/q\"\nregion = \"us-west-2\"\n{extra}"
    ))
    .unwrap()
}

#[test]
fn defaults_and_validation() {
    let defaults = config("");
    let (start, heartbeat) = defaults.validate().unwrap();
    assert_eq!(start, StartFrom::Beginning);
    assert_eq!(heartbeat, 150, "half the 300 s visibility timeout");
    assert_eq!(defaults.max_hold_secs, 36_000);
    assert_eq!(defaults.file_concurrency, 4);
    assert_eq!(defaults.max_inflight_manifests, 64);
    assert_eq!(defaults.max_tries, 3);
    assert_eq!(defaults.checksum, ChecksumMode::Verify);
    assert_eq!(defaults.operation_timeout_secs, 90);
    assert_eq!(defaults.read_timeout_secs, 60);
    assert_eq!(defaults.stream_idle_timeout_secs, 120);
    assert_eq!(defaults.fetch_max_tries, 4);
    assert_eq!(defaults.fetch_retry_backoff_secs, 1);
    assert_eq!(defaults.spool_threshold_bytes, 64 * 1024 * 1024);
    assert!(config("fetch_max_tries = 0").validate().is_err());

    assert!(config("heartbeat_interval_secs = 151").validate().is_err());
    assert!(config("heartbeat_interval_secs = 0").validate().is_err());
    assert!(config("max_number_of_messages = 11").validate().is_err());
    assert!(config("poll_secs = 21").validate().is_err());
    assert!(config("read_timeout_secs = 20").validate().is_err());
    assert!(config("start_from = \"later\"").validate().is_err());
    assert!(
        toml::from_str::<CrowdstrikeFdrConfig>("queue_url = \"https://q\"\nchecksum = \"maybe\"")
            .is_err()
    );
}

#[test]
fn outputs_include_lookups_port_when_routing() {
    let outputs = config("").outputs(LogNamespace::Legacy);
    let ports: Vec<_> = outputs.iter().map(|output| output.port.clone()).collect();
    assert_eq!(ports, vec![None, Some(LOOKUPS_OUTPUT.to_owned())]);
    let outputs = config("route_lookups = false").outputs(LogNamespace::Vector);
    assert_eq!(outputs.len(), 1);
}

// ---------------------------------------------------------------------------------------------
// Cursor

#[tokio::test]
async fn cursor_only_moves_forward_and_persists() {
    let dir = tempfile::tempdir().unwrap();
    let cursor = Cursor::open(dir.path()).unwrap();
    assert_eq!(cursor.get().await, None);
    cursor.advance(200).await.unwrap();
    cursor.advance(100).await.unwrap();
    assert_eq!(cursor.get().await, Some(200));
    let reopened = Cursor::open(dir.path()).unwrap();
    assert_eq!(reopened.get().await, Some(200));
}

// ---------------------------------------------------------------------------------------------
// Engine, with mock SQS and S3

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Call {
    Delete(String),
    Visibility(String, i32),
    Get(String),
}

#[derive(Default)]
pub(super) struct MockQueue {
    inbox: Mutex<VecDeque<QueueMessage>>,
    calls: Mutex<Vec<(Duration, Call)>>,
    /// Re-queue a message (new receipt, receive count + 1) when it is released.
    redeliver_on_release: bool,
    start: Mutex<Option<Instant>>,
    depth_calls: std::sync::atomic::AtomicUsize,
}

impl MockQueue {
    pub(super) fn with(messages: Vec<QueueMessage>, redeliver_on_release: bool) -> Arc<Self> {
        Arc::new(Self {
            inbox: Mutex::new(messages.into()),
            redeliver_on_release,
            start: Mutex::new(Some(Instant::now())),
            ..Self::default()
        })
    }

    fn record(&self, call: Call) {
        let start = self.start.lock().unwrap().expect("start set");
        self.calls.lock().unwrap().push((start.elapsed(), call));
    }

    pub(super) fn calls(&self) -> Vec<Call> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|(_, call)| call.clone())
            .collect()
    }

    fn timed_calls(&self) -> Vec<(Duration, Call)> {
        self.calls.lock().unwrap().clone()
    }

    pub(super) fn deletes(&self) -> usize {
        self.calls()
            .iter()
            .filter(|call| matches!(call, Call::Delete(_)))
            .count()
    }
}

#[async_trait]
impl Queue for MockQueue {
    async fn receive(&self, max: i32, _visibility: i32) -> Result<Vec<QueueMessage>, BoxError> {
        let batch: Vec<_> = {
            let mut inbox = self.inbox.lock().unwrap();
            let take = usize::try_from(max).unwrap().min(inbox.len());
            inbox.drain(..take).collect()
        };
        if batch.is_empty() {
            // Long poll.
            tokio::time::sleep(Duration::from_secs(20)).await;
        }
        Ok(batch)
    }

    async fn delete(&self, receipt: &str) -> Result<(), BoxError> {
        self.record(Call::Delete(receipt.to_owned()));
        Ok(())
    }

    async fn change_visibility(&self, receipt: &str, secs: i32) -> Result<(), BoxError> {
        self.record(Call::Visibility(receipt.to_owned(), secs));
        if self.redeliver_on_release && secs == RETRY_DELAY {
            let (id, count) = receipt.rsplit_once('#').unwrap();
            let count: u32 = count.parse().unwrap();
            let original = Self::find_body(id);
            self.inbox.lock().unwrap().push_back(QueueMessage {
                id: id.to_owned(),
                receipt: format!("{id}#{}", count + 1),
                body: original,
                receive_count: count + 1,
            });
        }
        Ok(())
    }

    async fn depth(&self) -> Result<QueueDepth, DepthError> {
        // As an FDR queue policy that grants Receive/Delete/ChangeVisibility only.
        self.depth_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(DepthError::Denied)
    }
}

impl MockQueue {
    fn find_body(id: &str) -> String {
        BODY_REGISTRY
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .expect("registered body")
    }
}

static BODY_REGISTRY: std::sync::LazyLock<Mutex<HashMap<String, String>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

pub(super) fn message(id: &str, body: &str) -> QueueMessage {
    BODY_REGISTRY
        .lock()
        .unwrap()
        .insert(id.to_owned(), body.to_owned());
    QueueMessage {
        id: id.to_owned(),
        receipt: format!("{id}#1"),
        body: body.to_owned(),
        receive_count: 1,
    }
}

/// How a mock object behaves.
#[derive(Clone)]
pub(super) enum Object {
    Data(Bytes),
    /// While `cuts` is above zero, each GET sends the first `at` bytes, then fails as a
    /// connection S3 closed mid-body (and decrements `cuts`); afterwards it serves `data`.
    Cut {
        data: Bytes,
        at: usize,
        cuts: Arc<std::sync::atomic::AtomicUsize>,
    },
    /// Sends the first bytes, then never sends again.
    Stall(Bytes),
    Missing,
}

#[derive(Default)]
pub(super) struct MockStore {
    pub(super) objects: Mutex<HashMap<String, Object>>,
    queue: Mutex<Option<Arc<MockQueue>>>,
}

impl MockStore {
    pub(super) fn with(objects: Vec<(&str, Object)>, queue: &Arc<MockQueue>) -> Arc<Self> {
        Arc::new(Self {
            objects: Mutex::new(
                objects
                    .into_iter()
                    .map(|(key, object)| (key.to_owned(), object))
                    .collect(),
            ),
            queue: Mutex::new(Some(Arc::clone(queue))),
        })
    }
}

#[async_trait]
impl ObjectStore for MockStore {
    async fn get(&self, _bucket: &str, key: &str) -> Result<ByteChunks, BoxError> {
        if let Some(queue) = self.queue.lock().unwrap().as_ref() {
            queue.record(Call::Get(key.to_owned()));
        }
        let object = self.objects.lock().unwrap().get(key).cloned();
        match object {
            Some(Object::Data(data)) => {
                // Several chunks, as S3 would send them.
                let chunks: Vec<Result<Bytes, BoxError>> = data
                    .chunks(7)
                    .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                    .collect();
                Ok(stream::iter(chunks).boxed())
            }
            Some(Object::Cut { data, at, cuts }) => {
                let cut = cuts
                    .fetch_update(
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                        |left| left.checked_sub(1),
                    )
                    .is_ok();
                let sent = if cut { data.slice(..at) } else { data };
                let mut chunks: Vec<Result<Bytes, BoxError>> = sent
                    .chunks(7)
                    .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                    .collect();
                if cut {
                    chunks.push(Err("connection closed before message completed".into()));
                }
                Ok(stream::iter(chunks).boxed())
            }
            Some(Object::Stall(first)) => Ok(stream::once(async move { Ok(first) })
                .chain(stream::pending())
                .boxed()),
            Some(Object::Missing) | None => Err("NoSuchKey".into()),
        }
    }
}

pub(super) const RETRY_DELAY: i32 = 30;

pub(super) fn settings() -> Settings {
    Settings {
        max_number_of_messages: 10,
        visibility_timeout_secs: 300,
        heartbeat_interval: Duration::from_secs(150),
        max_hold: Duration::from_hours(10),
        file_concurrency: 4,
        max_inflight_manifests: 64,
        max_inflight_kib: None,
        max_tries: 3,
        retry_delay_secs: RETRY_DELAY,
        // One try, so the release timing of the older cases is unchanged; the retry cases
        // set more.
        fetch_max_tries: 1,
        fetch_retry_backoff: Duration::from_secs(1),
        spool_threshold_bytes: 64 * 1024 * 1024,
        spool_dir: std::env::temp_dir().join("vector-crowdstrike-fdr-tests"),
        checksum: ChecksumMode::Verify,
        cutoff: None,
        route_lookups: true,
        max_line_bytes: 1024,
        max_object_bytes: 1024 * 1024,
        delete_message: true,
        delete_failed_message: false,
        call_timeout: Duration::from_secs(95),
        stream_idle_timeout: Duration::from_mins(2),
        queue_depth_interval: None,
        acknowledgements: true,
        log_namespace: LogNamespace::Legacy,
    }
}

pub(super) fn gzip(lines: &[&str]) -> Bytes {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    for line in lines {
        encoder.write_all(line.as_bytes()).unwrap();
        encoder.write_all(b"\n").unwrap();
    }
    Bytes::from(encoder.finish().unwrap())
}

pub(super) fn file_entry(path: &str, data: &Bytes) -> String {
    format!(
        r#"{{"path":"{path}","size":{},"checksum":"{}"}}"#,
        data.len(),
        md5_hex(data)
    )
}

pub(super) fn manifest_body(timestamp_ms: i64, files: &[String]) -> String {
    format!(
        r#"{{"cid":"cid0","timestamp":{timestamp_ms},"bucket":"fdr-alias","pathPrefix":"cid0/data/u","fileCount":{},"files":[{}]}}"#,
        files.len(),
        files.join(",")
    )
}

fn json_decoder() -> crate::codecs::Decoder {
    DecodingConfig::new(
        FramingConfig::Bytes,
        DeserializerConfig::Json(JsonDeserializerConfig::default()),
        LogNamespace::Legacy,
    )
    .build()
    .unwrap()
}

pub(super) struct Harness {
    trigger: Option<stream_cancel::Trigger>,
    pub(super) events: std::pin::Pin<Box<dyn Stream<Item = SourceSenderItem> + Send>>,
    lookups: Option<std::pin::Pin<Box<dyn Stream<Item = SourceSenderItem> + Send + Unpin>>>,
    task: tokio::task::JoinHandle<Result<(), ()>>,
}

fn start(
    queue: Arc<MockQueue>,
    store: Arc<MockStore>,
    settings: Settings,
    cursor: Option<Arc<Cursor>>,
) -> Harness {
    start_with(
        queue,
        store,
        settings,
        cursor,
        Arc::new(Completions::in_memory()),
        10_000,
    )
}

/// As [`start`], with the progress record and the pipeline's buffer size chosen.
pub(super) fn start_with<O: ObjectStore>(
    queue: Arc<MockQueue>,
    store: Arc<O>,
    settings: Settings,
    cursor: Option<Arc<Cursor>>,
    completions: Arc<Completions>,
    capacity: usize,
) -> Harness {
    let (mut out, rx) = SourceSender::new_test_sender_with_options(capacity, None);
    let lookups = settings.route_lookups.then(|| {
        Box::pin(out.add_outputs(EventStatus::Delivered, LOOKUPS_OUTPUT.to_owned()))
            as std::pin::Pin<Box<dyn Stream<Item = SourceSenderItem> + Send + Unpin>>
    });
    let (trigger, shutdown, _done) = ShutdownSignal::new_wired();
    let engine = Engine::new(queue, store, settings, json_decoder(), cursor, completions);
    let task = tokio::spawn(engine.run(out, shutdown));
    Harness {
        trigger: Some(trigger),
        events: rx.into_stream(),
        lookups,
        task,
    }
}

impl Harness {
    /// Takes `n` events from the default output without finalizing them.
    pub(super) async fn take(&mut self, n: usize) -> Vec<Event> {
        let mut events = Vec::new();
        while events.len() < n {
            let item = tokio::time::timeout(Duration::from_hours(1), self.events.next())
                .await
                .expect("events arrive")
                .expect("stream open");
            events.extend(item.events.into_events());
        }
        events
    }

    pub(super) async fn stop(mut self) {
        drop(self.trigger.take());
        drop(tokio::time::timeout(Duration::from_mins(1), self.task).await);
    }
}

pub(super) fn finalize(events: Vec<Event>, status: EventStatus) {
    for mut event in events {
        event.take_finalizers().update_status(status);
    }
}

/// Advances (paused) time until `check` holds.
pub(super) async fn until(what: &str, check: impl Fn() -> bool) {
    for _ in 0..100_000 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

pub(super) fn field<'a>(event: &'a Event, path: &str) -> Option<&'a Value> {
    event.as_log().parse_path_and_get_value(path).ok().flatten()
}

#[tokio::test(start_paused = true)]
async fn deletes_only_after_every_event_is_acknowledged() {
    let a = gzip(&[
        r#"{"event_simpleName":"ProcessRollup2","timestamp":"1"}"#,
        r#"{"n":2}"#,
    ]);
    let b = gzip(&[r#"{"n":3}"#]);
    let body = manifest_body(
        Utc::now().timestamp_millis(),
        &[
            file_entry("cid0/data/u/part-00000.gz", &a),
            file_entry("cid0/data/u/part-00001.gz", &b),
        ],
    );
    let queue = MockQueue::with(vec![message("m-ack", &body)], false);
    let store = MockStore::with(
        vec![
            ("cid0/data/u/part-00000.gz", Object::Data(a)),
            ("cid0/data/u/part-00001.gz", Object::Data(b)),
        ],
        &queue,
    );
    let mut harness = start(Arc::clone(&queue), store, settings(), None);

    let mut events = harness.take(3).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(queue.deletes(), 0, "nothing acknowledged yet");

    // Metadata on every event; the event's own `timestamp` is untouched.
    events.sort_by_key(|event| {
        (
            field(event, "fdr.file").unwrap().to_string(),
            field(event, "fdr.line").unwrap().as_integer().unwrap(),
        )
    });
    let first = &events[0];
    assert_eq!(
        field(first, "fdr.file").unwrap(),
        &Value::from("cid0/data/u/part-00000.gz")
    );
    assert_eq!(field(first, "fdr.line").unwrap(), &Value::from(0));
    assert_eq!(field(first, "fdr.cid").unwrap(), &Value::from("cid0"));
    assert_eq!(
        field(first, "fdr.bucket").unwrap(),
        &Value::from("fdr-alias")
    );
    assert_eq!(field(first, "fdr.kind").unwrap(), &Value::from("data"));
    assert!(field(first, "fdr.batch_timestamp").unwrap().is_timestamp());
    assert_eq!(field(first, "timestamp").unwrap(), &Value::from("1"));
    assert_eq!(field(&events[1], "fdr.line").unwrap(), &Value::from(1));

    // Acknowledge two of three: still held.
    let last = events.pop().unwrap();
    finalize(events, EventStatus::Delivered);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(queue.deletes(), 0, "one event still pending");

    finalize(vec![last], EventStatus::Delivered);
    until("delete", || queue.deletes() == 1).await;
    assert_eq!(
        queue
            .calls()
            .iter()
            .filter(|call| matches!(call, Call::Delete(receipt) if receipt == "m-ack#1"))
            .count(),
        1
    );
    harness.stop().await;
}

#[tokio::test(start_paused = true)]
async fn heartbeats_until_max_hold_then_releases() {
    let a = gzip(&[r#"{"n":1}"#]);
    let body = manifest_body(
        Utc::now().timestamp_millis(),
        &[file_entry("cid0/data/u/part-0.gz", &a)],
    );
    let queue = MockQueue::with(vec![message("m-hold", &body)], false);
    let store = MockStore::with(vec![("cid0/data/u/part-0.gz", Object::Data(a))], &queue);
    let mut config = settings();
    config.max_hold = Duration::from_secs(700);
    let mut harness = start(Arc::clone(&queue), store, config, None);

    let events = harness.take(1).await; // held, never acknowledged
    tokio::time::sleep(Duration::from_secs(1000)).await;

    let heartbeats: Vec<_> = queue
        .timed_calls()
        .into_iter()
        .filter_map(|(at, call)| match call {
            Call::Visibility(_, 300) => Some(at.as_secs()),
            _ => None,
        })
        .collect();
    // Every 150 s (half the visibility timeout) until the 700 s hold ends.
    assert_eq!(heartbeats.len(), 4, "heartbeats at {heartbeats:?}");
    for (index, at) in heartbeats.iter().enumerate() {
        let expected = 150 * (index as u64 + 1);
        assert!(
            (expected..expected + 2).contains(at),
            "heartbeat {index} at {at}s, expected about {expected}s"
        );
    }
    assert_eq!(queue.deletes(), 0, "never deleted without acknowledgement");
    drop(events);
    harness.stop().await;
}

#[tokio::test(start_paused = true)]
async fn sink_failure_releases_and_redelivery_loses_nothing() {
    let a = gzip(&[r#"{"n":1}"#, r#"{"n":2}"#]);
    let body = manifest_body(
        Utc::now().timestamp_millis(),
        &[file_entry("cid0/data/u/part-0.gz", &a)],
    );
    let queue = MockQueue::with(vec![message("m-retry", &body)], true);
    let store = MockStore::with(vec![("cid0/data/u/part-0.gz", Object::Data(a))], &queue);
    let mut harness = start(Arc::clone(&queue), store, settings(), None);

    // Sink outage: the first delivery errors.
    let first = harness.take(2).await;
    finalize(first, EventStatus::Errored);
    until("release", || {
        queue
            .calls()
            .contains(&Call::Visibility("m-retry#1".to_owned(), RETRY_DELAY))
    })
    .await;
    assert_eq!(queue.deletes(), 0);

    // Re-delivered: the same events again, same file and line offsets.
    let second = harness.take(2).await;
    let mut lines: Vec<_> = second
        .iter()
        .map(|event| field(event, "fdr.line").unwrap().as_integer().unwrap())
        .collect();
    lines.sort_unstable();
    assert_eq!(lines, vec![0, 1]);
    finalize(second, EventStatus::Delivered);
    until("delete", || queue.deletes() == 1).await;
    assert!(
        queue
            .calls()
            .contains(&Call::Delete("m-retry#2".to_owned()))
    );
    harness.stop().await;
}

#[tokio::test(start_paused = true)]
async fn stalled_stream_times_out_and_releases() {
    let a = gzip(&[r#"{"n":1}"#]);
    let body = manifest_body(
        Utc::now().timestamp_millis(),
        &[file_entry("cid0/data/u/part-0.gz", &a)],
    );
    let queue = MockQueue::with(vec![message("m-stall", &body)], false);
    let store = MockStore::with(
        vec![("cid0/data/u/part-0.gz", Object::Stall(a.slice(..4)))],
        &queue,
    );
    let harness = start(Arc::clone(&queue), store, settings(), None);

    until("release", || {
        queue
            .calls()
            .contains(&Call::Visibility("m-stall#1".to_owned(), RETRY_DELAY))
    })
    .await;
    let released_at = queue
        .timed_calls()
        .into_iter()
        .find(|(_, call)| matches!(call, Call::Visibility(_, RETRY_DELAY)))
        .unwrap()
        .0;
    assert!(
        (120..125).contains(&released_at.as_secs()),
        "released after the 120 s idle timeout, not {released_at:?}"
    );
    assert_eq!(queue.deletes(), 0);
    harness.stop().await;
}

#[tokio::test(start_paused = true)]
async fn failed_get_releases() {
    let a = gzip(&[r#"{"n":1}"#]);
    let body = manifest_body(
        Utc::now().timestamp_millis(),
        &[file_entry("cid0/data/u/part-0.gz", &a)],
    );
    let queue = MockQueue::with(vec![message("m-404", &body)], false);
    let store = MockStore::with(vec![("cid0/data/u/part-0.gz", Object::Missing)], &queue);
    let harness = start(Arc::clone(&queue), store, settings(), None);
    until("release", || {
        queue
            .calls()
            .contains(&Call::Visibility("m-404#1".to_owned(), RETRY_DELAY))
    })
    .await;
    assert_eq!(queue.deletes(), 0);
    harness.stop().await;
}

async fn run_checksum_case(mode: ChecksumMode, checksum: &str) -> (Arc<MockQueue>, usize) {
    let a = gzip(&[r#"{"n":1}"#]);
    let entry = format!(
        r#"{{"path":"cid0/data/u/part-0.gz","size":{},"checksum":"{checksum}"}}"#,
        a.len()
    );
    let body = manifest_body(Utc::now().timestamp_millis(), &[entry]);
    let id = format!("m-sum-{mode:?}-{}", checksum.len());
    let queue = MockQueue::with(vec![message(&id, &body)], false);
    let store = MockStore::with(vec![("cid0/data/u/part-0.gz", Object::Data(a))], &queue);
    let mut config = settings();
    config.checksum = mode;
    let mut harness = start(Arc::clone(&queue), store, config, None);
    let mut received = 0;
    for _ in 0..50 {
        if let Ok(Some(item)) =
            tokio::time::timeout(Duration::from_secs(1), harness.events.next()).await
        {
            let events: Vec<_> = item.events.into_events().collect();
            received += events.len();
            finalize(events, EventStatus::Delivered);
        }
        if queue.deletes() > 0
            || queue
                .calls()
                .iter()
                .any(|call| matches!(call, Call::Visibility(_, RETRY_DELAY)))
        {
            break;
        }
    }
    harness.stop().await;
    (queue, received)
}

#[tokio::test(start_paused = true)]
async fn checksum_modes() {
    let a = gzip(&[r#"{"n":1}"#]);
    let good_md5 = md5_hex(&a);
    let good_sha = sha256_hex(&a);
    let wrong = md5_hex(b"other");

    for good in [&good_md5, &good_sha] {
        let (queue, received) = run_checksum_case(ChecksumMode::Verify, good).await;
        assert_eq!((queue.deletes(), received), (1, 1), "verify accepts {good}");
    }

    let (queue, received) = run_checksum_case(ChecksumMode::Verify, &wrong).await;
    assert_eq!(
        (queue.deletes(), received),
        (0, 0),
        "verify rejects a mismatch"
    );

    let (queue, received) = run_checksum_case(ChecksumMode::Verify, "abcdef").await;
    assert_eq!(
        (queue.deletes(), received),
        (0, 0),
        "verify fails closed on unknown shape"
    );
    assert!(
        !queue
            .calls()
            .iter()
            .any(|call| matches!(call, Call::Get(_))),
        "fails closed before downloading"
    );

    let (queue, received) = run_checksum_case(ChecksumMode::Warn, &wrong).await;
    assert_eq!((queue.deletes(), received), (1, 1), "warn continues");

    let (queue, received) = run_checksum_case(ChecksumMode::Off, "abcdef").await;
    assert_eq!((queue.deletes(), received), (1, 1), "off ignores checksums");
}

#[tokio::test(start_paused = true)]
async fn cutoff_skips_old_batches_without_download() {
    let a = gzip(&[r#"{"n":1}"#]);
    let old = manifest_body(1_000, &[file_entry("cid0/data/u/old.gz", &a)]);
    let new = manifest_body(
        Utc::now().timestamp_millis(),
        &[file_entry("cid0/data/u/new.gz", &a)],
    );
    let queue = MockQueue::with(vec![message("m-old", &old), message("m-new", &new)], false);
    let store = MockStore::with(
        vec![
            ("cid0/data/u/old.gz", Object::Data(a.clone())),
            ("cid0/data/u/new.gz", Object::Data(a)),
        ],
        &queue,
    );
    let mut config = settings();
    config.cutoff = Some(Utc::now() - chrono::Duration::hours(1));
    let dir = tempfile::tempdir().unwrap();
    let cursor = Arc::new(Cursor::open(dir.path()).unwrap());
    let mut harness = start(Arc::clone(&queue), store, config, Some(Arc::clone(&cursor)));

    let events = harness.take(1).await;
    assert_eq!(
        field(&events[0], "fdr.file").unwrap(),
        &Value::from("cid0/data/u/new.gz")
    );
    finalize(events, EventStatus::Delivered);
    until("both deleted", || queue.deletes() == 2).await;
    assert!(
        !queue
            .calls()
            .contains(&Call::Get("cid0/data/u/old.gz".to_owned()))
    );
    assert!(queue.calls().contains(&Call::Delete("m-old#1".to_owned())));
    // The cursor advanced to the delivered batch only.
    for _ in 0..100 {
        if cursor.get().await.is_some_and(|ms| ms > 1_000) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(cursor.get().await.is_some_and(|ms| ms > 1_000));
    harness.stop().await;
}

#[tokio::test(start_paused = true)]
async fn parks_after_max_tries() {
    let a = gzip(&[r#"{"n":1}"#]);
    let body = manifest_body(
        Utc::now().timestamp_millis(),
        &[file_entry("cid0/data/u/part-0.gz", &a)],
    );
    let mut parked = message("m-parked", &body);
    parked.receive_count = 4;
    let queue = MockQueue::with(vec![parked], false);
    let store = MockStore::with(vec![("cid0/data/u/part-0.gz", Object::Data(a))], &queue);
    let harness = start(Arc::clone(&queue), store, settings(), None);
    tokio::time::sleep(Duration::from_mins(1)).await;
    assert!(queue.calls().is_empty(), "left alone: {:?}", queue.calls());
    harness.stop().await;
}

#[tokio::test(start_paused = true)]
async fn routes_lookup_snapshots_to_lookups_output() {
    let data = gzip(&[r#"{"kind":"event"}"#]);
    let lookup = gzip(&[r#"{"aid":"a1"}"#, r#"{"aid":"a2"}"#]);
    let body = manifest_body(
        Utc::now().timestamp_millis(),
        &[
            file_entry("cid0/data/u/part-0.gz", &data),
            file_entry("cid0/fdrv2-aidmaster-x/part-0.gz", &lookup),
        ],
    );
    let queue = MockQueue::with(vec![message("m-lookup", &body)], false);
    let store = MockStore::with(
        vec![
            ("cid0/data/u/part-0.gz", Object::Data(data)),
            ("cid0/fdrv2-aidmaster-x/part-0.gz", Object::Data(lookup)),
        ],
        &queue,
    );
    let mut harness = start(Arc::clone(&queue), store, settings(), None);

    let events = harness.take(1).await;
    assert_eq!(field(&events[0], "fdr.kind").unwrap(), &Value::from("data"));
    finalize(events, EventStatus::Delivered);

    let mut lookups = harness.lookups.take().unwrap();
    let mut lookup_events = Vec::new();
    while lookup_events.len() < 2 {
        let item = lookups.next().await.unwrap();
        lookup_events.extend(item.events.into_events());
    }
    assert!(
        lookup_events
            .iter()
            .all(|event| { field(event, "fdr.kind") == Some(&Value::from("lookup")) })
    );
    drop(lookup_events);
    until("delete", || queue.deletes() == 1).await;
    harness.stop().await;
}

#[tokio::test(start_paused = true)]
async fn without_acknowledgements_deletes_after_dispatch() {
    let a = gzip(&[r#"{"n":1}"#]);
    let body = manifest_body(
        Utc::now().timestamp_millis(),
        &[file_entry("cid0/data/u/part-0.gz", &a)],
    );
    let queue = MockQueue::with(vec![message("m-noack", &body)], false);
    let store = MockStore::with(vec![("cid0/data/u/part-0.gz", Object::Data(a))], &queue);
    let mut config = settings();
    config.acknowledgements = false;
    let mut harness = start(Arc::clone(&queue), store, config, None);
    let _events = harness.take(1).await;
    until("delete", || queue.deletes() == 1).await;
    harness.stop().await;
}

#[tokio::test(start_paused = true)]
async fn invalid_manifest_is_released() {
    let queue = MockQueue::with(vec![message("m-bad", "{\"Message\":\"s3 test\"}")], false);
    let store = MockStore::with(vec![], &queue);
    let harness = start(Arc::clone(&queue), store, settings(), None);
    until("release", || {
        queue
            .calls()
            .contains(&Call::Visibility("m-bad#1".to_owned(), RETRY_DELAY))
    })
    .await;
    harness.stop().await;
}

#[tokio::test(start_paused = true)]
async fn oversized_object_fails_and_releases() {
    let a = gzip(&[r#"{"n":1}"#, r#"{"n":2}"#]);
    let body = manifest_body(
        Utc::now().timestamp_millis(),
        &[file_entry("cid0/data/u/part-0.gz", &a)],
    );
    let queue = MockQueue::with(vec![message("m-big", &body)], false);
    let store = MockStore::with(vec![("cid0/data/u/part-0.gz", Object::Data(a))], &queue);
    let mut config = settings();
    config.max_object_bytes = 10;
    let harness = start(Arc::clone(&queue), store, config, None);
    until("release", || {
        queue
            .calls()
            .contains(&Call::Visibility("m-big#1".to_owned(), RETRY_DELAY))
    })
    .await;
    assert_eq!(queue.deletes(), 0);
    harness.stop().await;
}

// ---------------------------------------------------------------------------------------------
// False delivery (NAN-5004 class): nothing downstream that fails to land an event may delete the
// message. Each case must release (re-deliver), never `DeleteMessage`.

/// One message, one file of `lines` events; returns the queue, store and a running harness.
fn single_file_case(
    id: &str,
    lines: &[&str],
    redeliver: bool,
    config: Settings,
) -> (Arc<MockQueue>, Arc<MockStore>, Harness) {
    let a = gzip(lines);
    let body = manifest_body(
        Utc::now().timestamp_millis(),
        &[file_entry("cid0/data/u/part-0.gz", &a)],
    );
    let queue = MockQueue::with(vec![message(id, &body)], redeliver);
    let store = MockStore::with(vec![("cid0/data/u/part-0.gz", Object::Data(a))], &queue);
    let harness = start(Arc::clone(&queue), Arc::clone(&store), config, None);
    (queue, store, harness)
}

#[tokio::test(start_paused = true)]
async fn sink_rejection_releases_and_never_deletes() {
    let (queue, _store, mut harness) =
        single_file_case("m-reject", &[r#"{"n":1}"#, r#"{"n":2}"#], false, settings());
    let events = harness.take(2).await;
    finalize(events, EventStatus::Rejected);
    until("release", || {
        queue
            .calls()
            .contains(&Call::Visibility("m-reject#1".to_owned(), RETRY_DELAY))
    })
    .await;
    // Long after: still never deleted.
    tokio::time::sleep(Duration::from_mins(10)).await;
    assert_eq!(
        queue.deletes(),
        0,
        "a rejected batch must not delete the message"
    );
    harness.stop().await;
}

#[tokio::test(start_paused = true)]
async fn sink_rejection_deletes_only_when_delete_failed_message_is_set() {
    let mut config = settings();
    config.delete_failed_message = true;
    let (queue, _store, mut harness) =
        single_file_case("m-reject-del", &[r#"{"n":1}"#], false, config);
    let events = harness.take(1).await;
    finalize(events, EventStatus::Rejected);
    until("delete", || queue.deletes() == 1).await;
    harness.stop().await;
}

#[tokio::test(start_paused = true)]
async fn one_errored_event_among_delivered_ones_releases() {
    let (queue, _store, mut harness) = single_file_case(
        "m-partial",
        &[r#"{"n":1}"#, r#"{"n":2}"#, r#"{"n":3}"#],
        false,
        settings(),
    );
    let mut events = harness.take(3).await;
    let dropped = events.pop().unwrap();
    finalize(events, EventStatus::Delivered);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(queue.deletes(), 0);
    finalize(vec![dropped], EventStatus::Errored);
    until("release", || {
        queue
            .calls()
            .contains(&Call::Visibility("m-partial#1".to_owned(), RETRY_DELAY))
    })
    .await;
    assert_eq!(
        queue.deletes(),
        0,
        "a partially delivered batch must not delete"
    );
    harness.stop().await;
}

#[tokio::test(start_paused = true)]
async fn closed_pipeline_releases_and_never_deletes() {
    let (queue, _store, harness) = single_file_case("m-closed", &[r#"{"n":1}"#], false, settings());
    // The downstream side goes away before anything is read: every send fails.
    let Harness {
        trigger,
        events,
        lookups,
        task,
    } = harness;
    drop(events);
    drop(lookups);
    until("release", || {
        queue
            .calls()
            .iter()
            .any(|call| matches!(call, Call::Visibility(receipt, _) if receipt == "m-closed#1"))
    })
    .await;
    assert_eq!(queue.deletes(), 0, "an undelivered batch must not delete");
    drop(trigger);
    drop(tokio::time::timeout(Duration::from_mins(1), task).await);
}

#[tokio::test(start_paused = true)]
async fn poisoned_file_retries_the_batch_without_loss_or_duplicates() {
    let good = gzip(&[r#"{"f":"a","n":1}"#, r#"{"f":"a","n":2}"#]);
    let fixed = gzip(&[r#"{"f":"b","n":1}"#]);
    // A gzip member cut short: the checksum (none here) passes, decompression fails.
    let poisoned = fixed.slice(..fixed.len() - 6);
    let entry = |path: &str| format!(r#"{{"path":"{path}"}}"#);
    let body = manifest_body(
        Utc::now().timestamp_millis(),
        &[
            entry("cid0/data/u/part-a.gz"),
            entry("cid0/data/u/part-b.gz"),
        ],
    );
    let queue = MockQueue::with(vec![message("m-poison", &body)], true);
    let store = MockStore::with(
        vec![
            ("cid0/data/u/part-a.gz", Object::Data(good)),
            ("cid0/data/u/part-b.gz", Object::Data(poisoned)),
        ],
        &queue,
    );
    let mut config = settings();
    config.checksum = ChecksumMode::Off;
    config.file_concurrency = 1;
    let mut harness = start(Arc::clone(&queue), Arc::clone(&store), config, None);

    // First delivery: part-a is emitted and even acknowledged, part-b fails to decode.
    let first = harness.take(2).await;
    finalize(first, EventStatus::Delivered);
    until("release", || {
        queue
            .calls()
            .contains(&Call::Visibility("m-poison#1".to_owned(), RETRY_DELAY))
    })
    .await;
    assert_eq!(
        queue.deletes(),
        0,
        "a batch with a failed file must not delete"
    );

    // The object is readable on the next delivery. part-a was acknowledged, so only part-b is
    // emitted (no duplicates), then the message deletes.
    store
        .objects
        .lock()
        .unwrap()
        .insert("cid0/data/u/part-b.gz".to_owned(), Object::Data(fixed));
    let second = harness.take(1).await;
    assert_eq!(
        field(&second[0], "fdr.file").unwrap(),
        &Value::from("cid0/data/u/part-b.gz")
    );
    finalize(second, EventStatus::Delivered);
    until("delete", || queue.deletes() == 1).await;
    assert!(
        queue
            .calls()
            .contains(&Call::Delete("m-poison#2".to_owned()))
    );
    assert_eq!(
        queue
            .calls()
            .iter()
            .filter(|call| **call == Call::Get("cid0/data/u/part-a.gz".to_owned()))
            .count(),
        1,
        "part-a is not even downloaded again"
    );
    harness.stop().await;
}

#[test]
fn max_hold_cannot_exceed_the_sqs_visibility_ceiling() {
    let error = config("max_hold_secs = 43201").validate().unwrap_err();
    assert!(error.to_string().contains("max_hold_secs"), "{error}");
    config("max_hold_secs = 43200").validate().unwrap();
}

#[test]
fn virtual_hosted_addressing_is_the_default() {
    // S3 access-point aliases are addressed like buckets; virtual-hosted is the SDK default.
    assert!(!config("").force_path_style);
}

#[tokio::test(start_paused = true)]
async fn denied_queue_depth_stops_asking_and_ingestion_continues() {
    let mut config = settings();
    config.queue_depth_interval = Some(Duration::from_secs(10));
    let (queue, _store, mut harness) = single_file_case("m-depth", &[r#"{"n":1}"#], false, config);
    let events = harness.take(1).await;
    finalize(events, EventStatus::Delivered);
    until("delete", || queue.deletes() == 1).await;
    tokio::time::sleep(Duration::from_mins(5)).await;
    assert_eq!(
        queue.depth_calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "GetQueueAttributes is asked once, then the gauge is disabled"
    );
    harness.stop().await;
}

/// Two one-file manifests; returns whether the second's object was fetched while the first
/// was still held (unacknowledged).
async fn second_fetched_while_first_held(config: Settings) -> bool {
    let a = gzip(&[r#"{"m":1}"#]);
    let b = gzip(&[r#"{"m":2}"#]);
    let now = Utc::now().timestamp_millis();
    let queue = MockQueue::with(
        vec![
            message(
                "m-in1",
                &manifest_body(now, &[file_entry("cid0/data/1/part-0.gz", &a)]),
            ),
            message(
                "m-in2",
                &manifest_body(now, &[file_entry("cid0/data/2/part-0.gz", &b)]),
            ),
        ],
        false,
    );
    let store = MockStore::with(
        vec![
            ("cid0/data/1/part-0.gz", Object::Data(a)),
            ("cid0/data/2/part-0.gz", Object::Data(b)),
        ],
        &queue,
    );
    let mut harness = start(Arc::clone(&queue), store, config, None);
    let first = harness.take(1).await;
    tokio::time::sleep(Duration::from_mins(1)).await;
    let gets = || {
        queue
            .calls()
            .iter()
            .filter(|call| matches!(call, Call::Get(_)))
            .count()
    };
    let concurrent = gets() == 2;
    finalize(first, EventStatus::Delivered);
    let second = harness.take(1).await;
    finalize(second, EventStatus::Delivered);
    until("both deleted", || queue.deletes() == 2).await;
    harness.stop().await;
    concurrent
}

#[tokio::test(start_paused = true)]
async fn inflight_limits_hold_back_further_manifests() {
    // Unbounded enough: both manifests are worked on at once.
    assert!(second_fetched_while_first_held(settings()).await);

    // max_inflight_manifests = 1: the second is not even received until the first deletes.
    let mut config = settings();
    config.max_inflight_manifests = 1;
    assert!(!second_fetched_while_first_held(config).await);

    // max_inflight_bytes = 1 KiB (each manifest rounds up to 1 KiB): the second is received but
    // waits for budget, heartbeating, before any download.
    let mut config = settings();
    config.max_inflight_kib = Some(1);
    assert!(!second_fetched_while_first_held(config).await);
}

// ---------------------------------------------------------------------------------------------
// start_from must never delete an undelivered message across restarts.

/// Polls the cursor (paused time) until `check` holds.
async fn until_cursor(cursor: &Cursor, what: &str, check: impl Fn(Option<i64>) -> bool) {
    for _ in 0..1000 {
        if check(cursor.get().await) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("cursor never reached {what}: {:?}", cursor.get().await);
}

#[tokio::test(start_paused = true)]
async fn cutoff_spares_redelivered_and_unknown_count_messages() {
    let a = gzip(&[r#"{"n":1}"#]);
    let b = gzip(&[r#"{"n":2}"#]);
    // Both batches are older than the cutoff, but neither is a first delivery.
    let mut redelivered = message(
        "m-redelivered",
        &manifest_body(1_000, &[file_entry("cid0/data/r/part-0.gz", &a)]),
    );
    redelivered.receive_count = 2;
    let mut unknown = message(
        "m-unknown",
        &manifest_body(1_000, &[file_entry("cid0/data/k/part-0.gz", &b)]),
    );
    unknown.receive_count = 0;
    let queue = MockQueue::with(vec![redelivered, unknown], false);
    let store = MockStore::with(
        vec![
            ("cid0/data/r/part-0.gz", Object::Data(a)),
            ("cid0/data/k/part-0.gz", Object::Data(b)),
        ],
        &queue,
    );
    let mut config = settings();
    config.cutoff = Some(Utc::now());
    let mut harness = start(Arc::clone(&queue), store, config, None);

    let events = harness.take(2).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(queue.deletes(), 0, "processed and held, not skipped");
    finalize(events, EventStatus::Delivered);
    until("both delivered and deleted", || queue.deletes() == 2).await;
    harness.stop().await;
}

#[tokio::test(start_paused = true)]
async fn restart_in_now_mode_deletes_nothing_in_flight() {
    let a = gzip(&[r#"{"n":1}"#, r#"{"n":2}"#]);
    let batch_ms = Utc::now().timestamp_millis();
    let body = manifest_body(batch_ms, &[file_entry("cid0/data/u/part-0.gz", &a)]);

    // Run 1: started before the batch; the batch is in flight (unacknowledged) at shutdown.
    let queue = MockQueue::with(vec![message("m-restart", &body)], false);
    let store = MockStore::with(
        vec![("cid0/data/u/part-0.gz", Object::Data(a.clone()))],
        &queue,
    );
    let mut config = settings();
    config.cutoff = Some(Utc::now() - chrono::Duration::hours(1));
    let mut harness = start(Arc::clone(&queue), store, config, None);
    let held = harness.take(2).await;
    harness.stop().await;
    drop(held);
    // Fully emitted and waiting for acknowledgements when Vector stopped: never deleted; its
    // visibility lapses and SQS re-delivers it.
    assert_eq!(
        queue.deletes(),
        0,
        "a restart never deletes an in-flight batch"
    );

    // Run 2: a later start whose cutoff (worst case: not pinned) is past the batch. SQS
    // re-delivers the released message with receive count 2: it is processed, not skipped.
    let mut again = message("m-restart", &body);
    again.receipt = "m-restart#2".to_owned();
    again.receive_count = 2;
    let queue = MockQueue::with(vec![again], false);
    let store = MockStore::with(vec![("cid0/data/u/part-0.gz", Object::Data(a))], &queue);
    let mut config = settings();
    config.cutoff = Some(Utc::now() + chrono::Duration::hours(1));
    let mut harness = start(Arc::clone(&queue), store, config, None);
    let events = harness.take(2).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(queue.deletes(), 0);
    finalize(events, EventStatus::Delivered);
    until("delete after delivery", || queue.deletes() == 1).await;
    assert!(
        queue
            .calls()
            .contains(&Call::Delete("m-restart#2".to_owned()))
    );
    harness.stop().await;
}

#[test]
fn now_cutoff_is_pinned_to_the_first_start() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        super::cursor::pinned_start(dir.path(), 1_000).unwrap(),
        1_000
    );
    // A restart later still uses the first start.
    assert_eq!(
        super::cursor::pinned_start(dir.path(), 9_000).unwrap(),
        1_000
    );
}

#[tokio::test(start_paused = true)]
async fn cursor_never_passes_a_held_or_parked_batch() {
    let older = gzip(&[r#"{"b":"older"}"#]);
    let newer = gzip(&[r#"{"b":"newer"}"#]);
    let parked_ms = 3_000_000_000_000;
    let older_ms = 4_000_000_000_000;
    let newer_ms = 5_000_000_000_000;
    let mut parked = message(
        "m-pin-parked",
        &manifest_body(parked_ms, &[file_entry("cid0/data/p/part-0.gz", &older)]),
    );
    parked.receive_count = 4;
    let queue = MockQueue::with(
        vec![
            parked,
            message(
                "m-pin-older",
                &manifest_body(older_ms, &[file_entry("cid0/data/o/part-0.gz", &older)]),
            ),
            message(
                "m-pin-newer",
                &manifest_body(newer_ms, &[file_entry("cid0/data/n/part-0.gz", &newer)]),
            ),
        ],
        false,
    );
    let store = MockStore::with(
        vec![
            ("cid0/data/o/part-0.gz", Object::Data(older)),
            ("cid0/data/n/part-0.gz", Object::Data(newer)),
        ],
        &queue,
    );
    let dir = tempfile::tempdir().unwrap();
    let cursor = Arc::new(Cursor::open(dir.path()).unwrap());
    let mut harness = start(
        Arc::clone(&queue),
        store,
        settings(),
        Some(Arc::clone(&cursor)),
    );

    let events = harness.take(2).await;
    let (newer_events, older_events): (Vec<_>, Vec<_>) = events
        .into_iter()
        .partition(|event| field(event, "b") == Some(&Value::from("newer")));
    // The newer batch is delivered while the older one is still held: the cursor stops just
    // before the oldest pinned batch (the parked one), not at the newer batch.
    finalize(newer_events, EventStatus::Delivered);
    until("newer deleted", || queue.deletes() == 1).await;
    until_cursor(&cursor, "parked - 1", |ms| ms == Some(parked_ms - 1)).await;

    // Delivering the older batch does not pass the parked one either.
    finalize(older_events, EventStatus::Delivered);
    until("older deleted", || queue.deletes() == 2).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(cursor.get().await, Some(parked_ms - 1));
    harness.stop().await;
}

#[tokio::test(start_paused = true)]
async fn cursor_stops_before_a_held_batch_then_catches_up() {
    let older = gzip(&[r#"{"b":"older"}"#]);
    let newer = gzip(&[r#"{"b":"newer"}"#]);
    let older_ms = 4_000_000_000_000;
    let newer_ms = 5_000_000_000_000;
    let queue = MockQueue::with(
        vec![
            message(
                "m-held-older",
                &manifest_body(older_ms, &[file_entry("cid0/data/o/part-0.gz", &older)]),
            ),
            message(
                "m-held-newer",
                &manifest_body(newer_ms, &[file_entry("cid0/data/n/part-0.gz", &newer)]),
            ),
        ],
        false,
    );
    let store = MockStore::with(
        vec![
            ("cid0/data/o/part-0.gz", Object::Data(older)),
            ("cid0/data/n/part-0.gz", Object::Data(newer)),
        ],
        &queue,
    );
    let dir = tempfile::tempdir().unwrap();
    let cursor = Arc::new(Cursor::open(dir.path()).unwrap());
    let mut harness = start(
        Arc::clone(&queue),
        store,
        settings(),
        Some(Arc::clone(&cursor)),
    );
    let events = harness.take(2).await;
    let (newer_events, older_events): (Vec<_>, Vec<_>) = events
        .into_iter()
        .partition(|event| field(event, "b") == Some(&Value::from("newer")));
    finalize(newer_events, EventStatus::Delivered);
    until("newer deleted", || queue.deletes() == 1).await;
    until_cursor(&cursor, "older - 1", |ms| ms == Some(older_ms - 1)).await;

    finalize(older_events, EventStatus::Delivered);
    until("older deleted", || queue.deletes() == 2).await;
    // Nothing is pinned any more; the cursor reaches the older batch (it only moves forward,
    // so the newer batch's time is reached by the next delivery after it).
    until_cursor(&cursor, "older", |ms| ms == Some(older_ms)).await;
    harness.stop().await;
}
