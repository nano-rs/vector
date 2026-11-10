//! Full fetch before emit, per-object retries and per-batch progress (NAN-5005 follow-up).
//!
//! The field failure: a GET for file 16 of a batch was cut mid-body ("connection closed before
//! message completed"); the batch was released and its re-delivery duplicated files 0-15. These
//! cases cut downloads mid-body and check that no event is ever emitted twice.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, atomic::AtomicUsize},
    time::Duration,
};

use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpListener,
};
use vector_lib::event::{EventContainer, EventStatus};
use vrl::value::Value;

use super::{
    aws::S3Store,
    completion::{Completions, batch_key},
    manifest::{md5_hex, parse_manifest},
    source::Settings,
    tests::{
        Call, Harness, MockQueue, MockStore, Object, RETRY_DELAY, field, file_entry, finalize,
        gzip, manifest_body, message, settings, start_with, until,
    },
};
use crate::{
    aws::{AwsAuthentication, create_client},
    common::s3::S3ClientBuilder,
    config::ProxyConfig,
    event::Event,
};

/// `files` objects of `lines` events each; every event carries its file and line index.
pub(super) fn objects(files: usize, lines: usize) -> Vec<(String, Bytes)> {
    (0..files)
        .map(|file| {
            let lines: Vec<String> = (0..lines)
                .map(|line| format!(r#"{{"f":{file},"l":{line}}}"#))
                .collect();
            let refs: Vec<&str> = lines.iter().map(String::as_str).collect();
            (format!("cid0/data/u/part-{file:05}.gz"), gzip(&refs))
        })
        .collect()
}

pub(super) fn body_for(objects: &[(String, Bytes)]) -> String {
    let entries: Vec<String> = objects
        .iter()
        .map(|(key, data)| file_entry(key, data))
        .collect();
    manifest_body(Utc::now().timestamp_millis(), &entries)
}

pub(super) fn gets(queue: &MockQueue, key: &str) -> usize {
    queue
        .calls()
        .iter()
        .filter(|call| **call == Call::Get(key.to_owned()))
        .count()
}

pub(super) fn released(queue: &MockQueue, receipt: &str) -> bool {
    queue
        .calls()
        .contains(&Call::Visibility(receipt.to_owned(), RETRY_DELAY))
}

/// (file, line) of an event.
pub(super) fn position(event: &Event) -> (i64, i64) {
    let int = |path| field(event, path).and_then(Value::as_integer).unwrap();
    (int("f"), int("l"))
}

/// Acknowledges events as they arrive (with `status`) until `want` distinct ones were seen;
/// returns how often each was seen.
pub(super) async fn drain(
    harness: &mut Harness,
    want: usize,
    status: EventStatus,
) -> HashMap<(i64, i64), usize> {
    let mut seen: HashMap<(i64, i64), usize> = HashMap::new();
    while seen.len() < want {
        let item = tokio::time::timeout(Duration::from_hours(1), harness.events.next())
            .await
            .expect("events arrive")
            .expect("stream open");
        let events: Vec<Event> = item.events.into_events().collect();
        for event in &events {
            *seen.entry(position(event)).or_default() += 1;
        }
        finalize(events, status);
    }
    seen
}

pub(super) fn assert_each_once(seen: &HashMap<(i64, i64), usize>) {
    let duplicated: Vec<_> = seen.iter().filter(|(_, count)| **count > 1).collect();
    assert!(duplicated.is_empty(), "duplicated events: {duplicated:?}");
}

pub(super) fn mock_store(
    objects: &[(String, Bytes)],
    queue: &Arc<MockQueue>,
    cut: Option<(usize, usize)>,
) -> Arc<MockStore> {
    let entries: Vec<(&str, Object)> = objects
        .iter()
        .enumerate()
        .map(|(index, (key, data))| {
            let object = match cut {
                Some((file, cuts)) if file == index => Object::Cut {
                    data: data.clone(),
                    at: data.len() / 2,
                    cuts: Arc::new(AtomicUsize::new(cuts)),
                },
                _ => Object::Data(data.clone()),
            };
            (key.as_str(), object)
        })
        .collect();
    MockStore::with(entries, queue)
}

/// Twenty files, file 16 cut mid-body twice: the third try succeeds inside the hold. Nothing
/// is released and every event is emitted exactly once.
#[tokio::test(start_paused = true)]
async fn cut_download_is_retried_inside_the_hold_without_duplicates() {
    let objects = objects(20, 3);
    let queue = MockQueue::with(vec![message("m-cut", &body_for(&objects))], true);
    let store = mock_store(&objects, &queue, Some((16, 2)));
    let mut config = settings();
    config.fetch_max_tries = 3;
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        config,
        None,
        Arc::new(Completions::in_memory()),
        10_000,
    );

    let seen = drain(&mut harness, 60, EventStatus::Delivered).await;
    until("delete", || queue.deletes() == 1).await;
    assert_each_once(&seen);
    assert!(!released(&queue, "m-cut#1"), "never released");
    assert_eq!(
        gets(&queue, &objects[16].0),
        3,
        "two cut tries, then success"
    );
    for (key, _) in objects.iter().filter(|(key, _)| *key != objects[16].0) {
        assert_eq!(gets(&queue, key), 1, "{key} fetched once");
    }
    harness.stop().await;
}

/// File 16 fails every try of the first delivery: the batch is released only after files 0-15
/// are acknowledged, and the re-delivery emits (and fetches) only what was not delivered.
#[tokio::test(start_paused = true)]
async fn exhausted_tries_release_and_the_redelivery_skips_delivered_files() {
    let objects = objects(20, 3);
    let queue = MockQueue::with(vec![message("m-exhaust", &body_for(&objects))], true);
    let store = mock_store(&objects, &queue, Some((16, 3)));
    let mut config = settings();
    config.fetch_max_tries = 3;
    config.file_concurrency = 1;
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        config,
        None,
        Arc::new(Completions::in_memory()),
        10_000,
    );

    let seen = drain(&mut harness, 60, EventStatus::Delivered).await;
    until("delete", || queue.deletes() == 1).await;
    assert_each_once(&seen);
    assert!(released(&queue, "m-exhaust#1"), "first delivery released");
    assert!(
        queue
            .calls()
            .contains(&Call::Delete("m-exhaust#2".to_owned()))
    );
    for (index, (key, _)) in objects.iter().enumerate() {
        let want = match index {
            16 => 4, // three cut tries, then the re-delivery's good one
            _ => 1,
        };
        assert_eq!(gets(&queue, key), want, "GETs of {key}");
    }
    harness.stop().await;
}

/// A file counts as delivered only once its own events are acknowledged: the release waits for
/// the files already emitted, and one whose events errored is emitted again.
#[tokio::test(start_paused = true)]
async fn a_file_is_recorded_only_after_its_events_are_acknowledged() {
    let objects = objects(3, 1);
    let body = body_for(&objects);
    let key = batch_key(&parse_manifest(&body).unwrap());
    let queue = MockQueue::with(vec![message("m-ackrec", &body)], true);
    let store = mock_store(&objects, &queue, Some((2, 2)));
    let completions = Arc::new(Completions::in_memory());
    let mut config = settings();
    config.fetch_max_tries = 2;
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        config,
        None,
        Arc::clone(&completions),
        10_000,
    );

    let mut first = harness.take(2).await;
    first.sort_by_key(position);
    tokio::time::sleep(Duration::from_mins(5)).await;
    assert!(
        !released(&queue, "m-ackrec#1"),
        "the release waits for the emitted files' acknowledgements"
    );
    assert!(completions.load(&key).await.is_empty(), "nothing acked yet");

    let file1 = first.pop().unwrap();
    finalize(first, EventStatus::Delivered); // file 0
    finalize(vec![file1], EventStatus::Errored); // file 1
    until("release", || released(&queue, "m-ackrec#1")).await;
    let done = completions.load(&key).await;
    assert_eq!(done.len(), 1);
    assert!(done.contains(&objects[0].0));

    // Re-delivery: file 0 is skipped; file 1 (errored) and file 2 are emitted.
    let mut second = harness.take(2).await;
    second.sort_by_key(position);
    let positions: Vec<_> = second.iter().map(position).collect();
    assert_eq!(positions, vec![(1, 0), (2, 0)]);
    finalize(second, EventStatus::Delivered);
    until("delete", || queue.deletes() == 1).await;
    assert_eq!(gets(&queue, &objects[0].0), 1);
    // Forgotten once the message is deleted.
    for _ in 0..100 {
        if completions.load(&key).await.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        completions.load(&key).await.is_empty(),
        "forgotten after delete"
    );
    harness.stop().await;
}

/// With `checksum = verify`, an object whose digest does not match emits nothing at all, even
/// though it decodes fine, and is tried again before the batch is released.
#[tokio::test(start_paused = true)]
async fn a_checksum_mismatch_emits_nothing_from_that_object() {
    let good = gzip(&[r#"{"f":0,"l":0}"#]);
    let lines: Vec<String> = (0..5000).map(|l| format!(r#"{{"f":1,"l":{l}}}"#)).collect();
    let refs: Vec<&str> = lines.iter().map(String::as_str).collect();
    let tampered = gzip(&refs);
    let entries = [
        file_entry("cid0/data/u/good.gz", &good),
        format!(
            r#"{{"path":"cid0/data/u/tampered.gz","size":{},"checksum":"{}"}}"#,
            tampered.len(),
            md5_hex(b"what the manifest promised")
        ),
    ];
    let body = manifest_body(Utc::now().timestamp_millis(), &entries);
    let queue = MockQueue::with(vec![message("m-tamper", &body)], false);
    let store = MockStore::with(
        vec![
            ("cid0/data/u/good.gz", Object::Data(good)),
            ("cid0/data/u/tampered.gz", Object::Data(tampered)),
        ],
        &queue,
    );
    let mut config = settings();
    config.fetch_max_tries = 2;
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        config,
        None,
        Arc::new(Completions::in_memory()),
        10_000,
    );

    let mut received = Vec::new();
    while !released(&queue, "m-tamper#1") {
        if let Ok(Some(item)) =
            tokio::time::timeout(Duration::from_secs(1), harness.events.next()).await
        {
            let events: Vec<Event> = item.events.into_events().collect();
            received.extend(events.iter().map(position));
            finalize(events, EventStatus::Delivered);
        }
    }
    assert_eq!(received, vec![(0, 0)], "only the good object's event");
    assert_eq!(gets(&queue, "cid0/data/u/tampered.gz"), 2, "tried again");
    assert_eq!(queue.deletes(), 0);
    harness.stop().await;
}

/// Objects past `spool_threshold_bytes` go to a spool file, not memory, whether the manifest
/// lists their size (spooled from the start) or not (spilled mid-download); the files are
/// removed once emitted.
#[tokio::test]
async fn large_objects_spool_to_disk_and_are_removed_after_emission() {
    let spool = tempfile::tempdir().unwrap();
    let lines: Vec<String> = (0..4000u32)
        .map(|l| format!(r#"{{"f":0,"l":{l},"pad":"{}"}}"#, md5_hex(&l.to_le_bytes())))
        .collect();
    let refs: Vec<&str> = lines.iter().map(String::as_str).collect();
    let sized = gzip(&refs);
    let lines: Vec<String> = (0..4000u32)
        .map(|l| {
            format!(
                r#"{{"f":1,"l":{l},"pad":"{}"}}"#,
                md5_hex(&(l + 7).to_le_bytes())
            )
        })
        .collect();
    let refs: Vec<&str> = lines.iter().map(String::as_str).collect();
    let unsized_ = gzip(&refs);
    assert!(sized.len() > 16 * 1024 && unsized_.len() > 16 * 1024);
    let entries = [
        file_entry("cid0/data/u/sized.gz", &sized),
        format!(
            r#"{{"path":"cid0/data/u/unsized.gz","checksum":"{}"}}"#,
            md5_hex(&unsized_)
        ),
    ];
    let body = manifest_body(Utc::now().timestamp_millis(), &entries);
    let queue = MockQueue::with(vec![message("m-spool", &body)], false);
    let store = MockStore::with(
        vec![
            ("cid0/data/u/sized.gz", Object::Data(sized.clone())),
            ("cid0/data/u/unsized.gz", Object::Data(unsized_.clone())),
        ],
        &queue,
    );
    let mut config = settings();
    config.spool_threshold_bytes = 4 * 1024;
    config.spool_dir = spool.path().to_owned();
    config.max_object_bytes = 4 * 1024 * 1024;
    config.max_line_bytes = 4096;
    // A small pipeline buffer: emission stalls mid-object while both objects are on disk.
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        config,
        None,
        Arc::new(Completions::in_memory()),
        100,
    );

    let spooled = || -> Vec<u64> {
        let mut lengths: Vec<u64> = std::fs::read_dir(spool.path())
            .unwrap()
            .flatten()
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "part"))
            .map(|entry| entry.metadata().unwrap().len())
            .collect();
        lengths.sort_unstable();
        lengths
    };
    let first = harness.take(1).await;
    let mut want = vec![sized.len() as u64, unsized_.len() as u64];
    want.sort_unstable();
    for _ in 0..200 {
        if spooled() == want {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(spooled(), want, "both objects complete on disk");
    let mut all: HashMap<(i64, i64), usize> = HashMap::new();
    for event in &first {
        *all.entry(position(event)).or_default() += 1;
    }
    let taken = first.len();
    finalize(first, EventStatus::Delivered);

    let seen = drain(&mut harness, 8000 - taken, EventStatus::Delivered).await;
    for (position, count) in seen {
        *all.entry(position).or_default() += count;
    }
    assert_eq!(all.len(), 8000);
    assert_each_once(&all);
    until("delete", || queue.deletes() == 1).await;
    assert!(spooled().is_empty(), "spool files removed after emission");
    harness.stop().await;
}

#[tokio::test]
async fn progress_persists_and_is_forgotten() {
    let dir = tempfile::tempdir().unwrap();
    let completions = Completions::open(dir.path()).unwrap();
    completions.mark("batch", "a/part-0.gz").await.unwrap();
    completions.mark("batch", "a/part-1.gz").await.unwrap();
    // A torn append (crash mid-write) is ignored.
    let path = dir
        .path()
        .join(super::completion::DIR_NAME)
        .join("batch.done");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend_from_slice(b"\"a/part-2");
    std::fs::write(&path, bytes).unwrap();

    let reopened = Completions::open(dir.path()).unwrap();
    let done = reopened.load("batch").await;
    assert_eq!(done.len(), 2);
    assert!(done.contains("a/part-0.gz") && done.contains("a/part-1.gz"));
    assert!(reopened.load("other").await.is_empty());

    // The next append after a torn line starts a fresh line, so it is not lost with it.
    reopened.mark("batch", "a/part-3.gz").await.unwrap();
    let done = Completions::open(dir.path()).unwrap().load("batch").await;
    assert_eq!(done.len(), 3, "{done:?}");
    assert!(done.contains("a/part-3.gz"));

    reopened.forget("batch");
    assert!(reopened.load("batch").await.is_empty());
    assert!(!path.exists());

    // Expired records are swept.
    reopened.mark("old", "x").await.unwrap();
    reopened.sweep(Duration::ZERO);
    assert!(reopened.load("old").await.contains("x"), "memory survives");
    assert!(
        Completions::open(dir.path())
            .unwrap()
            .load("old")
            .await
            .is_empty()
    );
}

#[test]
fn batch_key_is_stable_and_distinct() {
    let one = r#"{"cid":"c","timestamp":1,"bucket":"b","files":[{"path":"p/0"},{"path":"p/1"}]}"#;
    let same = r#"{"cid":"c","timestamp":"1","bucket":"b","totalSize":9,"files":[{"path":"p/0","size":4},{"path":"p/1"}]}"#;
    let other = r#"{"cid":"c","timestamp":2,"bucket":"b","files":[{"path":"p/0"},{"path":"p/1"}]}"#;
    let key = |body| batch_key(&parse_manifest(body).unwrap());
    assert_eq!(key(one), key(same));
    assert_ne!(key(one), key(other));
}

/// A restart between deliveries: the progress record in the data dir still skips the files
/// the first run delivered.
#[tokio::test]
async fn a_restart_still_skips_delivered_files() {
    let data_dir = tempfile::tempdir().unwrap();
    let objects = objects(3, 2);
    let body = body_for(&objects);

    // Run 1: files 0 and 1 delivered, file 2 cut on every try; released.
    let queue = MockQueue::with(vec![message("m-restart-skip", &body)], false);
    let store = mock_store(&objects, &queue, Some((2, 1)));
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        settings(),
        None,
        Arc::new(Completions::open(data_dir.path()).unwrap()),
        10_000,
    );
    let seen = drain(&mut harness, 4, EventStatus::Delivered).await;
    assert_each_once(&seen);
    until("release", || released(&queue, "m-restart-skip#1")).await;
    harness.stop().await;

    // Run 2, a new process: SQS re-delivers; only file 2 is fetched and emitted.
    let mut again = message("m-restart-skip", &body);
    again.receipt = "m-restart-skip#2".to_owned();
    again.receive_count = 2;
    let queue = MockQueue::with(vec![again], false);
    let store = mock_store(&objects, &queue, None);
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        settings(),
        None,
        Arc::new(Completions::open(data_dir.path()).unwrap()),
        10_000,
    );
    let seen = drain(&mut harness, 2, EventStatus::Delivered).await;
    assert!(seen.keys().all(|(file, _)| *file == 2), "{seen:?}");
    until("delete", || queue.deletes() == 1).await;
    assert_eq!(gets(&queue, &objects[0].0), 0);
    assert_eq!(gets(&queue, &objects[1].0), 0);
    let records = data_dir.path().join(super::completion::DIR_NAME);
    until("record removed", || {
        std::fs::read_dir(&records).unwrap().next().is_none()
    })
    .await;
    harness.stop().await;
}

// ---------------------------------------------------------------------------------------------
// The real S3 client against an S3 endpoint that closes the connection mid-body.

/// A path-style S3 endpoint serving `objects`. While a key has cuts left, its GET sends the
/// headers (full `Content-Length`) and half the body, then closes the connection, as S3 did
/// in the field ("connection closed before message completed").
pub(super) struct CuttingS3 {
    pub(super) endpoint: String,
    pub(super) gets: Arc<Mutex<Vec<String>>>,
}

pub(super) async fn cutting_s3(
    objects: HashMap<String, Bytes>,
    cuts: HashMap<String, usize>,
) -> CuttingS3 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let gets = Arc::new(Mutex::new(Vec::new()));
    let objects = Arc::new(objects);
    let cuts = Arc::new(Mutex::new(cuts));
    let log = Arc::clone(&gets);
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let objects = Arc::clone(&objects);
            let cuts = Arc::clone(&cuts);
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0u8; 4096];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                }
                let request = String::from_utf8_lossy(&request);
                let target = request.split_whitespace().nth(1).unwrap_or("/");
                let path = target.split('?').next().unwrap_or("");
                // "/<bucket>/<key>"
                let key = path
                    .trim_start_matches('/')
                    .split_once('/')
                    .map_or("", |(_, key)| key)
                    .to_owned();
                log.lock().unwrap().push(key.clone());
                let Some(data) = objects.get(&key) else {
                    drop(
                        socket
                            .write_all(
                                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                            )
                            .await,
                    );
                    return;
                };
                let cut = {
                    let mut cuts = cuts.lock().unwrap();
                    match cuts.get_mut(&key) {
                        Some(left) if *left > 0 => {
                            *left -= 1;
                            true
                        }
                        _ => false,
                    }
                };
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/gzip\r\nConnection: close\r\n\r\n",
                    data.len()
                );
                drop(socket.write_all(head.as_bytes()).await);
                let sent = if cut {
                    &data[..data.len() / 2]
                } else {
                    &data[..]
                };
                drop(socket.write_all(sent).await);
                drop(socket.flush().await);
                drop(socket.shutdown().await);
            });
        }
    });
    CuttingS3 { endpoint, gets }
}

pub(super) async fn s3_store(endpoint: String) -> S3Store {
    S3Store {
        client: create_client::<S3ClientBuilder>(
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
        .unwrap(),
    }
}

fn real_time_settings() -> Settings {
    let mut config = settings();
    config.fetch_max_tries = 3;
    config.fetch_retry_backoff = Duration::from_millis(50);
    config.call_timeout = Duration::from_secs(10);
    config.stream_idle_timeout = Duration::from_secs(5);
    config
}

/// The field failure end to end through the AWS SDK: file 16 of 20 is cut mid-body once. The
/// SDK reports the closed connection, the source tries again, and every event lands once.
#[tokio::test]
async fn s3_connection_closed_mid_body_is_retried_without_duplicates() {
    let objects = objects(20, 3);
    let s3 = cutting_s3(
        objects.iter().cloned().collect(),
        HashMap::from([(objects[16].0.clone(), 1)]),
    )
    .await;
    let queue = MockQueue::with(vec![message("m-s3-cut", &body_for(&objects))], true);
    let mut harness = start_with(
        Arc::clone(&queue),
        Arc::new(s3_store(s3.endpoint.clone()).await),
        real_time_settings(),
        None,
        Arc::new(Completions::in_memory()),
        10_000,
    );

    let seen = drain(&mut harness, 60, EventStatus::Delivered).await;
    until("delete", || queue.deletes() == 1).await;
    assert_each_once(&seen);
    assert!(!released(&queue, "m-s3-cut#1"));
    let gets = s3.gets.lock().unwrap().clone();
    assert_eq!(
        gets.iter().filter(|key| **key == objects[16].0).count(),
        2,
        "cut once, then fetched whole: {gets:?}"
    );
    assert_eq!(gets.len(), 21);
    harness.stop().await;
}

/// Through the AWS SDK, file 16 is cut on every try of the first delivery: the batch is
/// released, and the re-delivery fetches and emits only files 16-19.
#[tokio::test]
async fn s3_cut_past_max_tries_redelivers_only_the_undelivered_files() {
    let objects = objects(20, 3);
    let s3 = cutting_s3(
        objects.iter().cloned().collect(),
        HashMap::from([(objects[16].0.clone(), 3)]),
    )
    .await;
    let queue = MockQueue::with(vec![message("m-s3-exhaust", &body_for(&objects))], true);
    let mut config = real_time_settings();
    config.file_concurrency = 1;
    let mut harness = start_with(
        Arc::clone(&queue),
        Arc::new(s3_store(s3.endpoint.clone()).await),
        config,
        None,
        Arc::new(Completions::in_memory()),
        10_000,
    );

    // The release makes the message visible again after RETRY_DELAY (30 s) in production; the
    // mock queue re-delivers at once.
    let seen = drain(&mut harness, 60, EventStatus::Delivered).await;
    until("delete", || queue.deletes() == 1).await;
    assert_each_once(&seen);
    assert!(released(&queue, "m-s3-exhaust#1"));
    let gets = s3.gets.lock().unwrap().clone();
    for (index, (key, _)) in objects.iter().enumerate() {
        let count = gets.iter().filter(|got| *got == key).count();
        assert_eq!(count, if index == 16 { 4 } else { 1 }, "GETs of {key}");
    }
    harness.stop().await;
}

// ---------------------------------------------------------------------------------------------
// Review follow-ups (L1-L5).

/// L1: acknowledgements that arrive after `max_hold_secs` are still recorded, so the
/// re-delivery skips those files.
#[tokio::test(start_paused = true)]
async fn late_acks_after_the_hold_expired_are_still_recorded() {
    let objects = objects(2, 1);
    let body = body_for(&objects);
    let key = batch_key(&parse_manifest(&body).unwrap());
    let queue = MockQueue::with(vec![message("m-late", &body)], false);
    let store = mock_store(&objects, &queue, None);
    let completions = Arc::new(Completions::in_memory());
    let mut config = settings();
    config.max_hold = Duration::from_secs(700);
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        config,
        None,
        Arc::clone(&completions),
        10_000,
    );

    let mut held = harness.take(2).await;
    held.sort_by_key(position);
    tokio::time::sleep(Duration::from_secs(1000)).await; // the hold expired
    assert_eq!(queue.deletes(), 0);
    let file1 = held.pop().unwrap();
    finalize(held, EventStatus::Delivered); // file 0, late
    for _ in 0..100 {
        if !completions.load(&key).await.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let done = completions.load(&key).await;
    assert!(done.contains(&objects[0].0), "late ack recorded: {done:?}");
    // File 1's sink fails, late too: not recorded. (A dropped event counts as delivered.)
    finalize(vec![file1], EventStatus::Errored);

    // SQS re-delivers after the visibility timeout: only file 1 is emitted.
    let mut again = message("m-late", &body);
    again.receipt = "m-late#2".to_owned();
    again.receive_count = 2;
    queue.inbox.lock().unwrap().push_back(again);
    let second = harness.take(1).await;
    assert_eq!(
        second.iter().map(position).collect::<Vec<_>>(),
        vec![(1, 0)]
    );
    finalize(second, EventStatus::Delivered);
    until("delete", || queue.deletes() == 1).await;
    assert_eq!(gets(&queue, &objects[0].0), 1);
    harness.stop().await;
}

/// L2: `forget` while another holder of the same batch is active waits for it.
#[tokio::test]
async fn forget_waits_for_every_holder_of_the_batch() {
    let dir = tempfile::tempdir().unwrap();
    let completions = Arc::new(Completions::open(dir.path()).unwrap());
    let path = dir
        .path()
        .join(super::completion::DIR_NAME)
        .join("batch.done");

    let first = completions.begin("batch");
    let second = completions.begin("batch");
    completions.mark("batch", "p/0").await.unwrap();
    completions.forget("batch"); // the first copy finished
    drop(first);
    assert!(completions.load("batch").await.contains("p/0"));
    completions.mark("batch", "p/1").await.unwrap(); // the other copy goes on
    assert_eq!(completions.load("batch").await.len(), 2);
    assert!(path.exists());
    drop(second);
    assert!(!completions.is_active("batch"));
    assert!(completions.load("batch").await.is_empty());
    assert!(!path.exists(), "no orphan record");

    // Without holders, forget is immediate.
    completions.mark("solo", "p/0").await.unwrap();
    completions.forget("solo");
    assert!(completions.load("solo").await.is_empty());
}

/// L2 through the engine: the same batch arrives as two messages processed at once. The first
/// to finish must not remove the record under the second, and nothing is left behind once
/// both are deleted.
#[tokio::test]
async fn two_copies_of_a_batch_leave_no_orphan_record() {
    let data_dir = tempfile::tempdir().unwrap();
    let objects = objects(1, 1);
    let body = body_for(&objects);
    let key = batch_key(&parse_manifest(&body).unwrap());
    let queue = MockQueue::with(
        vec![message("m-copy-a", &body), message("m-copy-b", &body)],
        false,
    );
    let store = mock_store(&objects, &queue, None);
    let completions = Arc::new(Completions::open(data_dir.path()).unwrap());
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        settings(),
        None,
        Arc::clone(&completions),
        10_000,
    );

    let mut both = harness.take(2).await;
    let second = both.pop().unwrap();
    finalize(both, EventStatus::Delivered);
    until("first copy deleted", || queue.deletes() == 1).await;
    assert!(completions.is_active(&key), "the other copy still holds it");
    assert!(completions.load(&key).await.contains(&objects[0].0));

    finalize(vec![second], EventStatus::Delivered);
    until("second copy deleted", || queue.deletes() == 2).await;
    let records = data_dir.path().join(super::completion::DIR_NAME);
    until("record removed", || {
        !completions.is_active(&key) && std::fs::read_dir(&records).unwrap().next().is_none()
    })
    .await;
    assert!(completions.load(&key).await.is_empty());
    harness.stop().await;
}

/// L4: a shutdown while an object waits to be tried again releases the batch at once instead
/// of after every try.
#[tokio::test(start_paused = true)]
async fn shutdown_during_a_retry_wait_releases_promptly() {
    let started = tokio::time::Instant::now();
    let objects = objects(1, 1);
    let queue = MockQueue::with(vec![message("m-retry-stop", &body_for(&objects))], false);
    let store = mock_store(&objects, &queue, Some((0, 100)));
    let mut config = settings();
    config.fetch_max_tries = 10;
    config.fetch_retry_backoff = Duration::from_secs(30);
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        config,
        None,
        Arc::new(Completions::in_memory()),
        10_000,
    );

    until("first try", || gets(&queue, &objects[0].0) >= 1).await;
    let shutdown_at = started.elapsed();
    harness.begin_shutdown();
    until("release", || {
        queue
            .calls()
            .contains(&Call::Visibility("m-retry-stop#1".to_owned(), 0))
    })
    .await;
    let released_at = queue
        .timed_calls()
        .into_iter()
        .find(|(_, call)| *call == Call::Visibility("m-retry-stop#1".to_owned(), 0))
        .unwrap()
        .0;
    assert!(
        released_at < shutdown_at + Duration::from_secs(2),
        "released {released_at:?}, shutdown at {shutdown_at:?}"
    );
    assert!(gets(&queue, &objects[0].0) < 3, "no further tries");
    harness.stop().await;
}

/// L5: an unwritable spool directory is detected at startup.
#[cfg(unix)]
#[test]
fn unwritable_spool_dir_is_detected() {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = tempfile::tempdir().unwrap();
    super::check_spool_dir(&dir.path().join("spool")).unwrap();
    let locked = dir.path().join("locked");
    std::fs::create_dir(&locked).unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).unwrap();
    let error = super::check_spool_dir(&locked.join("spool")).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(super::check_spool_dir(&locked).is_err());
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
}
