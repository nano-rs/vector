//! Field findings on `0.61.0-nano.8` (NAN-5005): throughput default, metrics that disappeared,
//! per-batch logs, progress across a crash, acknowledgement advice, and the delivery
//! guarantees the peer asked to confirm (rejection, `start_from`, delete after the last ack).

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use chrono::Utc;
use vector_lib::{event::EventStatus, metrics::Controller};

use super::{
    SkipAction,
    completion::{Completions, batch_key},
    fetch_tests::{
        assert_each_once, body_for, drain, gets, mock_store, objects, position, released,
    },
    filesystem_of,
    manifest::parse_manifest,
    tests::{
        Call, MockQueue, MockStore, Object, RETRY_DELAY, file_entry, finalize, gzip, manifest_body,
        message, settings, start_with, until,
    },
};
use crate::event::Event;

// ---------------------------------------------------------------------------------------------
// 1. file_concurrency 16 with bounded memory

/// `max_memory_bytes` caps memory whatever `file_concurrency` is: objects that do not fit
/// spool to disk, and everything is still delivered once.
#[tokio::test]
async fn memory_budget_spools_objects_that_do_not_fit() {
    let spool = tempfile::tempdir().unwrap();
    // Six objects of a few KiB each; room in memory for one.
    let objects: Vec<(String, Bytes)> = (0..6)
        .map(|file| {
            let lines: Vec<String> = (0..300)
                .map(|line| format!(r#"{{"f":{file},"l":{line},"pad":"{line:0>24}"}}"#))
                .collect();
            let refs: Vec<&str> = lines.iter().map(String::as_str).collect();
            (format!("cid0/data/u/part-{file:05}.gz"), gzip(&refs))
        })
        .collect();
    let largest = objects.iter().map(|(_, data)| data.len()).max().unwrap();
    let queue = MockQueue::with(vec![message("m-budget", &body_for(&objects))], false);
    let store = mock_store(&objects, &queue, None);
    let mut config = settings();
    config.file_concurrency = 16;
    config.max_memory_bytes = largest.div_ceil(1024) * 1024 + 1024;
    config.spool_dir = spool.path().to_owned();
    // A small pipeline: emission of the first object stalls while the others are fetched.
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        config,
        None,
        Arc::new(Completions::in_memory()),
        10,
    );

    let spooled = || {
        std::fs::read_dir(spool.path())
            .unwrap()
            .flatten()
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "part"))
            .count()
    };
    let first = harness.take(1).await;
    for _ in 0..200 {
        if spooled() >= 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        spooled() >= 4,
        "objects past the memory budget spool ({} spooled)",
        spooled()
    );
    let mut seen: HashMap<(i64, i64), usize> = HashMap::new();
    for event in &first {
        *seen.entry(position(event)).or_default() += 1;
    }
    let taken = first.len();
    finalize(first, EventStatus::Delivered);
    for (key, count) in drain(&mut harness, 1800 - taken, EventStatus::Delivered).await {
        *seen.entry(key).or_default() += count;
    }
    assert_eq!(seen.len(), 1800);
    assert_each_once(&seen);
    until("delete", || queue.deletes() == 1).await;
    assert_eq!(spooled(), 0, "spool files removed");
    harness.stop().await;
}

#[test]
fn file_concurrency_defaults_to_16_within_a_memory_budget() {
    let config: super::CrowdstrikeFdrConfig =
        toml::from_str("queue_url = \"https://sqs.us-west-2.amazonaws.com/1/q\"").unwrap();
    assert_eq!(config.file_concurrency, 16);
    assert_eq!(config.max_memory_bytes, 512 * 1024 * 1024);
    assert_eq!(config.spool_threshold_bytes, 64 * 1024 * 1024);
}

// ---------------------------------------------------------------------------------------------
// 2. Metrics present from start-up and never expired

fn series(controller: &Controller, name: &str, tag: Option<(&str, &str)>) -> Option<f64> {
    controller
        .capture_metrics()
        .into_iter()
        .find(|metric| {
            metric.name() == name
                && tag.is_none_or(|(key, value)| {
                    metric.tags().and_then(|tags| tags.get(key)) == Some(value)
                })
        })
        .map(|metric| match metric.value() {
            vector_lib::event::MetricValue::Counter { value }
            | vector_lib::event::MetricValue::Gauge { value } => *value,
            other => panic!("unexpected {other:?}"),
        })
}

/// Vector drops internal metrics idle for `expire_metrics_secs` (300 s by default) unless a
/// handle is held. The source registers its series at zero when it starts and holds them, so
/// they exist before the first batch and outlive any idle period.
#[tokio::test]
async fn batch_metrics_exist_from_start_and_never_expire() {
    vector_lib::metrics::init_test();
    let controller = Controller::get().unwrap();
    controller.set_expiry(Some(0.05), Vec::new()).unwrap();

    let queue = MockQueue::with(Vec::new(), false);
    let store = MockStore::with(Vec::new(), &queue);
    let harness = start_with(
        Arc::clone(&queue),
        store,
        settings(),
        None,
        Arc::new(Completions::in_memory()),
        10,
    );
    // An unheld counter, for contrast: it does expire.
    vector_lib::counter!(
        vector_lib::internal_event::CounterName::ConnectionEstablishedTotal,
        "test" => "unheld",
    )
    .increment(1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    for _ in 0..3 {
        // Expiry is applied as metrics are captured.
        controller.capture_metrics();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert_eq!(
        series(
            controller,
            "connection_established_total",
            Some(("test", "unheld"))
        ),
        None,
        "expiry is active in this test"
    );
    for status in ["received", "acked", "released", "skipped", "parked"] {
        assert_eq!(
            series(
                controller,
                "crowdstrike_fdr_manifests_total",
                Some(("status", status))
            ),
            Some(0.0),
            "manifests_total{{status={status}}}"
        );
    }
    for name in [
        "crowdstrike_fdr_files_fetched_total",
        "crowdstrike_fdr_bytes_fetched_total",
        "crowdstrike_fdr_files_skipped_total",
        "crowdstrike_fdr_inflight_manifests",
        "crowdstrike_fdr_inflight_bytes",
    ] {
        assert_eq!(series(controller, name, None), Some(0.0), "{name}");
    }
    controller.set_expiry(None, Vec::new()).unwrap();
    harness.stop().await;
}

// ---------------------------------------------------------------------------------------------
// 3. Per-batch info logs

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Captured {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }

    fn lines_with(&self, needle: &str) -> Vec<String> {
        self.text()
            .lines()
            .filter(|line| line.contains(needle))
            .map(str::to_owned)
            .collect()
    }
}

/// Runs the named test of this binary alone in a child process (with `FDR_TEST_CHILD` set),
/// where no other test thread can race tracing's per-callsite interest cache.
fn run_alone(name: &str) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([name, "--exact", "--test-threads=1", "--nocapture"])
        .env("FDR_TEST_CHILD", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Each batch logs, at info: received (message id, files, bytes, batch time), delivered and
/// deleted, or released with the reason.
#[tokio::test]
async fn batches_log_received_delivered_and_released_with_reason() {
    if std::env::var_os("FDR_TEST_CHILD").is_none() {
        run_alone(
            "sources::crowdstrike_fdr::operations_tests::batches_log_received_delivered_and_released_with_reason",
        );
        return;
    }
    let captured = Captured::default();
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _default = tracing::subscriber::set_default(subscriber);

    let good = objects(1, 2);
    let bad = vec![(
        "cid0/data/v/part-00000.gz".to_owned(),
        gzip(&[r#"{"f":9,"l":0}"#]),
    )];
    let queue = MockQueue::with(
        vec![
            message("m-log-good", &body_for(&good)),
            message("m-log-bad", &body_for(&bad)),
        ],
        false,
    );
    let mut entries: Vec<(&str, Object)> = good
        .iter()
        .map(|(key, data)| (key.as_str(), Object::Data(data.clone())))
        .collect();
    entries.push((bad[0].0.as_str(), Object::Missing));
    let store = MockStore::with(entries, &queue);
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        settings(),
        None,
        Arc::new(Completions::in_memory()),
        10_000,
    );

    let events = harness.take(2).await;
    finalize(events, EventStatus::Delivered);
    until("delete and release", || {
        queue.deletes() == 1 && released(&queue, "m-log-bad#1")
    })
    .await;
    harness.stop().await;

    let received = captured.lines_with("FDR batch received.");
    assert_eq!(received.len(), 2, "{}", captured.text());
    let good_line = received
        .iter()
        .find(|line| line.contains("m-log-good"))
        .unwrap();
    for field in [
        "INFO",
        "files=1",
        "bytes=",
        "batch_timestamp=",
        "receive_count=1",
    ] {
        assert!(good_line.contains(field), "{field} in {good_line}");
    }
    let delivered = captured.lines_with("FDR batch delivered; message deleted.");
    assert_eq!(delivered.len(), 1);
    assert!(delivered[0].contains("m-log-good") && delivered[0].contains("INFO"));
    let released_lines = captured.lines_with("FDR batch released for re-delivery.");
    assert_eq!(released_lines.len(), 1);
    assert!(
        released_lines[0].contains("m-log-bad")
            && released_lines[0].contains("reason=\"failed_fetching_object\"")
            && released_lines[0].contains("INFO"),
        "{}",
        released_lines[0]
    );
}

// ---------------------------------------------------------------------------------------------
// 4. Progress survives a crash (persistent data_dir)

/// The process is killed (no shutdown, no release) with files 0-4 of a batch delivered and
/// file 5 still downloading. SQS re-delivers the message to a new process using the same
/// `data_dir`: it fetches and emits only files 5-7.
#[tokio::test]
async fn a_crash_mid_batch_resumes_from_the_progress_record() {
    let data_dir = tempfile::tempdir().unwrap();
    let objects = objects(8, 2);
    let body = body_for(&objects);
    let key = batch_key(&parse_manifest(&body).unwrap());

    let queue = MockQueue::with(vec![message("m-crash", &body)], false);
    let mut entries: Vec<(&str, Object)> = objects
        .iter()
        .map(|(key, data)| (key.as_str(), Object::Data(data.clone())))
        .collect();
    entries[5].1 = Object::Stall(objects[5].1.slice(..4));
    let store = MockStore::with(entries, &queue);
    let mut config = settings();
    config.file_concurrency = 1;
    let completions = Arc::new(Completions::open(data_dir.path()).unwrap());
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        config,
        None,
        Arc::clone(&completions),
        10_000,
    );
    let seen = drain(&mut harness, 10, EventStatus::Delivered).await;
    assert!(seen.keys().all(|(file, _)| *file < 5));
    for _ in 0..100 {
        if completions.load(&key).await.len() == 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    harness.kill(); // crash while file 5 downloads
    drop(completions);
    assert_eq!(queue.deletes(), 0);
    assert!(
        !queue
            .calls()
            .iter()
            .any(|call| matches!(call, Call::Visibility(_, RETRY_DELAY | 0))),
        "a crash neither releases nor deletes"
    );

    // A new process, same data_dir; the visibility timeout lapsed and SQS re-delivers.
    let mut again = message("m-crash", &body);
    again.receipt = "m-crash#2".to_owned();
    again.receive_count = 2;
    let queue = MockQueue::with(vec![again], false);
    let store = mock_store(&objects, &queue, None);
    let completions = Arc::new(Completions::open(data_dir.path()).unwrap());
    assert_eq!(completions.records_on_disk(), 1, "the record survived");
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        settings(),
        None,
        completions,
        10_000,
    );
    let seen = drain(&mut harness, 6, EventStatus::Delivered).await;
    assert!(seen.keys().all(|(file, _)| *file >= 5), "{seen:?}");
    until("delete", || queue.deletes() == 1).await;
    for (key, _) in &objects[..5] {
        assert_eq!(gets(&queue, key), 0, "{key} not fetched again");
    }
    harness.stop().await;
}

#[test]
fn ephemeral_data_dirs_are_recognised() {
    let mountinfo = "\
22 1 0:21 / / rw,relatime - overlay overlay rw,lowerdir=/l,upperdir=/u
30 22 8:1 /volumes/vector /var/lib/vector rw,relatime - ext4 /dev/sda1 rw
31 22 0:40 / /tmp rw - tmpfs tmpfs rw
32 22 8:2 / /data\\040dir rw - xfs /dev/sdb rw";
    let fs = |path: &str| filesystem_of(mountinfo, std::path::Path::new(path));
    assert_eq!(
        fs("/var/lib/vector/fdr").as_deref(),
        Some("ext4"),
        "a mounted volume"
    );
    assert_eq!(
        fs("/var/lib/other").as_deref(),
        Some("overlay"),
        "the container layer"
    );
    assert_eq!(fs("/tmp/x").as_deref(), Some("tmpfs"));
    assert_eq!(fs("/data dir/x").as_deref(), Some("xfs"));
    assert_eq!(
        fs("/var/lib/vectorx").as_deref(),
        Some("overlay"),
        "whole path components"
    );
}

// ---------------------------------------------------------------------------------------------
// (a) A sink rejection releases and never deletes

/// A sink that rejects a file's events without retrying (the ClickHouse sink on a 403: a
/// non-transient HTTP error is `Rejected`) releases the message; it is never deleted, and each
/// re-delivery emits only the rejected file, until `max_tries` leaves it for the dead-letter
/// queue.
#[tokio::test(start_paused = true)]
async fn a_rejecting_sink_releases_and_never_deletes() {
    let objects = objects(3, 1);
    let queue = MockQueue::with(vec![message("m-403", &body_for(&objects))], true);
    let store = mock_store(&objects, &queue, None);
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        settings(),
        None,
        Arc::new(Completions::in_memory()),
        10_000,
    );

    let mut emitted: Vec<(i64, i64)> = Vec::new();
    for delivery in 1..=3 {
        let want = if delivery == 1 { 3 } else { 1 };
        let events = harness.take(want).await;
        let (rejected, delivered): (Vec<Event>, Vec<Event>) =
            events.into_iter().partition(|event| position(event).0 == 1);
        emitted.extend(rejected.iter().chain(&delivered).map(position));
        finalize(delivered, EventStatus::Delivered);
        finalize(rejected, EventStatus::Rejected);
        until("release", || released(&queue, &format!("m-403#{delivery}"))).await;
    }
    // The fourth delivery exceeds max_tries (3): left alone, never deleted.
    tokio::time::sleep(Duration::from_mins(10)).await;
    assert_eq!(queue.deletes(), 0, "a rejected batch is never deleted");
    emitted.sort_unstable();
    assert_eq!(emitted, vec![(0, 0), (1, 0), (1, 0), (1, 0), (2, 0)]);
    harness.stop().await;
}

// ---------------------------------------------------------------------------------------------
// (b) start_from: delete or release skipped batches

fn old_and_new() -> (String, String, Bytes) {
    let data = gzip(&[r#"{"f":0,"l":0}"#]);
    let old = manifest_body(1_000, &[file_entry("cid0/data/o/part-0.gz", &data)]);
    let new = manifest_body(
        Utc::now().timestamp_millis(),
        &[file_entry("cid0/data/n/part-0.gz", &data)],
    );
    (old, new, data)
}

/// The default `skip_action = delete` deletes a skipped batch (first delivery only), without
/// downloading it.
#[tokio::test(start_paused = true)]
async fn start_from_deletes_skipped_batches_by_default() {
    let (old, _, data) = old_and_new();
    let queue = MockQueue::with(vec![message("m-old-del", &old)], false);
    let store = MockStore::with(vec![("cid0/data/o/part-0.gz", Object::Data(data))], &queue);
    let mut config = settings();
    config.cutoff = Some(Utc::now());
    let harness = start_with(
        Arc::clone(&queue),
        store,
        config,
        None,
        Arc::new(Completions::in_memory()),
        10_000,
    );
    until("delete", || queue.deletes() == 1).await;
    assert_eq!(gets(&queue, "cid0/data/o/part-0.gz"), 0);
    harness.stop().await;
}

/// `skip_action = release` never deletes a skipped batch, on any delivery: it is handed back
/// (visible after `retry_delay_secs`) without downloading, and does not pin the cursor. Newer
/// batches are processed normally.
#[tokio::test(start_paused = true)]
async fn start_from_release_never_deletes_skipped_batches() {
    let (old, new, data) = old_and_new();
    let queue = MockQueue::with(
        vec![message("m-old-rel", &old), message("m-new-rel", &new)],
        true,
    );
    let store = MockStore::with(
        vec![
            ("cid0/data/o/part-0.gz", Object::Data(data.clone())),
            ("cid0/data/n/part-0.gz", Object::Data(data)),
        ],
        &queue,
    );
    let mut config = settings();
    config.cutoff = Some(Utc::now() - chrono::Duration::hours(1));
    config.skip_action = SkipAction::Release;
    let dir = tempfile::tempdir().unwrap();
    let cursor = Arc::new(super::cursor::Cursor::open(dir.path()).unwrap());
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        config,
        Some(Arc::clone(&cursor)),
        Arc::new(Completions::in_memory()),
        10_000,
    );

    let events = harness.take(1).await;
    finalize(events, EventStatus::Delivered);
    until("new batch deleted", || {
        queue
            .calls()
            .contains(&Call::Delete("m-new-rel#1".to_owned()))
    })
    .await;
    // Re-delivered (count 2, 3) and still released, never processed or deleted.
    until("released three times", || released(&queue, "m-old-rel#3")).await;
    tokio::time::sleep(Duration::from_mins(5)).await;
    assert_eq!(queue.deletes(), 1, "only the new batch is deleted");
    assert_eq!(gets(&queue, "cid0/data/o/part-0.gz"), 0, "never downloaded");
    assert!(
        cursor.get().await.is_some_and(|ms| ms > 1_000),
        "the skipped batch does not pin the cursor"
    );
    harness.stop().await;
}

// ---------------------------------------------------------------------------------------------
// (c) Deleted only after the last acknowledgement

/// Three files acknowledged one at a time, last file first: the message is deleted only once
/// the very last event of the batch is acknowledged.
#[tokio::test(start_paused = true)]
async fn deleted_only_after_the_last_acknowledgement() {
    let objects = objects(3, 2);
    let queue = MockQueue::with(vec![message("m-last-ack", &body_for(&objects))], false);
    let store = mock_store(&objects, &queue, None);
    let mut harness = start_with(
        Arc::clone(&queue),
        store,
        settings(),
        None,
        Arc::new(Completions::in_memory()),
        10_000,
    );
    let mut events = harness.take(6).await;
    events.sort_by_key(position);
    let last = events.remove(0); // file 0, line 0: acknowledged last
    while let Some(event) = events.pop() {
        finalize(vec![event], EventStatus::Delivered);
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(
            queue.deletes(),
            0,
            "deleted before every event was acknowledged"
        );
    }
    finalize(vec![last], EventStatus::Delivered);
    until("delete", || queue.deletes() == 1).await;
    harness.stop().await;
}

// ---------------------------------------------------------------------------------------------
// 5. Acknowledgement advice points at the sinks

#[test]
fn acknowledgement_warning_points_at_the_sinks() {
    let captured = Captured::default();
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || super::warn_unless_acknowledged(false));
    let text = captured.text();
    assert!(
        text.contains("acknowledgements.enabled = true` on the sinks"),
        "{text}"
    );
    assert!(text.contains("source-level `acknowledgements` option is deprecated"));
}
