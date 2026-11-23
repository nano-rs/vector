//! NAN-5874 (nano-rs/vector): a reload that re-points an UNCHANGED upstream must not drop the
//! events that upstream takes in while the new components build.
//!
//! The shape, as nano hit it (v2.0.35 inserted `<parser>_source_tz` in front of every
//! `<parser>_parse`): `prepare` is unchanged; `parse` changes and now reads a NEW stage `tz`
//! instead of `prepare`. `shutdown_diff` used to REMOVE `parse` from `prepare`'s fanout up front
//! (the new config no longer feeds `parse` from `prepare`), so for the whole build `prepare` kept
//! running with no consumer: every event it emitted was dropped and finalized as delivered (HTTP
//! 200, HEC ackIds true, nothing stored). A real build compiles VRL for tens of seconds; here
//! `tz` takes `BUILD_MS` to build.
//!
//! Now that connection is paused (sends wait, as for a changed consumer that keeps its input)
//! and removed only once the new topology is connected.

use std::time::Duration;

use futures::StreamExt;
use vector_lib::event::{Event, EventContainer, LogEvent};

use crate::{
    config::Config,
    extra_context::ExtraContext,
    test_util::{
        mock::{
            basic_sink, basic_source, basic_transform, build_error_source, slow_build_transform,
        },
        start_topology, trace_init,
    },
    topology::ReloadError,
};

const BUILD_MS: u64 = 1500;
const EVENTS: usize = 40;

fn message(event: Event) -> String {
    let log = event.into_log();
    let key = crate::config::log_schema()
        .message_key_target_path()
        .expect("message key");
    log.get(key).unwrap().to_string_lossy().into_owned()
}

/// Sends `EVENTS` events, one every 50 ms, so most of them arrive while the reload builds.
fn spawn_sender(mut tx: vector_lib::source_sender::SourceSender) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        for i in 0..EVENTS {
            tx.send_event(Event::Log(LogEvent::from(format!("e{i}"))))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
}

/// Collects messages until `EVENTS` arrived or nothing arrives for `idle`.
async fn collect(
    out: &mut (impl futures::Stream<Item = vector_lib::source_sender::SourceSenderItem> + Unpin),
    idle: Duration,
) -> Vec<String> {
    let mut got = Vec::new();
    while got.len() < EVENTS {
        match tokio::time::timeout(idle, out.next()).await {
            Ok(Some(item)) => got.extend(item.events.into_events().map(message)),
            _ => break,
        }
    }
    got
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reload_that_repoints_an_unchanged_upstream_loses_no_event() {
    trace_init();

    let (in_tx, source) = basic_source();
    let (mut out_rx, sink) = basic_sink(100);
    let prepare = basic_transform("", 0.0);

    let mut old = Config::builder();
    old.add_source("in", source.clone());
    old.add_transform("prepare", &["in"], prepare.clone());
    old.add_transform("parse", &["prepare"], basic_transform(" p1", 0.0));
    old.add_sink("out", &["parse"], sink.clone());
    let (mut topology, _crash) = start_topology(old.build().unwrap(), false).await;

    let mut new = Config::builder();
    new.add_source("in", source);
    new.add_transform("prepare", &["in"], prepare);
    new.add_transform("tz", &["prepare"], slow_build_transform(" tz", BUILD_MS));
    new.add_transform("parse", &["tz"], basic_transform(" p2", 0.0));
    new.add_sink("out", &["parse"], sink);

    let sender = spawn_sender(in_tx);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = std::time::Instant::now();
    topology
        .reload_config_and_respawn(new.build().unwrap(), ExtraContext::default())
        .await
        .expect("the reload succeeds");
    assert!(
        started.elapsed() >= Duration::from_millis(BUILD_MS),
        "the build was slow"
    );
    sender.await.unwrap();

    let got = collect(&mut out_rx, Duration::from_secs(5)).await;
    assert_eq!(
        got.len(),
        EVENTS,
        "events were lost across the reload: {got:?}"
    );
    // Before the reload the old wiring, after it the new one; never anything else.
    assert!(
        got.iter()
            .all(|m| m.ends_with(" p1") || m.ends_with(" tz p2")),
        "{got:?}"
    );
    assert!(
        got.iter().any(|m| m.ends_with(" tz p2")),
        "the new wiring is live: {got:?}"
    );

    topology.stop().await;
}

/// The same re-pointing reload, removing the old consumer outright: `prepare` is read by the
/// new `tz` and the old `parse` is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reload_that_removes_the_consumer_of_an_unchanged_upstream_loses_no_event() {
    trace_init();

    let (in_tx, source) = basic_source();
    let (mut out_rx, sink) = basic_sink(100);
    let prepare = basic_transform("", 0.0);

    let mut old = Config::builder();
    old.add_source("in", source.clone());
    old.add_transform("prepare", &["in"], prepare.clone());
    old.add_transform("parse", &["prepare"], basic_transform(" p1", 0.0));
    old.add_sink("out", &["parse"], sink.clone());
    let (mut topology, _crash) = start_topology(old.build().unwrap(), false).await;

    let mut new = Config::builder();
    new.add_source("in", source);
    new.add_transform("prepare", &["in"], prepare);
    new.add_transform("tz", &["prepare"], slow_build_transform(" tz", BUILD_MS));
    new.add_sink("out", &["tz"], sink);

    let sender = spawn_sender(in_tx);
    tokio::time::sleep(Duration::from_millis(200)).await;
    topology
        .reload_config_and_respawn(new.build().unwrap(), ExtraContext::default())
        .await
        .expect("the reload succeeds");
    sender.await.unwrap();

    let got = collect(&mut out_rx, Duration::from_secs(5)).await;
    assert_eq!(
        got.len(),
        EVENTS,
        "events were lost across the reload: {got:?}"
    );
    assert!(
        got.iter().all(|m| m.ends_with(" p1") || m.ends_with(" tz")),
        "{got:?}"
    );

    topology.stop().await;
}

/// A failed re-pointing reload rolls back: the paused connection is RESUMED (not re-added, which
/// would panic on a duplicate fanout id), and the events sent during the failed build arrive
/// through the old wiring.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_repointing_reload_resumes_the_paused_connection() {
    trace_init();

    let (in_tx, source) = basic_source();
    let (mut out_rx, sink) = basic_sink(100);
    let prepare = basic_transform("", 0.0);
    let parse = basic_transform(" p1", 0.0);

    let mut old = Config::builder();
    old.add_source("in", source.clone());
    old.add_transform("prepare", &["in"], prepare.clone());
    old.add_transform("parse", &["prepare"], parse.clone());
    old.add_sink("out", &["parse"], sink.clone());
    let (mut topology, crash) = start_topology(old.build().unwrap(), false).await;
    let mut crash = tokio_stream::wrappers::UnboundedReceiverStream::new(crash);

    let (_bad_tx, bad_sink) = basic_sink(10);
    let mut new = Config::builder();
    new.add_source("in", source);
    new.add_transform("prepare", &["in"], prepare);
    new.add_transform("tz", &["prepare"], slow_build_transform(" tz", BUILD_MS));
    new.add_transform("parse", &["tz"], basic_transform(" p2", 0.0));
    new.add_sink("out", &["parse"], sink);
    new.add_source("bad", build_error_source());
    new.add_sink("bad_out", &["bad"], bad_sink);

    let sender = spawn_sender(in_tx);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let result = topology
        .reload_config_and_respawn(new.build().unwrap(), ExtraContext::default())
        .await;
    assert!(
        matches!(result, Err(ReloadError::TopologyBuildFailed)),
        "{result:?}"
    );
    sender.await.unwrap();

    let got = collect(&mut out_rx, Duration::from_secs(5)).await;
    let crashed = tokio::time::timeout(Duration::from_millis(500), crash.next()).await;
    assert!(
        crashed.is_err(),
        "the restored topology crashed: {crashed:?}"
    );
    assert_eq!(
        got.len(),
        EVENTS,
        "events were lost across the failed reload: {got:?}"
    );
    assert!(
        got.iter().all(|m| m.ends_with(" p1")),
        "only the old wiring: {got:?}"
    );

    topology.stop().await;
}
