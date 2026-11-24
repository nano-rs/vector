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
use vector_lib::{
    event::{Event, EventContainer, LogEvent},
    extra_context::ExtraContext,
};

use crate::{
    config::Config,
    test_util::{
        mock::{
            basic_sink, basic_source, basic_transform, build_error_source, flaky_build_transform,
            slow_build_transform, two_port_transform,
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

fn free_port() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

/// A reload that must wait for a removed sink (its port is claimed by the replacement) while
/// the transform feeding it is idle. The idle transform has to apply the control message that
/// drops the removed sink's sender; it used to apply control messages only when it had events
/// to send, so the reload waited forever (and with #33's held inputs, a paused upstream made
/// such an idle transform the common case).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reload_waiting_on_a_sink_fed_by_an_idle_transform_completes() {
    trace_init();

    let port = free_port();
    let (in_tx, source) = basic_source();
    let (_out1_rx, sink1) = basic_sink(10);
    let (_out2_rx, sink2) = basic_sink(10);

    let mut old = Config::builder();
    old.add_source("in", source.clone());
    old.add_transform("mid", &["in"], basic_transform("", 0.0));
    old.add_sink(
        "s1",
        &["mid"],
        sink1.with_resource(crate::config::Resource::tcp(port)),
    );
    let (mut topology, _crash) = start_topology(old.build().unwrap(), false).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut new = Config::builder();
    new.add_source("in", source);
    new.add_transform("mid", &["in"], basic_transform("", 0.0));
    new.add_sink(
        "s2",
        &["mid"],
        sink2.with_resource(crate::config::Resource::tcp(port)),
    );
    tokio::time::timeout(
        Duration::from_secs(10),
        topology.reload_config_and_respawn(new.build().unwrap(), ExtraContext::default()),
    )
    .await
    .expect("the reload must not wait forever on the idle transform")
    .expect("the reload succeeds");

    drop(in_tx);
    topology.stop().await;
}

/// `T1 -> T2` where `T1` changes and `T2` is removed: the old `T1` must drain what it holds into
/// the old `T2` (and on to the sink), never into an emptied fanout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chained_changed_and_removed_transforms_lose_no_event() {
    trace_init();

    for round in 0..5 {
        let (mut in_tx, source) = basic_source();
        // A small sink channel that is not read until after the reload: the old chain is full
        // of events when the reload starts.
        let (mut out_rx, sink) = basic_sink(1);

        let mut old = Config::builder();
        old.add_source("in", source.clone());
        old.add_transform("t1", &["in"], basic_transform(" a", 0.0));
        old.add_transform("t2", &["t1"], basic_transform(" b", 0.0));
        old.add_transform("t3", &["t2"], basic_transform(" c", 0.0));
        old.add_sink("out", &["t3"], sink.clone());
        let (mut topology, _crash) = start_topology(old.build().unwrap(), false).await;

        let sender = tokio::spawn(async move {
            for i in 0..EVENTS {
                in_tx
                    .send_event(Event::Log(LogEvent::from(format!("e{i}"))))
                    .await
                    .unwrap();
            }
            in_tx
        });
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut new = Config::builder();
        new.add_source("in", source);
        new.add_transform("t1", &["in"], basic_transform(" A", 0.0));
        new.add_transform("t3", &["t1"], basic_transform(" C", 0.0));
        new.add_sink("out", &["t3"], sink);
        topology
            .reload_config_and_respawn(new.build().unwrap(), ExtraContext::default())
            .await
            .expect("the reload succeeds");

        let got = collect(&mut out_rx, Duration::from_secs(5)).await;
        let _in_tx = sender.await.unwrap();
        assert_eq!(
            got.len(),
            EVENTS,
            "round {round}: events lost in the chain: {got:?}"
        );
        topology.stop().await;
    }
}

/// A reload that only removes a consumer of an unchanged input (nothing new reads it) does not
/// hold that input: its other consumers keep receiving while the reload builds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removing_a_consumer_does_not_stall_its_siblings_during_the_build() {
    trace_init();

    let (in_tx, source) = basic_source();
    let (mut out1_rx, sink1) = basic_sink(100);
    let (_out2_rx, sink2) = basic_sink(100);
    let (_slow_tx, slow_source) = basic_source();
    let (_out3_rx, sink3) = basic_sink(10);

    let mut old = Config::builder();
    old.add_source("in", source.clone());
    old.add_transform("c1", &["in"], basic_transform(" c1", 0.0));
    old.add_transform("c2", &["in"], basic_transform(" c2", 0.0));
    old.add_sink("out1", &["c1"], sink1.clone());
    old.add_sink("out2", &["c2"], sink2);
    let (mut topology, _crash) = start_topology(old.build().unwrap(), false).await;

    let mut new = Config::builder();
    new.add_source("in", source);
    new.add_transform("c1", &["in"], basic_transform(" c1", 0.0));
    new.add_sink("out1", &["c1"], sink1);
    // Unrelated, slow to build: the reload's build window.
    new.add_source("other", slow_source);
    new.add_transform("slow", &["other"], slow_build_transform(" s", BUILD_MS));
    new.add_sink("out3", &["slow"], sink3);

    let sender = spawn_sender(in_tx);
    tokio::time::sleep(Duration::from_millis(200)).await;
    {
        let reload =
            topology.reload_config_and_respawn(new.build().unwrap(), ExtraContext::default());
        tokio::pin!(reload);
        // While the reload builds, c1 keeps delivering.
        let mut during = 0;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(BUILD_MS / 2);
        loop {
            tokio::select! {
                result = &mut reload => {
                    result.expect("the reload succeeds");
                    panic!("the reload finished before the build window was observed");
                }
                item = out1_rx.next() => {
                    if let Some(item) = item { during += item.events.len(); }
                }
                () = tokio::time::sleep_until(deadline) => break,
            }
        }
        assert!(during > 0, "c1 delivered nothing while the reload built");
        reload.await.expect("the reload succeeds");
    }
    sender.await.unwrap();
    topology.stop().await;
}

/// `FailedToRestore` with a tap watch subscriber alive, as with `api.enabled = true` (nano enables
/// the API): the watch holds clones of every control channel, so the channels never close during
/// `stop()`. The paused connection must still be released (`ControlMessage::Stop`): `stop()`
/// completes gracefully, and the events that waited on it are not acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_restore_with_a_tap_watch_alive_still_stops_gracefully() {
    use vector_lib::event::{BatchNotifier, BatchStatus};

    trace_init();

    let (mut in_tx, source) = basic_source();
    let (_out_rx, out) = basic_sink(1000);
    let prepare = basic_transform("", 0.0);

    let mut old = Config::builder();
    old.add_source("in", source.clone());
    old.add_transform("prepare", &["in"], prepare.clone());
    old.add_transform("parse", &["prepare"], flaky_build_transform(" p", 1));
    old.add_sink("out", &["parse"], out.clone());
    let (mut topology, _crash) = start_topology(old.build().unwrap(), false).await;
    let watch = topology.watch();

    let (_bad_rx, bad_sink) = basic_sink(10);
    let mut new = Config::builder();
    new.add_source("in", source);
    new.add_transform("prepare", &["in"], prepare);
    new.add_transform("parse_new", &["prepare"], basic_transform(" p2", 0.0));
    new.add_sink("out", &["parse_new"], out);
    new.add_source("bad", build_error_source());
    new.add_sink("bad_out", &["bad"], bad_sink);
    let result = topology
        .reload_config_and_respawn(new.build().unwrap(), ExtraContext::default())
        .await;
    assert!(
        matches!(result, Err(ReloadError::FailedToRestore)),
        "{result:?}"
    );

    // An event that reaches the paused connection after the failed restore.
    let (batch, receiver) = BatchNotifier::new_with_receiver();
    in_tx
        .send_event(Event::Log(
            LogEvent::from("held").with_batch_notifier(&batch),
        ))
        .await
        .unwrap();
    drop(batch);
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(in_tx);

    let stopped = tokio::time::timeout(Duration::from_secs(10), topology.stop())
        .await
        .expect("stop() finishes while a tap watch is alive");
    assert!(stopped, "stop() was graceful");
    let status = tokio::time::timeout(Duration::from_secs(5), receiver)
        .await
        .expect("the held event was finalized");
    assert_ne!(
        status,
        BatchStatus::Delivered,
        "a held event must not be acknowledged"
    );
    drop(watch);
}

/// A concurrent multi-port transform under sustained traffic on port `a`, while a reload waits
/// for a sink on its idle port `b` (removed; its replacement claims the same port). Its control
/// messages are served before new input, so the traffic cannot starve the reload.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sustained_traffic_does_not_starve_a_reload_waiting_on_an_idle_port() {
    trace_init();

    let port = free_port();
    let (mut in_tx, source) = basic_source();
    let (mut a_rx, sink_a) = basic_sink(1000);
    let (_b_rx, sink_b) = basic_sink(10);
    let (_b2_rx, sink_b2) = basic_sink(10);

    let mut old = Config::builder();
    old.add_source("in", source.clone());
    old.add_transform("split", &["in"], two_port_transform());
    old.add_sink("out_a", &["split.a"], sink_a.clone());
    old.add_sink(
        "s_b",
        &["split.b"],
        sink_b.with_resource(crate::config::Resource::tcp(port)),
    );
    let (mut topology, _crash) = start_topology(old.build().unwrap(), false).await;

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sending = {
        let stop = std::sync::Arc::clone(&stop);
        tokio::spawn(async move {
            let mut sent = 0usize;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                in_tx
                    .send_event(Event::Log(LogEvent::from(format!("e{sent}"))))
                    .await
                    .unwrap();
                sent += 1;
            }
            sent
        })
    };
    let draining = tokio::spawn(async move {
        let mut got = 0usize;
        while let Some(item) = a_rx.next().await {
            got += item.events.len();
        }
        got
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut new = Config::builder();
    new.add_source("in", source);
    new.add_transform("split", &["in"], two_port_transform());
    new.add_sink("out_a", &["split.a"], sink_a);
    new.add_sink(
        "s_b2",
        &["split.b"],
        sink_b2.with_resource(crate::config::Resource::tcp(port)),
    );
    tokio::time::timeout(
        Duration::from_secs(10),
        topology.reload_config_and_respawn(new.build().unwrap(), ExtraContext::default()),
    )
    .await
    .expect("sustained traffic must not starve the reload")
    .expect("the reload succeeds");

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let sent = sending.await.unwrap();
    topology.stop().await;
    let got = draining.await.unwrap();
    assert_eq!(got, sent, "port a lost events across the reload");
}
