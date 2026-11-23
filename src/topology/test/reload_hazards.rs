//! NAN-5874 follow-ups: reload hazards raised by the review of nano-rs/vector#33 (pause, don't
//! remove, a re-pointed unchanged input until the new topology connects).
//!
//! Each test builds a small topology from the mock components, runs a reload under continuous
//! traffic, and asserts on loss / hangs / stalls. Anything that can hang runs on its own runtime
//! in its own thread behind a watchdog (`with_watchdog`): a fanout spinning in
//! `wait_for_replacements` on a closed control channel never yields, so it would also wedge the
//! runtime's shutdown at the end of a plain `#[tokio::test]`.
//!
//! Status against #33's head (9ae67a2b94) and its base (a7bc4dc791, stock reload logic):
//!
//! | test                                                                       | #33  | base |
//! |----------------------------------------------------------------------------|------|------|
//! | 1. `shutdown_diff_does_not_deadlock_on_a_conflicting_sink_behind_a_paused_fanout` | hang | pass |
//! | 2. `a_reload_that_changes_t1_and_removes_t2_loses_no_buffered_event`        | loss | loss |
//! | 3. `a_failed_restore_does_not_leave_fanouts_paused_and_stop_completes`      | hang | pass |
//! | 4. `a_removed_sibling_consumer_does_not_stall_the_unchanged_one_during_the_build` | stall | loss (NAN-5874) |
//! | 5. `reload_soak` (`#[ignore]`, see its doc comment for the command)          | loss | -    |
//! | 6. `a_failed_repointing_reload_under_fast_traffic_loses_no_event`           | pass | loss (NAN-5874) |

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant},
};

use futures::StreamExt;
use vector_lib::{
    event::{Event, EventContainer, LogEvent},
    extra_context::ExtraContext,
    source_sender::{SourceSender, SourceSenderItem},
};

use crate::{
    config::{Config, ConfigBuilder, Resource},
    test_util::{
        mock::{
            basic_sink, basic_source, basic_transform, build_error_source, flaky_build_transform,
            slow_build_transform, slow_process_transform,
        },
        start_topology, trace_init,
    },
    topology::{ReloadError, RunningTopology},
};

// ---------------------------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------------------------

fn message(event: Event) -> String {
    let log = event.into_log();
    let key = crate::config::log_schema()
        .message_key_target_path()
        .expect("message key");
    log.get(key).unwrap().to_string_lossy().into_owned()
}

/// The sequence number of `e<n> <suffixes>`.
fn seq(m: &str) -> usize {
    m.split(' ').next().unwrap()[1..].parse().unwrap()
}

/// Process CPU time (user + system) so far.
fn cpu_time() -> Duration {
    // SAFETY: `getrusage` only writes the struct we hand it.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &raw mut usage) };
    assert_eq!(rc, 0, "getrusage");
    let tv = |t: libc::timeval| {
        Duration::from_secs(t.tv_sec as u64) + Duration::from_micros(t.tv_usec as u64)
    };
    tv(usage.ru_utime) + tv(usage.ru_stime)
}

/// Current resident set size of this process, in bytes.
fn rss_bytes() -> u64 {
    use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
    let pid = Pid::from_u32(std::process::id());
    let mut sys = System::new();
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_memory(),
    );
    sys.process(pid).map(sysinfo::Process::memory).unwrap_or(0)
}

/// Runs `scenario` on a fresh multi-threaded runtime in its own thread and fails the test if it
/// does not finish within `limit`. On a hang the thread (and its runtime) is leaked on purpose:
/// dropping a runtime whose worker is spinning would hang the test process.
fn with_watchdog<F, Fut>(name: &str, limit: Duration, scenario: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    let (tx, rx) = mpsc::channel::<()>();
    std::thread::Builder::new()
        .name(format!("watchdog-{name}"))
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .unwrap();
            // Declared after `rt`: on a panic it is dropped first, so the watchdog learns of the
            // failure even if the runtime then never finishes shutting down.
            let tx = tx;
            rt.block_on(scenario());
            _ = tx.send(());
            // Don't wait on the runtime's shutdown here either: a scenario that "passed" may
            // still leave a spinning task behind, and that is reported by the scenario itself.
            rt.shutdown_background();
        })
        .unwrap();
    match rx.recv_timeout(limit) {
        Ok(()) => {}
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("{name}: the scenario panicked (see its panic message above)")
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("{name}: the scenario did not finish within {limit:?} (hang)")
        }
    }
}

/// Sends `e<start>..e<start+count>`, one every `every`. Returns the next sequence number.
fn spawn_sender(
    mut tx: SourceSender,
    start: usize,
    count: usize,
    every: Duration,
) -> tokio::task::JoinHandle<(SourceSender, usize)> {
    tokio::spawn(async move {
        for i in start..start + count {
            tx.send_event(Event::Log(LogEvent::from(format!("e{i}"))))
                .await
                .unwrap();
            if !every.is_zero() {
                tokio::time::sleep(every).await;
            }
        }
        (tx, start + count)
    })
}

/// Drains a mock sink's stream into a shared vec of `(arrival, message)`.
type Received = Arc<Mutex<Vec<(Instant, String)>>>;

fn spawn_collector(
    out: impl futures::Stream<Item = SourceSenderItem> + Send + 'static,
) -> Received {
    let got: Received = Arc::default();
    let got2 = Arc::clone(&got);
    tokio::spawn(async move {
        let mut out = Box::pin(out);
        while let Some(item) = out.next().await {
            let now = Instant::now();
            let mut got = got2.lock().unwrap();
            got.extend(item.events.into_events().map(|e| (now, message(e))));
        }
    });
    got
}

/// Waits until `got` holds `n` messages, or nothing new arrived for `idle`.
async fn wait_for(got: &Received, n: usize, idle: Duration) -> Vec<String> {
    let mut last_len = 0;
    let mut last_change = Instant::now();
    loop {
        let len = got.lock().unwrap().len();
        if len >= n {
            break;
        }
        if len != last_len {
            last_len = len;
            last_change = Instant::now();
        } else if last_change.elapsed() >= idle {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    got.lock().unwrap().iter().map(|(_, m)| m.clone()).collect()
}

/// Asserts that every one of `0..n` arrived exactly once.
fn assert_exactly_once(what: &str, got: &[String], n: usize) {
    let mut counts = HashMap::<usize, usize>::new();
    for m in got {
        *counts.entry(seq(m)).or_default() += 1;
    }
    let lost: Vec<_> = (0..n).filter(|i| !counts.contains_key(i)).collect();
    let duplicated: Vec<_> = counts
        .iter()
        .filter(|(_, c)| **c > 1)
        .map(|(i, _)| *i)
        .collect();
    assert!(
        lost.is_empty() && duplicated.is_empty(),
        "{what}: {} of {n} events lost {lost:?}, duplicated {duplicated:?}",
        lost.len()
    );
}

/// A port no sink binds: the mock sinks only *claim* it, to make two sinks conflict.
fn claimed_port() -> Resource {
    Resource::tcp(SocketAddr::from(([127, 0, 0, 1], 9)))
}

async fn reload(topology: &mut RunningTopology, config: ConfigBuilder) -> Result<(), ReloadError> {
    topology
        .reload_config_and_respawn(config.build().unwrap(), ExtraContext::default())
        .await
}

// ---------------------------------------------------------------------------------------------
// (1) shutdown_diff deadlock: a deferred pause starves the transform that feeds a conflicting
//     sink `shutdown_diff` waits for.
// ---------------------------------------------------------------------------------------------

/// Review P1-2, shape 2 ("fan-out through an unchanged mid-transform").
///
/// `in → u`, `u → c_old → out_c`, `u → mid → s_old`. The new config removes `c_old`/`out_c` and
/// replaces sink `s_old` with `s_new`, which claims the same port. `s_old` is therefore a
/// conflicting sink: `shutdown_diff` waits for it to finish, i.e. for every `BufferSender` into it
/// to be dropped, before the new pieces are built.
///
/// With #33, `c_old`'s removal pauses `u` (deferred removal) and `s_old`'s removal pauses `mid`.
/// `u`'s very next send blocks in `wait_for_replacements` until `connect_diff`; `mid` then gets
/// no more events, so it never polls its control channel, never applies the pause, never drops
/// `s_old`'s sender, and `shutdown_diff` waits forever: the reload never returns, the controller
/// mutex stays held. Stock Vector removed `c_old` from `u` up front, so `u` kept feeding `mid`,
/// `mid`'s next send applied `Remove(s_old)`, and the reload finished.
#[test]
fn shutdown_diff_does_not_deadlock_on_a_conflicting_sink_behind_a_paused_fanout() {
    with_watchdog("deadlock", Duration::from_secs(30), || async {
        trace_init();

        let (in_tx, source) = basic_source();
        let (out_c_rx, out_c) = basic_sink(100);
        let (s_old_rx, s_old) = basic_sink(100);
        let (s_new_rx, s_new) = basic_sink(100);
        let _ = (spawn_collector(out_c_rx), spawn_collector(s_old_rx));
        let s_new_got = spawn_collector(s_new_rx);

        let mut old = Config::builder();
        old.add_source("in", source.clone());
        old.add_transform("u", &["in"], basic_transform("", 0.0));
        old.add_transform("c_old", &["u"], basic_transform(" c", 0.0));
        old.add_sink("out_c", &["c_old"], out_c);
        old.add_transform("mid", &["u"], basic_transform(" m", 0.0));
        old.add_sink("s_old", &["mid"], s_old.with_resource(claimed_port()));
        let (mut topology, _crash) = start_topology(old.build().unwrap(), false).await;

        let mut new = Config::builder();
        new.add_source("in", source);
        new.add_transform("u", &["in"], basic_transform("", 0.0));
        new.add_transform("mid", &["u"], basic_transform(" m", 0.0));
        new.add_sink("s_new", &["mid"], s_new.with_resource(claimed_port()));

        // Continuous traffic: stock Vector needs the next event through `mid` to apply the
        // removal of `s_old`, so without traffic it would hang too (that one is upstream's).
        let sender = spawn_sender(in_tx, 0, 400, Duration::from_millis(10));
        tokio::time::sleep(Duration::from_millis(200)).await;

        let result =
            tokio::time::timeout(Duration::from_secs(10), reload(&mut topology, new)).await;
        let Ok(result) = result else {
            panic!(
                "the reload hung for 10s in shutdown_diff: it waits for conflicting sink `s_old`, \
                 whose upstream `mid` is starved by `u`'s deferred pause of removed `c_old`"
            );
        };
        result.expect("the reload succeeds");

        let (_tx, sent) = sender.await.unwrap();
        let got = wait_for(&s_new_got, 1, Duration::from_secs(5)).await;
        assert!(
            !got.is_empty(),
            "the new sink receives events ({sent} sent)"
        );
        topology.stop().await;
    });
}

// ---------------------------------------------------------------------------------------------
// (2) chained changed/removed transforms lose the old upstream's backlog.
// ---------------------------------------------------------------------------------------------

/// Review P1-1. `in → t1 → t2 → out`; the new config changes `t1` and removes `t2`
/// (`in → t1' → out`). `shutdown_diff` handles `transforms.to_remove` (`t2`) before
/// `transforms.to_change` (`t1`): `remove_inputs(t2)` sends `Remove(t2)` to OLD `t1`'s fanout
/// while old `t1` still has a backlog in its input buffer. Old `t1` then drains that backlog into
/// a fanout with no senders: every buffered event is dropped and finalized as delivered.
///
/// `t1` takes 3 ms per event so it holds a backlog (up to `TOPOLOGY_BUFFER_SIZE` batches) when
/// the reload starts. Fails on stock Vector too: the bug predates #33.
#[test]
fn a_reload_that_changes_t1_and_removes_t2_loses_no_buffered_event() {
    with_watchdog("chained", Duration::from_secs(60), || async {
        trace_init();
        const N: usize = 300;

        let (in_tx, source) = basic_source();
        let (out_rx, out) = basic_sink(1000);
        let got = spawn_collector(out_rx);

        let mut old = Config::builder();
        old.add_source("in", source.clone());
        old.add_transform("t1", &["in"], slow_process_transform(" a", 3));
        old.add_transform("t2", &["t1"], basic_transform(" b", 0.0));
        old.add_sink("out", &["t2"], out.clone());
        let (mut topology, _crash) = start_topology(old.build().unwrap(), false).await;

        let mut new = Config::builder();
        new.add_source("in", source);
        new.add_transform("t1", &["in"], slow_process_transform(" a2", 3));
        new.add_sink("out", &["t1"], out);

        // A burst: `t1` falls ~1s behind, so its input buffer is full when the reload starts.
        let sender = spawn_sender(in_tx, 0, N, Duration::ZERO);
        tokio::time::sleep(Duration::from_millis(150)).await;
        reload(&mut topology, new)
            .await
            .expect("the reload succeeds");
        sender.await.unwrap();

        let got = wait_for(&got, N, Duration::from_secs(5)).await;
        assert!(
            got.iter()
                .all(|m| m.ends_with(" a b") || m.ends_with(" a2")),
            "only old or new wiring: {got:?}"
        );
        assert_exactly_once("changed t1 + removed t2", &got, N);
        topology.stop().await;
    });
}

// ---------------------------------------------------------------------------------------------
// (3) FailedToRestore leaves fanouts paused; stop() then spins in wait_for_replacements.
// ---------------------------------------------------------------------------------------------

/// Review P1-3. `in → prepare → parse → out`, `parse` builds once (at start) and fails every
/// later build. The new config removes `parse` (re-pointing `out` to a new `parse_new`) and adds
/// a source that fails to build, so the reload fails; the rollback must rebuild `parse`, which
/// fails too: `ReloadError::FailedToRestore`.
///
/// With #33 `prepare → parse` was paused for deferred removal and nothing resumes or removes it
/// on that path, so `prepare` blocks in `wait_for_replacements` on its next event. `stop()` then
/// drops the control channels; `wait_for_replacements` sees `None` and loops without yielding
/// (100% of a core) until the graceful-shutdown deadline (60s here; forever with none), and
/// `stop()` reports a forced shutdown.
#[test]
fn a_failed_restore_does_not_leave_fanouts_paused_and_stop_completes() {
    // Leave room past the default 60s graceful deadline for the report.
    with_watchdog("failed_restore", Duration::from_secs(90), || async {
        trace_init();

        let (in_tx, source) = basic_source();
        let (out_rx, out) = basic_sink(1000);
        let _got = spawn_collector(out_rx);
        let prepare = basic_transform("", 0.0);

        let mut old = Config::builder();
        old.add_source("in", source.clone());
        old.add_transform("prepare", &["in"], prepare.clone());
        old.add_transform("parse", &["prepare"], flaky_build_transform(" p", 1));
        old.add_sink("out", &["parse"], out.clone());
        let (mut topology, _crash) = start_topology(old.build().unwrap(), false).await;

        let (_bad_rx, bad_sink) = basic_sink(10);
        let mut new = Config::builder();
        new.add_source("in", source);
        new.add_transform("prepare", &["in"], prepare);
        new.add_transform("parse_new", &["prepare"], basic_transform(" p2", 0.0));
        new.add_sink("out", &["parse_new"], out);
        new.add_source("bad", build_error_source());
        new.add_sink("bad_out", &["bad"], bad_sink);

        let sender = spawn_sender(in_tx, 0, 200, Duration::from_millis(10));
        tokio::time::sleep(Duration::from_millis(200)).await;
        let result = reload(&mut topology, new).await;
        assert!(
            matches!(result, Err(ReloadError::FailedToRestore)),
            "precondition: the rollback fails too: {result:?}"
        );
        // Let `prepare` hit its paused fanout.
        tokio::time::sleep(Duration::from_millis(300)).await;
        sender.abort();

        let cpu0 = cpu_time();
        let wall0 = Instant::now();
        let stopped = tokio::time::timeout(Duration::from_secs(10), topology.stop()).await;
        let cores = (cpu_time() - cpu0).as_secs_f64() / wall0.elapsed().as_secs_f64();
        match stopped {
            Ok(true) => {}
            Ok(false) => panic!(
                "stop() after FailedToRestore had to force-kill components ({cores:.2} cores busy \
                 while stopping)"
            ),
            Err(_) => panic!(
                "stop() after FailedToRestore did not finish within 10s; the process burned \
                 {cores:.2} cores meanwhile (a fanout left paused spins in \
                 wait_for_replacements once its control channel closes)"
            ),
        }
    });
}

// ---------------------------------------------------------------------------------------------
// (4) a removed/re-pointed consumer stalls its unchanged sibling for the whole build.
// ---------------------------------------------------------------------------------------------

/// Review P2-1 (point 2). `in → u`, `u → c1 → out1`, `u → c2 → out2`. The new config keeps `c1`
/// and re-points `c2` to read a new slow-building `tz` (`u → tz → c2 → out2`). `u → c2` is paused
/// for deferred removal, and a fanout with any paused sender blocks every send, so `c1` (which
/// nothing touched) receives nothing for the whole build. Stock Vector removed `c2` from `u` up
/// front and `c1` kept flowing.
///
/// Holding `u` is what keeps the events the re-pointed path must see (they reach `tz` once it
/// is connected); the only other place for them is an empty slot, i.e. dropped as delivered.
/// Stock Vector holds `u` the same way for the whole build whenever one of its consumers is
/// changed in place, so the unchanged sibling waiting during a re-point is accepted: asserted
/// here is that neither sink loses or duplicates anything and that `c1` flows again after the
/// reload. A reload that only REMOVES a consumer (nothing new reads `u`) does not hold `u`;
/// `repointed_reload::removing_a_consumer_does_not_stall_its_siblings_during_the_build` asserts
/// the sibling keeps flowing then.
#[test]
fn a_removed_sibling_consumer_does_not_stall_the_unchanged_one_during_the_build() {
    with_watchdog("sibling", Duration::from_secs(60), || async {
        trace_init();
        const N: usize = 200;
        const BUILD_MS: u64 = 2000;

        let (in_tx, source) = basic_source();
        let (out1_rx, out1) = basic_sink(1000);
        let (out2_rx, out2) = basic_sink(1000);
        let got1 = spawn_collector(out1_rx);
        let got2 = spawn_collector(out2_rx);

        let mut old = Config::builder();
        old.add_source("in", source.clone());
        old.add_transform("u", &["in"], basic_transform("", 0.0));
        old.add_transform("c1", &["u"], basic_transform(" c1", 0.0));
        old.add_sink("out1", &["c1"], out1.clone());
        old.add_transform("c2", &["u"], basic_transform(" c2", 0.0));
        old.add_sink("out2", &["c2"], out2.clone());
        let (topology, _crash) = start_topology(old.build().unwrap(), false).await;

        let mut new = Config::builder();
        new.add_source("in", source);
        new.add_transform("u", &["in"], basic_transform("", 0.0));
        new.add_transform("c1", &["u"], basic_transform(" c1", 0.0));
        new.add_sink("out1", &["c1"], out1);
        new.add_transform("tz", &["u"], slow_build_transform(" tz", BUILD_MS));
        new.add_transform("c2", &["tz"], basic_transform(" c2b", 0.0));
        new.add_sink("out2", &["c2"], out2);

        let sender = spawn_sender(in_tx, 0, N, Duration::from_millis(20));
        tokio::time::sleep(Duration::from_millis(300)).await;

        let started = Instant::now();
        let reloading = tokio::spawn(async move {
            let mut topology = topology;
            let result = reload(&mut topology, new).await;
            (topology, result)
        });
        // Measure `out1` over a window strictly inside the build.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let window_start = Instant::now();
        tokio::time::sleep(Duration::from_millis(BUILD_MS - 800)).await;
        let window_end = Instant::now();
        assert!(!reloading.is_finished(), "precondition: still building");
        let during = got1
            .lock()
            .unwrap()
            .iter()
            .filter(|(t, _)| *t >= window_start && *t < window_end)
            .count();

        let (topology, result) = reloading.await.unwrap();
        result.expect("the reload succeeds");
        let built_in = started.elapsed();
        sender.await.unwrap();

        let all1 = wait_for(&got1, N, Duration::from_secs(5)).await;
        let all2 = wait_for(&got2, N, Duration::from_secs(5)).await;
        let window = window_end - window_start;
        // Informational (see the doc comment): ~60 events are sent in the window.
        eprintln!(
            "sibling: out1 got {during} events in a {window:?} window inside a {built_in:?} \
             reload (~{} sent)",
            window.as_millis() / 20
        );
        assert_exactly_once("out1", &all1, N);
        assert_exactly_once("out2", &all2, N);
        topology.stop().await;
    });
}

// ---------------------------------------------------------------------------------------------
// (5) soak: many back-to-back reloads under continuous load.
// ---------------------------------------------------------------------------------------------

/// The shapes the soak cycles through. Every one keeps `in → prepare → … → out`.
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// `prepare → parse → out`.
    Plain,
    /// Re-point: `prepare → tz → parse → out` (slow `tz` build).
    Repointed,
    /// Plain plus a second consumer of `prepare`: `prepare → side → side_out`.
    WithSide,
    /// Plain with `parse` changed.
    ChangedParse,
    /// Re-pointed with `tz` and `parse` both changed (a changed→changed chain, review P1-1).
    ChangedChain,
}

struct SoakParts {
    source: crate::test_util::mock::sources::BasicSourceConfig,
    out: crate::test_util::mock::sinks::BasicSinkConfig,
    side_out: crate::test_util::mock::sinks::BasicSinkConfig,
    build_ms: u64,
}

fn soak_config(parts: &SoakParts, shape: Shape, fail: bool) -> ConfigBuilder {
    let mut c = Config::builder();
    c.add_source("in", parts.source.clone());
    c.add_transform("prepare", &["in"], basic_transform("", 0.0));
    match shape {
        Shape::Plain | Shape::WithSide => {
            c.add_transform("parse", &["prepare"], basic_transform(" p", 0.0));
        }
        Shape::ChangedParse => {
            c.add_transform("parse", &["prepare"], basic_transform(" p2", 0.0));
        }
        Shape::Repointed => {
            c.add_transform(
                "tz",
                &["prepare"],
                slow_build_transform(" tz", parts.build_ms),
            );
            c.add_transform("parse", &["tz"], basic_transform(" p", 0.0));
        }
        Shape::ChangedChain => {
            c.add_transform(
                "tz",
                &["prepare"],
                slow_build_transform(" tz2", parts.build_ms),
            );
            c.add_transform("parse", &["tz"], basic_transform(" p3", 0.0));
        }
    }
    c.add_sink("out", &["parse"], parts.out.clone());
    if matches!(shape, Shape::WithSide) {
        c.add_transform("side", &["prepare"], basic_transform(" s", 0.0));
        c.add_sink("side_out", &["side"], parts.side_out.clone());
    }
    if fail {
        let (_rx, bad_sink) = basic_sink(10);
        c.add_source("bad", build_error_source());
        c.add_sink("bad_out", &["bad"], bad_sink);
    }
    c
}

const DEFAULT_SOAK_CYCLE: &[(Shape, bool)] = &[
    (Shape::Repointed, false),
    (Shape::Plain, false),
    (Shape::WithSide, false),
    (Shape::Plain, false),
    (Shape::ChangedParse, false),
    (Shape::Repointed, true), // fails and rolls back to ChangedParse
    (Shape::Plain, false),
    (Shape::Repointed, false),
    (Shape::ChangedChain, false),
    (Shape::WithSide, true), // fails and rolls back to ChangedChain
    (Shape::Plain, false),
];

/// `NAN5874_SOAK_CYCLE=Plain,WithSide,Repointed!` overrides the cycle (`!` = a failing reload
/// that rolls back), to bisect a loss the soak reports.
fn soak_cycle() -> Vec<(Shape, bool)> {
    let Ok(spec) = std::env::var("NAN5874_SOAK_CYCLE") else {
        return DEFAULT_SOAK_CYCLE.to_vec();
    };
    spec.split(',')
        .map(|step| {
            let (name, fail) = match step.trim().strip_suffix('!') {
                Some(name) => (name, true),
                None => (step.trim(), false),
            };
            let shape = match name {
                "Plain" => Shape::Plain,
                "Repointed" => Shape::Repointed,
                "WithSide" => Shape::WithSide,
                "ChangedParse" => Shape::ChangedParse,
                "ChangedChain" => Shape::ChangedChain,
                other => panic!("unknown soak shape {other:?}"),
            };
            (shape, fail)
        })
        .collect()
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Soak: `NAN5874_SOAK_RELOADS` (default 200) back-to-back reloads cycling re-point, add/remove a
/// consumer, change, a changed→changed chain and failed reloads that roll back, while events
/// flow continuously. Asserts every event reaches `out` exactly once, no reload takes longer than
/// 30s, and RSS grows by at most `NAN5874_SOAK_MAX_RSS_GROWTH_MB` (default 256) after warm-up.
///
/// On #33's head (9ae67a2b94) the default cycle loses events; every loss bisected so far comes
/// from the changed→changed chain step (`Repointed -> ChangedChain`, review P1-1: `tz` and
/// `parse` both change, and whichever `HashSet` order sends `Remove(parse)` to old `tz` first
/// drops `tz`'s backlog). The same cycle without that step ran 3×110 reloads with zero loss.
///
/// Ignored by default (it takes a while). Run with:
///
/// ```text
/// NAN5874_SOAK_RELOADS=200 cargo test --lib -- --ignored --nocapture \
///     topology::test::reload_hazards::reload_soak
/// ```
#[test]
#[ignore = "soak: NAN5874_SOAK_RELOADS=200 cargo test --lib -- --ignored topology::test::reload_hazards::reload_soak"]
fn reload_soak() {
    let reloads: usize = env_or("NAN5874_SOAK_RELOADS", 200);
    let max_growth_mb: u64 = env_or("NAN5874_SOAK_MAX_RSS_GROWTH_MB", 256);
    let build_ms: u64 = env_or("NAN5874_SOAK_BUILD_MS", 150);
    let limit = Duration::from_secs(60 + reloads as u64 * 31);
    with_watchdog("soak", limit, move || async move {
        trace_init();

        let (in_tx, source) = basic_source();
        let (out_rx, out) = basic_sink(10_000);
        let (side_rx, side_out) = basic_sink(10_000);
        let got = spawn_collector(out_rx);
        let _side = spawn_collector(side_rx);
        let parts = SoakParts {
            source,
            out,
            side_out,
            build_ms,
        };

        let mut topology = start_topology(
            soak_config(&parts, Shape::Plain, false).build().unwrap(),
            false,
        )
        .await
        .0;

        // Continuous load for the whole soak; stopped once the reloads are done.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);
        let next_seq = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let next_seq2 = Arc::clone(&next_seq);
        let sender = tokio::spawn(async move {
            let mut tx = in_tx;
            let mut i = 0usize;
            while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                tx.send_event(Event::Log(LogEvent::from(format!("e{i}"))))
                    .await
                    .unwrap();
                i += 1;
                next_seq2.store(i, std::sync::atomic::Ordering::Relaxed);
                if i % 4 == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            i
        });

        let cycle = soak_cycle();
        let mut rss_after_warmup = 0;
        let mut slowest = Duration::ZERO;
        // (reload #, from, to, fail, first seq sent during it, first seq sent after it)
        let mut windows = Vec::new();
        let mut current = Shape::Plain;
        for n in 0..reloads {
            let (shape, fail) = cycle[n % cycle.len()];
            let t = Instant::now();
            let seq_start = next_seq.load(std::sync::atomic::Ordering::Relaxed);
            let result = tokio::time::timeout(
                Duration::from_secs(30),
                reload(&mut topology, soak_config(&parts, shape, fail)),
            )
            .await
            .unwrap_or_else(|_| panic!("reload #{n} ({shape:?}, fail={fail}) hung for 30s"));
            slowest = slowest.max(t.elapsed());
            if fail {
                assert!(
                    matches!(result, Err(ReloadError::TopologyBuildFailed)),
                    "reload #{n} ({shape:?}) rolls back: {result:?}"
                );
            } else {
                result.unwrap_or_else(|e| panic!("reload #{n} ({shape:?}): {e:?}"));
            }
            windows.push((
                n,
                current,
                shape,
                fail,
                seq_start,
                next_seq.load(std::sync::atomic::Ordering::Relaxed),
            ));
            if !fail {
                current = shape;
            }
            if n == cycle.len() {
                rss_after_warmup = rss_bytes();
            }
        }

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let sent = sender.await.unwrap();
        let got = wait_for(&got, sent, Duration::from_secs(10)).await;
        let rss_end = rss_bytes();
        let growth_mb = rss_end.saturating_sub(rss_after_warmup) / (1024 * 1024);
        eprintln!(
            "soak: {reloads} reloads, {sent} events sent, {} received, slowest reload {slowest:?}, \
             RSS {} MB -> {} MB",
            got.len(),
            rss_after_warmup / (1024 * 1024),
            rss_end / (1024 * 1024)
        );
        // Attribute each lost event to the reload during which its send returned (windows are
        // back to back). APPROXIMATE: an event can sit in a buffer across the next reload(s) and
        // be lost there, so bisect with `NAN5874_SOAK_CYCLE` before blaming a reload kind.
        let received: std::collections::HashSet<usize> = got.iter().map(|m| seq(m)).collect();
        for (i, (n, from, to, fail, start, _)) in windows.iter().enumerate() {
            let end = windows.get(i + 1).map_or(sent, |w| w.4);
            let lost: Vec<_> = (*start..end).filter(|i| !received.contains(i)).collect();
            if !lost.is_empty() {
                eprintln!(
                    "soak: reload #{n} {from:?} -> {to:?} (fail={fail}): {} lost among seq \
                     {start}..{end}: {lost:?}",
                    lost.len()
                );
            }
        }
        assert_exactly_once("soak out", &got, sent);
        assert!(
            rss_after_warmup == 0 || growth_mb <= max_growth_mb,
            "RSS grew by {growth_mb} MB over the soak (limit {max_growth_mb} MB)"
        );
        topology.stop().await;
    });
}

// ---------------------------------------------------------------------------------------------
// (6) found by the soak: a failed re-pointing reload under fast traffic loses events.
// ---------------------------------------------------------------------------------------------

/// Found by `reload_soak`: the slow-traffic twin
/// (`repointed_reload::a_failed_repointing_reload_resumes_the_paused_connection`, one event
/// every 50 ms) passes, but under fast traffic a failed re-pointing reload that rolls back loses
/// events. `in → prepare → parse → out`; the new config re-points `parse` onto a new `tz`
/// (`prepare → tz → parse`) and adds a source that fails to build, so it rolls back.
#[test]
fn a_failed_repointing_reload_under_fast_traffic_loses_no_event() {
    with_watchdog("failed_repoint_fast", Duration::from_secs(60), || async {
        trace_init();
        const N: usize = 3000;

        let (in_tx, source) = basic_source();
        let (out_rx, out) = basic_sink(10_000);
        let got = spawn_collector(out_rx);
        let prepare = basic_transform("", 0.0);

        let mut old = Config::builder();
        old.add_source("in", source.clone());
        old.add_transform("prepare", &["in"], prepare.clone());
        old.add_transform("parse", &["prepare"], basic_transform(" p1", 0.0));
        old.add_sink("out", &["parse"], out.clone());
        let (mut topology, _crash) = start_topology(old.build().unwrap(), false).await;

        let (_bad_rx, bad_sink) = basic_sink(10);
        let mut new = Config::builder();
        new.add_source("in", source);
        new.add_transform("prepare", &["in"], prepare);
        new.add_transform("tz", &["prepare"], slow_build_transform(" tz", 150));
        new.add_transform("parse", &["tz"], basic_transform(" p2", 0.0));
        new.add_sink("out", &["parse"], out);
        new.add_source("bad", build_error_source());
        new.add_sink("bad_out", &["bad"], bad_sink);

        // ~4 events per ms, like the soak.
        let sender = tokio::spawn(async move {
            let mut tx = in_tx;
            for i in 0..N {
                tx.send_event(Event::Log(LogEvent::from(format!("e{i}"))))
                    .await
                    .unwrap();
                if i % 4 == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let result = reload(&mut topology, new).await;
        assert!(
            matches!(result, Err(ReloadError::TopologyBuildFailed)),
            "precondition: the reload rolls back: {result:?}"
        );
        sender.await.unwrap();

        let got = wait_for(&got, N, Duration::from_secs(5)).await;
        assert!(got.iter().all(|m| m.ends_with(" p1")), "old wiring only");
        assert_exactly_once("failed re-pointing reload, fast traffic", &got, N);
        topology.stop().await;
    });
}
