//! NAN-4936 (nano-rs/vector): a reload whose build fails must roll back without leaving a
//! fanout control message that names a sink the fanout no longer has.
//!
//! The failing shape, as nano hit it: `combined` reads from `prepare`; a new config moves
//! `combined` off `prepare` (a parser publish) AND adds a source that cannot build (a refused
//! credential). `shutdown_diff` sends `prepare`'s fanout `Remove(combined)` (the new config no
//! longer feeds `combined` from `prepare`). The build fails, the rollback reconnects the OLD
//! config, and `setup_inputs` used to decide "was it paused?" from `self.config` (the old config,
//! where `prepare` IS an input), so it sent `Replace(combined)`. `Fanout::replace` then panicked
//! ("Replacing unknown sink from fanout: combined") on the next event through `prepare`, and
//! Vector exited.

use std::time::Duration;

use futures::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use vector_lib::event::{Event, EventContainer, LogEvent};

use crate::{
    config::Config,
    extra_context::ExtraContext,
    test_util::{
        mock::{basic_sink, basic_source, basic_transform, build_error_source},
        start_topology, trace_init,
    },
    topology::ReloadError,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_reload_that_rewires_an_unchanged_upstream_rolls_back_without_panicking() {
    trace_init();

    let (mut in_tx, source) = basic_source();
    let (mut out_rx, sink) = basic_sink(10);
    let prepare = basic_transform("", 0.0);
    let combined = basic_transform(" combined", 0.0);

    let mut old = Config::builder();
    old.add_source("in", source.clone());
    old.add_transform("prepare", &["in"], prepare.clone());
    old.add_transform("combined", &["prepare"], combined.clone());
    old.add_sink("out", &["combined"], sink.clone());
    let (mut topology, crash) = start_topology(old.build().unwrap(), false).await;
    let mut crash = UnboundedReceiverStream::new(crash);

    // `combined` drops `prepare` (an unchanged upstream) for `in`, and a source that cannot
    // build is added: the reload must fail and roll back.
    let (_bad_tx, bad_sink) = basic_sink(10);
    let mut new = Config::builder();
    new.add_source("in", source);
    new.add_transform("prepare", &["in"], prepare);
    new.add_transform("combined", &["in"], combined);
    new.add_sink("out", &["combined"], sink);
    new.add_source("bad", build_error_source());
    new.add_sink("bad_out", &["bad"], bad_sink);
    let result = topology
        .reload_config_and_respawn(new.build().unwrap(), ExtraContext::default())
        .await;
    assert!(
        matches!(result, Err(ReloadError::TopologyBuildFailed)),
        "the reload must fail and restore the old config: {result:?}"
    );

    // The panic surfaced lazily: on the next event through `prepare`.
    in_tx
        .send_event(Event::Log(LogEvent::from("after rollback")))
        .await
        .unwrap();

    let delivered = tokio::time::timeout(Duration::from_secs(5), out_rx.next()).await;
    let crashed = tokio::time::timeout(Duration::from_millis(500), crash.next()).await;
    assert!(
        crashed.is_err(),
        "the restored topology crashed (Fanout::replace on a removed sink): {crashed:?}"
    );
    let delivered = delivered
        .expect("the event never reached the sink after the rollback")
        .expect("sink stream ended");
    assert_eq!(delivered.events.len(), 1);
    let log = delivered.events.into_events().next().unwrap().into_log();
    let message_key = crate::config::log_schema()
        .message_key_target_path()
        .expect("message key");
    let message = log.get(message_key).unwrap().to_string_lossy().into_owned();
    assert_eq!(
        message, "after rollback combined",
        "the OLD wiring (in -> prepare -> combined) is back"
    );

    topology.stop().await;
}

/// Same rollback, for a component the failed config REMOVED: `remove_inputs` removed (not
/// paused) its connections, so the rollback must re-add them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_reload_that_removes_a_component_rolls_back_without_panicking() {
    trace_init();

    let (mut in_tx, source) = basic_source();
    let (mut out_rx, sink) = basic_sink(10);
    let prepare = basic_transform("", 0.0);
    let combined = basic_transform(" combined", 0.0);

    let mut old = Config::builder();
    old.add_source("in", source.clone());
    old.add_transform("prepare", &["in"], prepare.clone());
    old.add_transform("combined", &["prepare"], combined);
    old.add_sink("out", &["combined"], sink.clone());
    let (mut topology, crash) = start_topology(old.build().unwrap(), false).await;
    let mut crash = UnboundedReceiverStream::new(crash);

    let (_bad_tx, bad_sink) = basic_sink(10);
    let mut new = Config::builder();
    new.add_source("in", source);
    new.add_transform("prepare", &["in"], prepare);
    new.add_sink("out", &["prepare"], sink);
    new.add_source("bad", build_error_source());
    new.add_sink("bad_out", &["bad"], bad_sink);
    let result = topology
        .reload_config_and_respawn(new.build().unwrap(), ExtraContext::default())
        .await;
    assert!(
        matches!(result, Err(ReloadError::TopologyBuildFailed)),
        "{result:?}"
    );

    in_tx
        .send_event(Event::Log(LogEvent::from("after rollback")))
        .await
        .unwrap();
    let delivered = tokio::time::timeout(Duration::from_secs(5), out_rx.next()).await;
    let crashed = tokio::time::timeout(Duration::from_millis(500), crash.next()).await;
    assert!(
        crashed.is_err(),
        "the restored topology crashed: {crashed:?}"
    );
    assert!(
        matches!(delivered, Ok(Some(_))),
        "the event never reached the sink after the rollback"
    );

    topology.stop().await;
}
