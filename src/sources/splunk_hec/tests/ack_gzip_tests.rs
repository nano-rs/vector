//! NAN-4976: indexer-ack queries with `Content-Encoding: gzip`.
//!
//! Vector's own `splunk_hec_logs` sink compresses its `/services/collector/ack`
//! poll with the sink's `compression` (gzip in most shipped configs) and sets
//! `Content-Encoding`. The source parsed the raw bytes as JSON and answered 400,
//! so every such sender logged `indexer_ack_failed` and fell back to treating
//! the plain 200 as delivery.

use std::io::Write;

use flate2::{Compression as GzLevel, write::GzEncoder};
use futures_util::StreamExt;
use vector_lib::event::{BatchNotifier, BatchStatus};

use std::num::NonZeroU8;

use super::*;

fn gzip(body: &[u8]) -> Vec<u8> {
    let mut enc = GzEncoder::new(Vec::new(), GzLevel::default());
    enc.write_all(body).unwrap();
    enc.finish().unwrap()
}

async fn post_ack(address: SocketAddr, body: Vec<u8>, encoding: Option<&str>) -> reqwest::Response {
    let mut req = reqwest::Client::new()
        .post(format!("http://{address}/services/collector/ack"))
        .header("Authorization", format!("Splunk {TOKEN}"))
        .header("X-Splunk-Request-Channel", "guid")
        .header("Content-Type", "application/json");
    if let Some(ce) = encoding {
        req = req.header("Content-Encoding", ce);
    }
    req.body(body).send().await.unwrap()
}

async fn acked_event(address: SocketAddr, source: impl Stream<Item = Event> + Unpin) -> u64 {
    let opts = SendWithOpts {
        channel: Some(Channel::Header("guid")),
        forwarded_for: None,
    };
    let res = send_with_response(
        address,
        "services/collector/event",
        r#"{"event":"gz-ack"}"#,
        TOKEN,
        &opts,
    )
    .await
    .json::<HecAckEventResponse>()
    .await
    .unwrap();
    // Dropping the collected event finalizes it as delivered.
    let _ = collect_n(source, 1).await;
    res.ack_id
}

fn acks_enabled() -> Option<HecAcknowledgementsConfig> {
    Some(HecAcknowledgementsConfig {
        enabled: Some(true),
        ..Default::default()
    })
}

#[tokio::test]
async fn ack_service_accepts_gzip_encoded_query() {
    let (source, address, _guard) = source(acks_enabled()).await;
    let ack_id = acked_event(address, source).await;

    let body = serde_json::to_vec(&HecAckStatusRequest { acks: vec![ack_id] }).unwrap();
    // The ack resolves asynchronously after finalization; poll briefly.
    let mut acked = false;
    for _ in 0..50 {
        let res = post_ack(address, gzip(&body), Some("gzip")).await;
        assert_eq!(200, res.status().as_u16(), "gzip ack query must not be refused");
        let parsed: HecAckStatusResponse = res.json().await.unwrap();
        if *parsed.acks.get(&ack_id).unwrap() {
            acked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(acked, "a delivered event's ackId reports true over a gzip query");
}

#[tokio::test]
async fn ack_service_rejects_corrupt_gzip_and_unknown_encoding() {
    let (_source, address, _guard) = source(acks_enabled()).await;
    let body = serde_json::to_vec(&HecAckStatusRequest { acks: vec![0] }).unwrap();

    // Claims gzip but is plain JSON: a bad request, not a silent parse.
    assert_eq!(400, post_ack(address, body.clone(), Some("gzip")).await.status().as_u16());
    // Unsupported encodings are refused like on the event endpoint.
    assert_eq!(415, post_ack(address, body, Some("zstd")).await.status().as_u16());
}

/// The real `splunk_hec_logs` sink, gzip-compressed, polling acks fast.
async fn gzip_sink_polling_acks(address: SocketAddr) -> VectorSink {
    let (sink, health) = HecLogsSinkConfig {
        default_token: TOKEN.to_owned().into(),
        endpoint: HttpEndpoint::parse(&format!("http://{address}")).unwrap(),
        host_key: None,
        indexed_fields: vec![],
        index: None,
        sourcetype: None,
        source: None,
        encoding: TextSerializerConfig::default().into(),
        compression: Compression::gzip_default(),
        batch: BatchConfig::default(),
        request: TowerRequestConfig::default(),
        tls: None,
        acknowledgements: HecClientAcknowledgementsConfig {
            indexer_acknowledgements_enabled: true,
            query_interval: NonZeroU8::new(1).unwrap(),
            retry_limit: NonZeroU8::new(3).unwrap(),
            ..Default::default()
        },
        timestamp_nanos_key: None,
        timestamp_key: None,
        auto_extract_timestamp: None,
        endpoint_target: EndpointTarget::default(),
        confinement: ConfinementConfig::default(),
    }
    .build(SinkContext::default())
    .await
    .unwrap();
    assert!(health.await.is_ok());
    sink
}

/// Sends `n` events through the gzip sink, finalizes them at the source with
/// `downstream`, and returns the status the SENDER's batch resolved to.
async fn sender_status_when_source_finalizes(n: usize, downstream: EventStatus) -> BatchStatus {
    // The shared `source()` helper finalizes everything Delivered; this one
    // finalizes with `downstream`, standing in for the sinks behind the source.
    let (sender, source) = SourceSender::new_test_finalize(downstream);
    let (_guard, address) = next_addr();
    let cx = SourceContext::new_test(sender, None);
    tokio::spawn(async move {
        SplunkConfig {
            address,
            token: Some(TOKEN.to_owned().into()),
            acknowledgements: acks_enabled().unwrap(),
            ..Default::default()
        }
        .build(cx)
        .await
        .unwrap()
        .await
        .unwrap();
    });
    wait_for_tcp(address).await;
    let sink = gzip_sink_polling_acks(address).await;
    let (batch, receiver) = BatchNotifier::new_with_receiver();
    let events: Vec<Event> = (0..n)
        .map(|i| Event::Log(LogEvent::from(format!("gz-{i}")).with_batch_notifier(&batch)))
        .collect();
    drop(batch);
    tokio::spawn(async move { sink.run_events(events).await.unwrap() });
    let got: Vec<Event> = source.take(n).collect().await;
    assert_eq!(n, got.len());
    drop(got);
    tokio::time::timeout(std::time::Duration::from_secs(30), receiver)
        .await
        .expect("the sender's batch resolves within the ack retry window")
}

#[tokio::test]
async fn gzip_sink_indexer_ack_resolves_delivered() {
    // The source delivered every event downstream: the sink's gzip ack poll
    // reports them acked and the sender's batch resolves Delivered.
    assert_eq!(
        BatchStatus::Delivered,
        sender_status_when_source_finalizes(5, EventStatus::Delivered).await
    );
}

#[tokio::test]
async fn gzip_sink_indexer_ack_reports_undelivered() {
    // Discriminating half: downstream did NOT deliver. With working acks the
    // ackId never reports true and the sink expires it as Rejected. While the
    // gzip ack poll was answered 400 the sink fell back to "Delivered on the
    // 200", which is exactly the weaker evidence NAN-4976 is about.
    assert_eq!(
        BatchStatus::Rejected,
        sender_status_when_source_finalizes(3, EventStatus::Errored).await
    );
}
