#![cfg(feature = "aws-sqs-integration-tests")]
#![cfg(test)]

use std::{collections::HashSet, time::Duration};

use aws_sdk_sqs::operation::create_queue::CreateQueueOutput;
use aws_types::region::Region;
use futures::StreamExt;
use tokio::time::timeout;

use crate::{
    SourceSender,
    aws::{RegionOrEndpoint, auth::AwsAuthentication, create_client},
    common::sqs::SqsClientBuilder,
    config::{SourceConfig, SourceContext, log_schema},
    event::Event,
    sources::aws_sqs::config::AwsSqsConfig,
    test_util::{
        components::{HTTP_PULL_SOURCE_TAGS, assert_source_compliance},
        random_string,
    },
};

fn sqs_address() -> String {
    std::env::var("SQS_ADDRESS").unwrap_or_else(|_| "http://localhost:4566".into())
}

fn gen_queue_name() -> String {
    random_string(10).to_lowercase()
}

async fn ensure_queue(queue_name: &str, client: &aws_sdk_sqs::Client) -> CreateQueueOutput {
    client
        .create_queue()
        .queue_name(queue_name)
        .send()
        .await
        .unwrap()
}

async fn send_test_events(count: u32, queue_url: &str, client: &aws_sdk_sqs::Client) {
    for i in 0..count {
        client
            .send_message()
            .message_body(calculate_message(i))
            .queue_url(queue_url)
            .send()
            .await
            .unwrap();
    }
}

async fn get_sqs_client() -> aws_sdk_sqs::Client {
    create_client::<SqsClientBuilder>(
        &SqsClientBuilder {},
        &AwsAuthentication::test_auth(),
        Some(Region::new("us-east-1")),
        Some(sqs_address()),
        &Default::default(),
        None,
        None,
    )
    .await
    .unwrap()
}

#[tokio::test]
pub(crate) async fn test() {
    assert_source_compliance(&HTTP_PULL_SOURCE_TAGS, async {
        let sqs_client = get_sqs_client().await;
        let queue_name = gen_queue_name();
        let queue_url = ensure_queue(&queue_name, &sqs_client)
            .await
            .queue_url
            .expect("Create queue should return the url");

        let num_events = 3;
        send_test_events(num_events, &queue_url, &sqs_client).await;

        let config = AwsSqsConfig {
            region: RegionOrEndpoint::with_both("us-east-1", sqs_address().as_str()),
            auth: AwsAuthentication::test_auth(),
            queue_url: queue_url.clone(),
            ..Default::default()
        };

        let (tx, rx) = SourceSender::new_test();
        tokio::spawn(async move {
            config
                .build(SourceContext::new_test(tx, None))
                .await
                .unwrap()
                .await
                .unwrap()
        });

        let mut expected_messages = HashSet::new();
        for i in 0..num_events {
            expected_messages.insert(calculate_message(i));
        }

        let events: Vec<Event> = timeout(
            Duration::from_secs(10),
            rx.take(num_events as usize).collect(),
        )
        .await
        .unwrap();

        for event in events {
            let message = event
                .as_log()
                .get(log_schema().message_key_target_path().unwrap())
                .unwrap()
                .to_string_lossy();
            if !expected_messages.remove(message.as_ref()) {
                panic!("Received unexpected message: {message:?}");
            }
        }
        assert!(expected_messages.is_empty());
    })
    .await;
}

fn calculate_message(index: u32) -> String {
    format!("Test message: {index}")
}

/// Starts the source with end-to-end acknowledgements, receives `count` events, and returns them
/// unacknowledged with the means to shut the source down.
async fn start_holding(
    queue_url: &str,
    count: usize,
) -> (
    Vec<Event>,
    crate::shutdown::SourceShutdownCoordinator,
    crate::config::ComponentKey,
    tokio::task::JoinHandle<()>,
) {
    let config = AwsSqsConfig {
        region: RegionOrEndpoint::with_both("us-east-1", sqs_address().as_str()),
        auth: AwsAuthentication::test_auth(),
        queue_url: queue_url.to_string(),
        poll_secs: 1,
        ..Default::default()
    };
    let component = crate::config::ComponentKey::from("aws_sqs");
    let (tx, rx) = SourceSender::new_test();
    let (mut cx, shutdown) = SourceContext::new_shutdown(&component, tx);
    cx.acknowledgements = true;
    let source = config.build(cx).await.unwrap();
    let handle = tokio::spawn(async move { source.await.unwrap() });
    let events: Vec<Event> = timeout(Duration::from_secs(10), rx.take(count).collect())
        .await
        .unwrap();
    (events, shutdown, component, handle)
}

async fn shut_down(
    mut shutdown: crate::shutdown::SourceShutdownCoordinator,
    component: &crate::config::ComponentKey,
    handle: tokio::task::JoinHandle<()>,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    assert!(shutdown.shutdown_source(component, deadline).await);
    handle.await.unwrap();
}

/// Messages visible right now (not waiting out a visibility timeout).
async fn visible_now(queue_url: &str, client: &aws_sdk_sqs::Client) -> Vec<String> {
    let mut bodies = Vec::new();
    loop {
        let output = client
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(0)
            .send()
            .await
            .unwrap();
        let messages = output.messages.unwrap_or_default();
        if messages.is_empty() {
            break;
        }
        bodies.extend(messages.into_iter().filter_map(|m| m.body));
    }
    bodies.sort();
    bodies
}

/// Shutting down with messages sent downstream but not acknowledged: they are neither deleted
/// nor left invisible for the visibility timeout (300 s by default), but released at once.
#[tokio::test]
async fn shutdown_releases_unacknowledged_messages() {
    let sqs_client = get_sqs_client().await;
    let queue_url = ensure_queue(&gen_queue_name(), &sqs_client)
        .await
        .queue_url
        .unwrap();
    send_test_events(3, &queue_url, &sqs_client).await;

    let (held, shutdown, component, handle) = start_holding(&queue_url, 3).await;
    shut_down(shutdown, &component, handle).await;
    drop(held);

    let mut expected: Vec<String> = (0..3).map(calculate_message).collect();
    expected.sort();
    assert_eq!(visible_now(&queue_url, &sqs_client).await, expected);
}

/// Acknowledged messages are deleted as before, and nothing is released.
#[tokio::test]
async fn shutdown_deletes_acknowledged_messages() {
    use vector_lib::{event::EventStatus, finalization::Finalizable};

    let sqs_client = get_sqs_client().await;
    let queue_url = ensure_queue(&gen_queue_name(), &sqs_client)
        .await
        .queue_url
        .unwrap();
    send_test_events(3, &queue_url, &sqs_client).await;

    let (held, shutdown, component, handle) = start_holding(&queue_url, 3).await;
    for mut event in held {
        event
            .take_finalizers()
            .update_status(EventStatus::Delivered);
    }
    shut_down(shutdown, &component, handle).await;

    assert!(visible_now(&queue_url, &sqs_client).await.is_empty());
}
