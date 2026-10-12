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
    poll_secs: u32,
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
        poll_secs,
        // Long enough that only a release can make a message visible during a test.
        visibility_timeout_secs: 300,
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
    component: impl std::borrow::Borrow<crate::config::ComponentKey>,
    handle: tokio::task::JoinHandle<()>,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    assert!(shutdown.shutdown_source(component.borrow(), deadline).await);
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

    let (held, shutdown, component, handle) = start_holding(&queue_url, 3, 1).await;
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

    let (held, shutdown, component, handle) = start_holding(&queue_url, 3, 1).await;
    for mut event in held {
        event
            .take_finalizers()
            .update_status(EventStatus::Delivered);
    }
    shut_down(shutdown, &component, handle).await;

    assert!(visible_now(&queue_url, &sqs_client).await.is_empty());
}

/// The worst case of a shutdown: messages held downstream (acknowledgements never come) while the
/// other receiving tasks sit in long polls (`poll_secs` 20). The release still goes out at the end
/// of the acknowledgement wait and the shutdown ends within the release deadline, not after the
/// polls.
#[tokio::test]
async fn shutdown_with_long_polls_open_stays_within_the_deadlines() {
    let sqs_client = get_sqs_client().await;
    let queue_url = ensure_queue(&gen_queue_name(), &sqs_client)
        .await
        .queue_url
        .unwrap();
    send_test_events(3, &queue_url, &sqs_client).await;

    let (held, shutdown, component, handle) = start_holding(&queue_url, 3, 20).await;
    let started = tokio::time::Instant::now();
    let deadlines = super::release::ShutdownDeadlines::starting_at(started);
    shut_down(shutdown, &component, handle).await;
    assert!(
        tokio::time::Instant::now() <= deadlines.release + Duration::from_millis(500),
        "shutdown took {:?}",
        started.elapsed()
    );
    drop(held);

    let mut expected: Vec<String> = (0..3).map(calculate_message).collect();
    expected.sort();
    assert_eq!(visible_now(&queue_url, &sqs_client).await, expected);
}

/// A long poll open at shutdown is not abandoned (which could strand what the server answers it
/// with for the visibility timeout): a message it returns during the shutdown is released.
#[tokio::test]
async fn shutdown_releases_what_an_open_poll_returns() {
    let sqs_client = get_sqs_client().await;
    let queue_url = ensure_queue(&gen_queue_name(), &sqs_client)
        .await
        .queue_url
        .unwrap();
    send_test_events(1, &queue_url, &sqs_client).await;
    // One message received and acknowledged: the source is up and its polls are open.
    let (held, shutdown, component, handle) = start_holding(&queue_url, 1, 20).await;
    for mut event in held {
        use vector_lib::{event::EventStatus, finalization::Finalizable};
        event
            .take_finalizers()
            .update_status(EventStatus::Delivered);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    let shutting_down = tokio::spawn(shut_down(shutdown, component, handle));
    // Shutdown has started, and the polls opened before it are still open.
    tokio::time::sleep(Duration::from_millis(200)).await;
    send_test_events(1, &queue_url, &sqs_client).await;
    shutting_down.await.unwrap();

    // Received by the open poll, released, and received again here (a message nobody received
    // would be visible too, with a count of 1). Polls answered late by a loaded localstack can
    // pass it around a few more times before the shutdown ends.
    let visible = visible_receive_counts(&queue_url, &sqs_client).await;
    assert_eq!(visible.len(), 1, "{visible:?}");
    assert_eq!(visible[0].0, calculate_message(0));
    assert!(visible[0].1 >= 2, "{visible:?}");
}

/// Receives the messages visible right now with how many times each has been received.
async fn visible_receive_counts(
    queue_url: &str,
    client: &aws_sdk_sqs::Client,
) -> Vec<(String, u32)> {
    use aws_sdk_sqs::types::MessageSystemAttributeName;
    let output = client
        .receive_message()
        .queue_url(queue_url)
        .max_number_of_messages(10)
        .wait_time_seconds(0)
        .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
        .send()
        .await
        .unwrap();
    output
        .messages
        .unwrap_or_default()
        .into_iter()
        .map(|message| {
            let count = message
                .attributes
                .as_ref()
                .and_then(|a| a.get(&MessageSystemAttributeName::ApproximateReceiveCount))
                .and_then(|count| count.parse().ok())
                .unwrap_or(0);
            (message.body.unwrap_or_default(), count)
        })
        .collect()
}
