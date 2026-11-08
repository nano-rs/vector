//! SQS and S3 implementations of the engine's [`Queue`] and [`ObjectStore`].

use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::{
    Client as SqsClient,
    error::ProvideErrorMetadata,
    types::{MessageSystemAttributeName, QueueAttributeName},
};
use futures::StreamExt;

use super::source::{
    BoxError, ByteChunks, DepthError, ObjectStore, Queue, QueueDepth, QueueMessage,
};

pub(super) struct SqsQueue {
    pub(super) client: SqsClient,
    pub(super) queue_url: String,
    pub(super) wait_time_secs: i32,
}

#[async_trait]
impl Queue for SqsQueue {
    async fn receive(&self, max: i32, visibility_secs: i32) -> Result<Vec<QueueMessage>, BoxError> {
        let output = self
            .client
            .receive_message()
            .queue_url(&self.queue_url)
            .max_number_of_messages(max)
            .visibility_timeout(visibility_secs)
            .wait_time_seconds(self.wait_time_secs)
            .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
            .send()
            .await?;
        let mut messages = Vec::new();
        for message in output.messages.unwrap_or_default() {
            let id = message.message_id.unwrap_or_default();
            let Some(receipt) = message.receipt_handle else {
                warn!(message = "Ignoring SQS message without a receipt handle.", message_id = %id);
                continue;
            };
            let receive_count = message
                .attributes
                .as_ref()
                .and_then(|attributes| {
                    attributes.get(&MessageSystemAttributeName::ApproximateReceiveCount)
                })
                .and_then(|count| count.parse().ok())
                .unwrap_or(1);
            messages.push(QueueMessage {
                id,
                receipt,
                body: message.body.unwrap_or_default(),
                receive_count,
            });
        }
        Ok(messages)
    }

    async fn delete(&self, receipt: &str) -> Result<(), BoxError> {
        self.client
            .delete_message()
            .queue_url(&self.queue_url)
            .receipt_handle(receipt)
            .send()
            .await?;
        Ok(())
    }

    async fn change_visibility(&self, receipt: &str, secs: i32) -> Result<(), BoxError> {
        self.client
            .change_message_visibility()
            .queue_url(&self.queue_url)
            .receipt_handle(receipt)
            .visibility_timeout(secs)
            .send()
            .await?;
        Ok(())
    }

    async fn depth(&self) -> Result<QueueDepth, DepthError> {
        let output = self
            .client
            .get_queue_attributes()
            .queue_url(&self.queue_url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
            .send()
            .await
            .map_err(|error| {
                if error
                    .code()
                    .is_some_and(|code| code.contains("AccessDenied"))
                {
                    DepthError::Denied
                } else {
                    DepthError::Other(error.into())
                }
            })?;
        let read = |name: &QueueAttributeName| {
            output
                .attributes
                .as_ref()
                .and_then(|attributes| attributes.get(name))
                .and_then(|value| value.parse().ok())
        };
        Ok(QueueDepth {
            visible: read(&QueueAttributeName::ApproximateNumberOfMessages),
            not_visible: read(&QueueAttributeName::ApproximateNumberOfMessagesNotVisible),
        })
    }
}

pub(super) struct S3Store {
    pub(super) client: S3Client,
}

#[async_trait]
impl ObjectStore for S3Store {
    async fn get(&self, bucket: &str, key: &str) -> Result<ByteChunks, BoxError> {
        let output = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        let body = output.body;
        Ok(futures::stream::unfold(body, |mut body| async move {
            body.next()
                .await
                .map(|chunk| (chunk.map_err(|error| Box::new(error) as BoxError), body))
        })
        .boxed())
    }
}
