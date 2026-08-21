use std::any::Any;

use anyhow::Context;
use async_trait::async_trait;
use futures::future::join_all;
use mq_bridge::{errors::PublisherError, traits::MessagePublisher, CanonicalMessage, SentBatch};
use pulsar::{
    error::Error as PulsarError,
    producer::{Message as PulsarMessage, Producer, ProducerOptions},
    TokioExecutor,
};
use tokio::sync::Mutex;

use crate::{config, connect};

struct PulsarPublisher {
    inner: Mutex<Producer<TokioExecutor>>,
}

fn to_pulsar_message(message: &CanonicalMessage) -> PulsarMessage {
    PulsarMessage {
        payload: message.payload.to_vec(),
        properties: message.metadata.clone(),
        ..Default::default()
    }
}

pub(crate) async fn create(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<Box<dyn MessagePublisher>> {
    let (config, topic, _) = config::resolve(route_name, value)?;
    let client = connect(&config.url).await?;
    let producer = client
        .producer()
        .with_topic(topic)
        .with_options(ProducerOptions {
            batch_size: Some(1_000),
            ..Default::default()
        })
        .build()
        .await
        .context("failed to create Pulsar producer")?;
    Ok(Box::new(PulsarPublisher {
        inner: Mutex::new(producer),
    }))
}

#[async_trait]
impl MessagePublisher for PulsarPublisher {
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }

        let payloads: Vec<PulsarMessage> = messages.iter().map(to_pulsar_message).collect();
        let receipts = {
            let mut producer = self.inner.lock().await;
            let receipts = producer.send_all(payloads).await.map_err(publisher_error)?;
            producer.send_batch().await.map_err(publisher_error)?;
            receipts
        };

        // Every receipt is awaited: a broker rejecting one message must not hide
        // the fate of the rest. Receipts come back in send order, so the failures
        // zip straight back onto the messages the route has to retry.
        let failed: Vec<(CanonicalMessage, PublisherError)> = join_all(receipts)
            .await
            .into_iter()
            .zip(messages)
            .filter_map(|(receipt, message)| {
                receipt.err().map(|error| (message, publisher_error(error)))
            })
            .collect();

        if failed.is_empty() {
            Ok(SentBatch::Ack)
        } else {
            Ok(SentBatch::Partial {
                responses: None,
                failed,
            })
        }
    }

    async fn flush(&self) -> anyhow::Result<()> {
        self.inner
            .lock()
            .await
            .send_batch()
            .await
            .context("failed to flush Pulsar producer")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn publisher_error(error: PulsarError) -> PublisherError {
    match error {
        PulsarError::Authentication(_) => PublisherError::NonRetryable(anyhow::Error::new(error)),
        PulsarError::Custom(_) => PublisherError::NonRetryable(anyhow::Error::new(error)),
        _ => PublisherError::Retryable(anyhow::Error::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publisher_errors_are_classified_for_retry() {
        assert!(matches!(
            publisher_error(PulsarError::Executor),
            PublisherError::Retryable(_)
        ));
        assert!(matches!(
            publisher_error(PulsarError::Custom("invalid message".into())),
            PublisherError::NonRetryable(_)
        ));
    }

    #[test]
    fn canonical_metadata_becomes_pulsar_properties() {
        let mut message = CanonicalMessage::from("payload");
        message.metadata.insert("source".into(), "test".into());

        let pulsar = to_pulsar_message(&message);

        assert_eq!(pulsar.payload, b"payload");
        assert_eq!(
            pulsar.properties.get("source").map(String::as_str),
            Some("test")
        );
    }
}
