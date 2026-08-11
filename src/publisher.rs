use std::any::Any;

use anyhow::Context;
use async_trait::async_trait;
use futures::future::try_join_all;
use mq_bridge::{errors::PublisherError, traits::MessagePublisher, CanonicalMessage, SentBatch};
use pulsar::{
    error::Error as PulsarError,
    producer::{Producer, ProducerOptions},
    TokioExecutor,
};
use tokio::sync::Mutex;

use crate::{config, connect};

struct PulsarPublisher {
    inner: Mutex<Producer<TokioExecutor>>,
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

        let payloads = messages.into_iter().map(|message| message.payload.to_vec());
        let receipts = {
            let mut producer = self.inner.lock().await;
            let receipts = producer.send_all(payloads).await.map_err(publisher_error)?;
            producer.send_batch().await.map_err(publisher_error)?;
            receipts
        };
        try_join_all(receipts).await.map_err(publisher_error)?;
        Ok(SentBatch::Ack)
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
}
