use std::{any::Any, sync::Arc, time::Duration};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use futures::TryStreamExt;
use mq_bridge::{
    errors::ConsumerError as BridgeConsumerError,
    traits::{BatchCommitFunc, MessageConsumer, MessageDisposition},
    CanonicalMessage, ReceivedBatch,
};
use pulsar::{consumer::Consumer, error::Error as PulsarError, SubType, TokioExecutor};
use tokio::sync::Mutex;

use crate::{config, connect};

const FIRST_MESSAGE_WAIT: Duration = Duration::from_millis(250);
const NEXT_MESSAGE_WAIT: Duration = Duration::from_millis(5);

type SharedConsumer = Arc<Mutex<Consumer<Vec<u8>, TokioExecutor>>>;

struct PulsarConsumer {
    inner: SharedConsumer,
}

pub(crate) async fn create(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<Box<dyn MessageConsumer>> {
    let (config, topic, subscription) = config::resolve(route_name, value)?;
    let client = connect(&config.url).await?;
    let consumer = client
        .consumer()
        .with_topic(topic)
        .with_consumer_name(format!("mq-bridge-{route_name}"))
        .with_subscription_type(SubType::Shared)
        .with_subscription(subscription)
        .build::<Vec<u8>>()
        .await
        .context("failed to create Pulsar consumer")?;
    Ok(Box::new(PulsarConsumer {
        inner: Arc::new(Mutex::new(consumer)),
    }))
}

#[async_trait]
impl MessageConsumer for PulsarConsumer {
    fn commit_requires_order(&self) -> bool {
        false
    }

    async fn receive_batch(
        &mut self,
        max_messages: usize,
    ) -> Result<ReceivedBatch, BridgeConsumerError> {
        if max_messages == 0 {
            return Ok(ReceivedBatch::empty());
        }

        let mut messages = Vec::with_capacity(max_messages);
        let mut acknowledgements = Vec::with_capacity(max_messages);
        let mut consumer = self.inner.lock().await;

        for index in 0..max_messages {
            let wait = if index == 0 {
                FIRST_MESSAGE_WAIT
            } else {
                NEXT_MESSAGE_WAIT
            };
            let next = match tokio::time::timeout(wait, consumer.try_next()).await {
                Ok(result) => result.map_err(consumer_error)?,
                Err(_) => break,
            };
            let Some(message) = next else {
                return Err(BridgeConsumerError::EndOfStream);
            };
            acknowledgements.push((message.topic.clone(), message.message_id().clone()));
            messages.push(CanonicalMessage::from(message.payload.data));
        }
        drop(consumer);

        if messages.is_empty() {
            return Ok(ReceivedBatch::empty());
        }

        let expected = messages.len();
        let shared = Arc::clone(&self.inner);
        let commit: BatchCommitFunc = Box::new(move |dispositions| {
            Box::pin(async move {
                validate_disposition_count(expected, dispositions.len())?;
                let mut consumer = shared.lock().await;
                for ((topic, id), disposition) in acknowledgements.into_iter().zip(dispositions) {
                    let result = match disposition {
                        MessageDisposition::Nack => consumer.nack_with_id(&topic, id).await,
                        _ => consumer.ack_with_id(&topic, id).await,
                    };
                    result.context("failed to commit Pulsar message disposition")?;
                }
                Ok(())
            })
        });
        Ok(ReceivedBatch { messages, commit })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn consumer_error(error: PulsarError) -> BridgeConsumerError {
    BridgeConsumerError::Connection(anyhow::Error::new(error))
}

fn validate_disposition_count(expected: usize, actual: usize) -> anyhow::Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(anyhow!(
            "Pulsar batch commit received {actual} dispositions for {expected} messages"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_commit_requires_one_disposition_per_message() {
        assert!(validate_disposition_count(2, 2).is_ok());
        assert!(validate_disposition_count(2, 1).is_err());
    }
}
