use std::{any::Any, sync::Arc, time::Duration};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use futures::TryStreamExt;
use mq_bridge::{
    errors::ConsumerError as BridgeConsumerError,
    traits::{BatchCommitFunc, BoxFuture, MessageConsumer, MessageDisposition},
    CanonicalMessage, ReceivedBatch,
};
use pulsar::{consumer::Consumer, error::Error as PulsarError, SubType, TokioExecutor};
use tokio::sync::Mutex;

use crate::{config, connect};

/// Only applied while draining, so an idle topic yields an empty batch and lets
/// `exit_on_empty` fire. Live consumption blocks until a message arrives.
const FIRST_MESSAGE_WAIT: Duration = Duration::from_millis(250);
const NEXT_MESSAGE_WAIT: Duration = Duration::from_millis(5);

type SharedConsumer = Arc<Mutex<Consumer<Vec<u8>, TokioExecutor>>>;

struct PulsarConsumer {
    inner: SharedConsumer,
    exit_on_empty: bool,
}

fn from_pulsar_message(
    payload: Vec<u8>,
    properties: impl IntoIterator<Item = (String, String)>,
) -> CanonicalMessage {
    let mut message = CanonicalMessage::from(payload);
    message.metadata.extend(properties);
    message
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
        exit_on_empty: false,
    }))
}

#[async_trait]
impl MessageConsumer for PulsarConsumer {
    fn commit_requires_order(&self) -> bool {
        false
    }

    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.exit_on_empty = exit_on_empty;
    }

    /// Closes the broker-side consumer. The trait's `close()` awaits this hook,
    /// so both route shutdown and an explicit `close()` release the subscription.
    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        Some(Box::pin(async move {
            self.inner
                .lock()
                .await
                .close()
                .await
                .context("failed to close Pulsar consumer")
        }))
    }

    async fn receive_batch(
        &mut self,
        max_messages: usize,
    ) -> Result<ReceivedBatch, BridgeConsumerError> {
        if max_messages == 0 {
            return Ok(ReceivedBatch::empty());
        }

        let exit_on_empty = self.exit_on_empty;
        let mut messages = Vec::with_capacity(max_messages);
        let mut acknowledgements = Vec::with_capacity(max_messages);
        let mut consumer = self.inner.lock().await;

        for index in 0..max_messages {
            let next = match message_wait(index, exit_on_empty) {
                Some(wait) => match tokio::time::timeout(wait, consumer.try_next()).await {
                    Ok(result) => result.map_err(consumer_error)?,
                    Err(_) => break,
                },
                None => consumer.try_next().await.map_err(consumer_error)?,
            };
            let Some(message) = next else {
                return Err(BridgeConsumerError::EndOfStream);
            };
            acknowledgements.push((message.topic.clone(), message.message_id().clone()));
            let properties = message
                .metadata()
                .properties
                .iter()
                .map(|property| (property.key.clone(), property.value.clone()))
                .collect::<Vec<_>>();
            messages.push(from_pulsar_message(message.payload.data, properties));
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

/// How long to wait for the message at `index`, or `None` to wait indefinitely.
/// A live route blocks for its first message; a draining one gives up after
/// [`FIRST_MESSAGE_WAIT`] so the empty batch can end the route.
fn message_wait(index: usize, exit_on_empty: bool) -> Option<Duration> {
    match index {
        0 if !exit_on_empty => None,
        0 => Some(FIRST_MESSAGE_WAIT),
        _ => Some(NEXT_MESSAGE_WAIT),
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

    #[test]
    fn live_consumption_waits_for_the_first_message() {
        assert_eq!(message_wait(0, false), None);
        assert_eq!(message_wait(1, false), Some(NEXT_MESSAGE_WAIT));
    }

    #[test]
    fn draining_gives_up_on_an_idle_topic() {
        assert_eq!(message_wait(0, true), Some(FIRST_MESSAGE_WAIT));
        assert_eq!(message_wait(1, true), Some(NEXT_MESSAGE_WAIT));
    }

    #[test]
    fn pulsar_properties_become_canonical_metadata() {
        let message = from_pulsar_message(
            b"payload".to_vec(),
            [("source".to_owned(), "test".to_owned())],
        );

        assert_eq!(message.get_payload_str(), "payload");
        assert_eq!(
            message.metadata.get("source").map(String::as_str),
            Some("test")
        );
    }
}
