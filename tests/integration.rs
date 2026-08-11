use std::time::Duration;

use mq_bridge::{
    test_utils::run_test_with_docker,
    traits::{MessageDisposition, MessagePublisher},
    CanonicalMessage,
};

#[tokio::test]
#[ignore = "requires Docker"]
async fn pulsar_publisher_consumer_round_trip_commits_after_receipt() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        mq_bridge_pulsar::register().expect("register Pulsar endpoint");
        let factory = mq_bridge::extensions::get_endpoint_factory("pulsar")
            .expect("pulsar endpoint factory should be registered");
        let route_name = format!("round-trip-{}", uuid::Uuid::new_v4());
        let config = serde_json::json!({
            "url": "pulsar://localhost:6650",
            "subscription": format!("mq-bridge-test-{route_name}")
        });

        let mut consumer = factory
            .create_consumer(&route_name, &config)
            .await
            .expect("create Pulsar consumer");
        let publisher = factory
            .create_publisher(&route_name, &config)
            .await
            .expect("create Pulsar publisher");
        let expected = vec![
            CanonicalMessage::from(b"one".to_vec()),
            CanonicalMessage::from(b"two".to_vec()),
            CanonicalMessage::from(b"three".to_vec()),
        ];

        publisher
            .send_batch(expected.clone())
            .await
            .expect("publish Pulsar batch");

        let received_payloads = tokio::time::timeout(Duration::from_secs(20), async {
            let mut payloads = Vec::with_capacity(expected.len());
            while payloads.len() < expected.len() {
                let batch = consumer
                    .receive_batch(expected.len() - payloads.len())
                    .await
                    .expect("receive Pulsar batch");
                if batch.messages.is_empty() {
                    continue;
                }

                let count = batch.messages.len();
                payloads.extend(
                    batch
                        .messages
                        .iter()
                        .map(|message| message.payload.to_vec()),
                );
                (batch.commit)(vec![MessageDisposition::Ack; count])
                    .await
                    .expect("acknowledge Pulsar batch after receipt");
            }
            payloads
        })
        .await
        .expect("timed out waiting for Pulsar messages");

        assert_eq!(
            received_payloads,
            expected
                .iter()
                .map(|message| message.payload.to_vec())
                .collect::<Vec<_>>()
        );

        consumer.close().await.expect("close Pulsar consumer");
    })
    .await;
}
