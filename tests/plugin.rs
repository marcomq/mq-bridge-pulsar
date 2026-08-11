//! The same Pulsar implementation, reached two ways.
//!
//! One conformance suite runs against the factory linked directly into this
//! test binary, and again against the factory the host builds from the compiled
//! plugin library. Identical results mean the ABI round trip preserved the
//! endpoint's behaviour — and that the Python and npm packages, which load that
//! very library, get the same endpoint.

use std::time::Duration;

use mq_bridge::plugin::conformance::{self, ConformanceOptions};
use mq_bridge::plugin::{load_endpoint_plugin, test_support::build_plugin_cdylib};
use mq_bridge::test_utils::run_test_with_docker;
use mq_bridge_pulsar::PulsarFactory;

/// Pulsar's negative-ack redelivery is delayed by the broker (a minute by
/// default), which is longer than a test should wait, so the redelivery checks
/// are left to the dedicated integration test.
fn options(topic: &str) -> ConformanceOptions {
    let mut options = ConformanceOptions::new(
        topic,
        serde_json::json!({
            "url": "pulsar://localhost:6650",
            "topic": format!("persistent://public/default/{topic}"),
            "subscription": format!("conformance-{topic}"),
        }),
    );
    options.messages = 4;
    options.receive_timeout = Duration::from_secs(30);
    options.expect_redelivery = false;
    options
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker"]
async fn the_endpoint_behaves_the_same_linked_directly_and_loaded_as_a_plugin() {
    run_test_with_docker("tests/docker-compose.yml", || async {
        let run = uuid::Uuid::new_v4();

        let direct = conformance::run(&PulsarFactory, options(&format!("direct-{run}")))
            .await
            .expect("the directly linked endpoint should pass conformance");

        let library = build_plugin_cdylib(".", "mq-bridge-pulsar").expect("build the plugin");
        let info = load_endpoint_plugin(&library).expect("load the plugin");
        assert_eq!(info.name, "pulsar");
        assert!(info.supports_consumer && info.supports_publisher);
        let factory = mq_bridge::extensions::get_endpoint_factory(&info.name)
            .expect("loading a plugin registers its endpoint");

        let loaded = conformance::run(factory.as_ref(), options(&format!("plugin-{run}")))
            .await
            .expect("the plugin-loaded endpoint should pass the same suite");

        assert_eq!(direct, loaded);
    })
    .await;
}
