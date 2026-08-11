use anyhow::{anyhow, Context};
use serde::Deserialize;

/// Configuration accepted by an endpoint named `pulsar`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PulsarConfig {
    pub url: String,
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub subscription: Option<String>,
}

pub(crate) fn resolve(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<(PulsarConfig, String, String)> {
    let config: PulsarConfig =
        serde_json::from_value(value.clone()).context("invalid Pulsar endpoint configuration")?;
    if config.url.trim().is_empty() {
        return Err(anyhow!("Pulsar `url` must not be empty"));
    }
    let topic = config
        .topic
        .clone()
        .unwrap_or_else(|| route_name.to_owned());
    if topic.trim().is_empty() {
        return Err(anyhow!("Pulsar `topic` must not be empty"));
    }
    let subscription = config
        .subscription
        .clone()
        .unwrap_or_else(|| format!("mq-bridge-{route_name}"));
    if subscription.trim().is_empty() {
        return Err(anyhow!("Pulsar `subscription` must not be empty"));
    }
    Ok((config, topic, subscription))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_topic_and_subscription_from_route_name() {
        let value = serde_json::json!({"url": "pulsar://localhost:6650"});
        let (_, topic, subscription) = resolve("orders", &value).unwrap();
        assert_eq!(topic, "orders");
        assert_eq!(subscription, "mq-bridge-orders");
    }

    #[test]
    fn explicit_topic_and_subscription_win() {
        let value = serde_json::json!({
            "url": "pulsar://localhost:6650",
            "topic": "persistent://public/default/input",
            "subscription": "workers"
        });
        let (_, topic, subscription) = resolve("route", &value).unwrap();
        assert_eq!(topic, "persistent://public/default/input");
        assert_eq!(subscription, "workers");
    }

    #[test]
    fn invalid_configuration_is_rejected_before_connecting() {
        assert!(resolve("route", &serde_json::json!({})).is_err());
        assert!(resolve("route", &serde_json::json!({"url": ""})).is_err());
        assert!(resolve(
            "route",
            &serde_json::json!({"url": "pulsar://localhost:6650", "extra": true})
        )
        .is_err());
    }
}
