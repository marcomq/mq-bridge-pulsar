//! Apache Pulsar input/output endpoint extension for `mq-bridge`.
//!
//! The same implementation is used three ways:
//!
//! * linked directly by a Rust program, which calls [`register`];
//! * loaded from the compiled `cdylib` by any mq-bridge host through
//!   `mq_bridge::plugin::load_endpoint_plugin`;
//! * from Python or Node.js, whose `mq-bridge-pulsar` packages ship that same
//!   library and call the host's generic loader.

mod config;
mod consumer;
mod publisher;

use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use mq_bridge::traits::{CustomEndpointFactory, MessageConsumer, MessagePublisher};
use pulsar::{Pulsar, TokioExecutor};

pub use config::PulsarConfig;

#[derive(Debug, Default)]
pub struct PulsarFactory;

// Exports the same factory as a loadable plugin. `register()` below covers the
// directly linked case; this covers every host that loads the compiled library,
// including the Python and Node.js packages.
#[cfg(feature = "plugin")]
mq_bridge::export_endpoint_plugin! {
    name: "pulsar",
    factory: PulsarFactory,
}

/// Registers this crate's factory under `pulsar`. Call once, before starting
/// routes that use it. Only needed when linking this crate directly; a host that
/// loads the compiled plugin registers the endpoint as part of loading it.
pub fn register() -> anyhow::Result<()> {
    mq_bridge::extensions::register_endpoint_factory("pulsar", Arc::new(PulsarFactory))
}

async fn connect(url: &str) -> anyhow::Result<Pulsar<TokioExecutor>> {
    Pulsar::builder(url, TokioExecutor)
        .build()
        .await
        .with_context(|| format!("failed to connect to Pulsar at {url}"))
}

#[async_trait]
impl CustomEndpointFactory for PulsarFactory {
    async fn create_consumer(
        &self,
        route_name: &str,
        value: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessageConsumer>> {
        consumer::create(route_name, value).await
    }

    async fn create_publisher(
        &self,
        route_name: &str,
        value: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessagePublisher>> {
        publisher::create(route_name, value).await
    }
}
