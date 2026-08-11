#[tokio::main]
async fn main() -> anyhow::Result<()> {
    const ROUTE_NAME: &str = "pulsar_to_file";
    mq_bridge_pulsar::register()?;
    let document: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string("examples/pulsar_to_file.yaml")?)?;
    let route = document
        .get("routes")
        .and_then(|routes| routes.get(ROUTE_NAME))
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("route '{ROUTE_NAME}' is missing"))?;
    let handle = serde_yaml::from_value::<mq_bridge::Route>(route)?
        .run(ROUTE_NAME)
        .await?;
    handle.join().await?;
    Ok(())
}
