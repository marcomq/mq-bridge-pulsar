# mq-bridge-pulsar

An external [mq-bridge](https://github.com/marcomq/mq-bridge) endpoint for
Apache Pulsar, implemented with the pure-Rust `pulsar` client. It supports both
Pulsar inputs and outputs without adding Pulsar dependencies to mq-bridge.

Building `pulsar-rs` requires the Protocol Buffers compiler (`protoc`) to be on
`PATH` (for example, `brew install protobuf` on macOS).

## Configuration

Register the endpoint before any route starts:

```rust
mq_bridge_pulsar::register();
```

Then use the explicit custom endpoint form:

```yaml
input:
  custom:
    name: pulsar
    config:
      url: "pulsar://localhost:6650"
      topic: "persistent://public/default/orders" # optional; route name by default
      subscription: "order-workers"              # optional; mq-bridge-<route> by default
```

Consumers use a shared subscription. Messages are acknowledged or negatively
acknowledged only from mq-bridge's batch commit callback, after downstream
processing supplies its dispositions. Publishers enqueue the complete batch,
flush Pulsar's producer batch once, and then await all receipts concurrently.

## Example

With a Pulsar broker listening on localhost:

```console
cargo run --features example-app --example pulsar_to_file
```

The runnable route is in `examples/pulsar_to_file.yaml`. The example's topic is
omitted intentionally, so it resolves to the route name `pulsar_to_file`.

## Use it from any mq-bridge process

The crate also builds a `cdylib` — the same endpoint as a native plugin — so a
host that never compiled against it can load it at runtime:

```rust
mq_bridge::plugin::load_endpoint_plugin("./libmq_bridge_pulsar.so")?;
```

Python and Node.js users install two independent packages; neither reimplements
Pulsar, both ship this library and hand its path to mq-bridge's generic loader.

```console
pip install mq-bridge mq-bridge-pulsar
```

```python
import mq_bridge_pulsar

mq_bridge_pulsar.register()   # once, before starting routes
```

```console
npm install mq-bridge mq-bridge-pulsar
```

```javascript
import { register } from "mq-bridge-pulsar";

register(); // once, before starting routes
```

The configuration is the same in every language (`name: pulsar`). See
[PLUGINS.md](https://github.com/marcomq/mq-bridge/blob/main/docs/PLUGINS.md) for
how loading, versioning and the ABI work.

### Packaging

Python publishes one platform wheel per target under the same distribution
name. The npm release is a single package containing all staged binaries under
`node/prebuilds/`. Build on each target, merge those directories, then pack once:

```console
pip install "mq-bridge-py[plugin-packaging]"
python -m mq_bridge.plugin_packaging --package python/mq_bridge_pulsar --out dist
mq-bridge-package-plugin
mq-bridge-package-plugin --pack --out npm
```

Both need `protoc` on `PATH`, like any build of this crate.

## Integration tests

Both Docker-backed tests are ignored by default:

```console
cargo test --test integration -- --ignored --nocapture
cargo test --test plugin -- --ignored --nocapture
```

The first starts a Pulsar standalone broker, publishes a batch through the
registered factory, consumes it through the same extension, verifies payload
order, and only then invokes the batch commit callback. The second runs
mq-bridge's endpoint conformance suite twice against the same broker — once
against the directly linked factory, once against the factory loaded from the
compiled plugin — and requires the results to match.
