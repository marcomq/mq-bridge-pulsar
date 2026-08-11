# mq-bridge-pulsar (Python)

An Apache Pulsar endpoint for [mq-bridge-py](https://pypi.org/project/mq-bridge-py/),
shipped as a native plugin. The package contains no Python implementation of
Pulsar — it bundles the compiled Rust endpoint and registers it with mq-bridge,
so Python, Node.js and Rust all run the same code and the same delivery
semantics.

```console
pip install mq-bridge-py mq-bridge-pulsar
```

```python
import mq_bridge
import mq_bridge_pulsar

mq_bridge_pulsar.register()   # call once, before starting routes

route = mq_bridge.Route.from_str("""
pulsar_to_file:
  input:
    custom:
      name: pulsar
      config:
        url: "pulsar://localhost:6650"
        topic: "persistent://public/default/orders"
        subscription: "order-workers"
  output:
    file:
      path: "orders.jsonl"
""")
route.start()
```

`register()` returns the endpoint name (`pulsar`) and is a no-op when called
again. It raises `ImportError` if mq-bridge is missing and `FileNotFoundError`
if the wheel does not carry a library for this platform.

The two packages are independent: mq-bridge has no Pulsar dependency, and
neither package forces an upgrade of the other. A plugin is native code with the
same privileges as the interpreter — install it as you would any other native
dependency.

The generic wheel builder is supplied by mq-bridge:

```console
pip install "mq-bridge-py[plugin-packaging]"
python -m mq_bridge.plugin_packaging --package python/mq_bridge_pulsar --out dist
```

## Building the wheel

The generic builder shipped by mq-bridge builds the Rust `cdylib`, stages it
into the package, and tags the wheel for the host platform:

```console
python -m mq_bridge.plugin_packaging --package python/mq_bridge_pulsar --out dist
```

Run it once per operating system and architecture you publish for; building
`pulsar-rs` needs `protoc` on `PATH`.
