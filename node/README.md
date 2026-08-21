# mq-bridge-pulsar (Node.js)

An Apache Pulsar endpoint for [mq-bridge](https://www.npmjs.com/package/mq-bridge),
shipped as a native plugin. The package contains no JavaScript implementation of
Pulsar — it loads the compiled Rust endpoint into mq-bridge, so Node.js, Python
and Rust all run the same code and the same delivery semantics.

```console
npm install mq-bridge mq-bridge-pulsar
```

```javascript
import { Route } from "mq-bridge";
import { register } from "mq-bridge-pulsar";

register(); // call once, before starting routes

const route = Route.fromStr(`
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
`);
route.start();
route.join(); // block until the route stops
```

`register()` returns the endpoint name (`pulsar`) and is a no-op when called
again. `mq-bridge` selects the current platform's library from this package's
`prebuilds/` directory using the shared plugin-package convention.

A plugin is native code with the same privileges as the Node process — install
it as you would any other native dependency.

## Building the package

The packaging command shipped by `mq-bridge` builds the Rust `cdylib` and stages
it under the current platform tag:

```console
mq-bridge-package-plugin
```

Run it on each supported target and merge the resulting `node/prebuilds/`
directories. Then create the single tarball that is published to npm:

```console
mq-bridge-package-plugin --pack --out npm
```

Building `pulsar-rs` needs `protoc` on `PATH`.
