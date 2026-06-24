# timslite for Node.js

Node.js bindings for `timslite`, a high-performance mmap-backed time-series storage engine with queues and journal support.

The package uses Node-API through `napi-rs`, so one published package can be used across supported Node.js versions without depending on V8 private ABI details.

## Installation

```bash
npm install timslite
```

Prebuilt native bindings are included for:

- macOS arm64
- Linux x64 GNU
- Linux arm64 GNU
- Windows x64 MSVC
- Windows arm64 MSVC

If the current platform does not have a prebuilt binding, the package attempts to build from source during `postinstall`. Source builds require a Rust toolchain and the platform's native C/C++ build tools. The source fallback depends on the matching `timslite` crate version from crates.io.

Set `TIMSLITE_SKIP_SOURCE_BUILD=1` to skip the source build attempt. Set `TIMSLITE_BUILD_FROM_SOURCE=1` to force a local source build even when a prebuilt binding exists.

## Usage

```js
const { Store } = require("timslite");

const store = Store.open("./data", {
  enableBackgroundThread: false,
});

const dataset = store.createDataset("metrics", "cpu");
dataset.write(1n, Buffer.from("hello"));

const record = dataset.read(1n);
if (record) {
  const [timestamp, data] = record;
  console.log(timestamp, data.toString());
}

dataset.close();
store.close();
```

Timestamps are exposed as `bigint` to preserve the Rust `i64` timestamp range.

## Development

```bash
npm install
npm run build
npm test
```

The repository build uses the local Rust crate through a path dependency. During npm publishing, the release workflow rewrites that dependency to the exact same `timslite` version on crates.io so source fallback installs can build outside the repository checkout.
