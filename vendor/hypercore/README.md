# Hypercore
[![crates.io version][1]][2] [![build status][3]][4]
[![downloads][5]][6] [![docs.rs docs][7]][8]

Hypercore is a secure, distributed append-only log. This crate is a limited Rust
port of the original Javascript
[holepunchto/hypercore](https://github.com/holepunchto/hypercore). The goal is to
maintain binary compatibility with the LTS version with regards to disk storage.

See [hypercore-protocol-rs](https://github.com/datrs/hypercore-protocol-rs) for the
corresponding wire protocol implementation.

- [Documentation][8]
- [Crates.io][2]

## Features

- [x] Create [in-memory](https://github.com/datrs/random-access-memory) and [disk](https://github.com/datrs/random-access-disk) hypercores
- [x] Append to hypercore either a single entry or a batch of entries
- [x] Get entries from hypercore
- [x] Clear range from hypercore, with optional support for sparse files
- [x] Support basic replication by creating proofs in a source hypercore and verifying and applying them to a destination hypercore
- [x] Support `tokio` or `async-std` runtimes
- [x] Support WASM for in-memory storage
- [x] Test Javascript interoperability for supported features
- [x] Add optional read cache
- [ ] Support the new [manifest](https://github.com/holepunchto/hypercore/blob/main/lib/manifest.js) in the wire protocol to remain compatible with upcoming v11
- [ ] Finalize documentation and release v1.0.0

## Usage

```rust
// Create an in-memory hypercore using a builder
let mut hypercore = HypercoreBuilder::new(Storage::new_memory().await.unwrap())
    .build()
    .await
    .unwrap();

// Append entries to the log
hypercore.append(b"Hello, ").await.unwrap();
hypercore.append(b"world!").await.unwrap();

// Read entries from the log
assert_eq!(hypercore.get(0).await.unwrap().unwrap(), b"Hello, ");
assert_eq!(hypercore.get(1).await.unwrap().unwrap(), b"world!");
```

Find more examples in the [examples](./examples) folder, and/or run:

```bash
cargo run --example memory
cargo run --example disk
cargo run --example replication
```

## Installation

```bash
cargo add hypercore
```

## Safety

This crate uses ``#![forbid(unsafe_code)]`` to ensure everythong is implemented in
100% Safe Rust.

## Development

To test interoperability with Javascript, enable the `js_interop_tests` feature:

```bash
cargo test --features js_interop_tests
```

Run benches with:

```bash
cargo bench
```

## Contributing

Want to join us? Check out our ["Contributing" guide][contributing] and take a
look at some of these issues:

- [Issues labeled "good first issue"][good-first-issue]
- [Issues labeled "help wanted"][help-wanted]

## License

[MIT](./LICENSE-MIT) OR [Apache-2.0](./LICENSE-APACHE)

[1]: https://img.shields.io/crates/v/hypercore.svg?style=flat-square
[2]: https://crates.io/crates/hypercore
[3]: https://github.com/datrs/hypercore/actions/workflows/ci.yml/badge.svg
[4]: https://github.com/datrs/hypercore/actions
[5]: https://img.shields.io/crates/d/hypercore.svg?style=flat-square
[6]: https://crates.io/crates/hypercore
[7]: https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square
[8]: https://docs.rs/hypercore

[releases]: https://github.com/datrs/hypercore/releases
[contributing]: https://github.com/datrs/hypercore/blob/master/.github/CONTRIBUTING.md
[good-first-issue]: https://github.com/datrs/hypercore/labels/good%20first%20issue
[help-wanted]: https://github.com/datrs/hypercore/labels/help%20wanted
