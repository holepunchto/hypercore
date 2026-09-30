# Credits and dependency provenance

- Original JavaScript Hypercore: Mathias Buus, Andrew Osheroff and upstream
  contributors. MIT; original LICENSE and source preserved. This repository's
  package was 11.37.1 at the audited baseline.
- Native cryptographic tree and storage engine:
  [datrs/hypercore](https://github.com/datrs/hypercore), pinned 0.16.0,
  MIT OR Apache-2.0. Also `hypercore_schema` 0.2.0. Their separate licenses apply.
- Rust application ecosystem: Tokio, Axum, Serde, Clap, Chrono and other resolved
  dependencies appear with exact versions/checksums in Cargo.lock. The vendored Hypercore engine retains its original source and licenses with
  bounded persistence repairs documented in `vendor/hypercore/SHADW_PATCHES.md`.
  It is not relabeled as SHADW original work.
- New SHADW code supplies the bounded append-only wrapper, verified proof-bundle
  interface, university-domain event/projection layer, local HTTP/CLI demo and UI.

The JS fixture oracle records its actual dependency versions; it is test tooling,
not a dependency of the native Rust runtime. Synthetic seed/project records are
for testing and teaching, not real university participants or production keys.
