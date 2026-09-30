# SHADW Core Rust port and university database

Goal: deliver a usable native Rust secure append-only log library and a working university-project database that builds, persists, audits and replicates its project history without a JavaScript runtime.

Source checkpoint: public SolutionsAsService/shadw-core main db1c818c92fd3882da02b1f190058f4f9dfc5d94, upstream Hypercore11.37.1 MIT. Preserve original JS files, credits, stores and APIs. Work branch feat/rust-core-university-db; no crates.io publication or replacement of JS stores.

GM MCP unavailable; native rg/toolchain fallback. Skill route: Polaris goal compiler -> foundation comparison -> execute -> WFGY verification. No measured embedding/drift numbers. All new code/deps/toolchains/build caches on D; no host shell/system-config edits. Independent bounded source-audit and Rust-preflight lanes closed; no recursive repair spawning.

## Atoms / dependency graph

1. Audit JS behavior + existing Rust solutions -> actual API/signature/storage contracts.
2. Install/verify local D-backed Rust tooling; write a minimal compiling foundation probe.
3. Native log implementation: persistence, writer/public-reader separation, signed proofs, audit, bounded export/import and key pinning; verification gate tamper/wrongkey/reopen tests.
4. University-project database: typed append events and deterministic indexed projection; CRUD-as-events, archive/history/query, reproducible synthetic example, CLI and accessible local demonstration.
5. Cross-runtime fixtures + bounded adversarial/persistence/concurrency tests + executable demo/browser proof.
6. Documents/API examples, feature compatibility matrix, licence/provenance, realGitidentity and repository delivery.

Parallel implementation lanes start only with agreed APIs; parent owns manifests/tooling/integration. Each lane writes disjoint files and returns test evidence.

## Foundation route / alternatives

A: rebuild cryptography/tree/storage from scratch. Rejected: duplicates a maintained native Rust implementation and broadens integrity risk.
B: use maintained Rust hypercore0.16.0 and add a native SHADW library + database. Preferred for a working native implementation with proven primitives and explicit scope. Rust upstream targets Hypercore10; JS11 non-compat manifests and RocksDB-backed storage are not compatible by assumption.
C: full JS11 protocol/storage/manifest rewrite. Requires explicit extra compatibility work, not a silent promise from a wrapper. User has been asked whether existing JS11 peer/storage interop is required; native-first is the provisional assumption while tooling/audits proceed.

## Truth objects and claim ceilings

A real Rust binary must append/reopen/query project records, reject tampered signed data, and replicate into a reader pinned to an independently supplied writer key. No JavaScript subprocess in runtime. Existing JS is only a test oracle. Proof transfer is not automatically Noise/Hyperswarm transport. Rust local format is not JS11 disk compatibility. Signature/integrity is not encryption, authorization or proof of truthful project claims. No benchmark/security-audit claims without measurements. Any missing JS11 APIs are explicitly itemized rather than labelled full parity.

## Verification gates

cargo fmt/check/clippy/test; meaningful local disk corruption/proof mutation/wrongkey/read-only/duplicate/stale/append-only/reopen/batch tests; emitted JavaScript compatibility fixtures actually generated and independently checked; malformed DB events and deterministic replay; bounded inputs and errors; genuine local demo create/update/archive/filter/history/export/reader workflows, offline/no-network operation where applicable; no private-key export/logging, upstreamcredits preserved, actual remote commit evidence if pushed.
