# SHADW Core Rust migration

- Preserve JavaScript Hypercore source, MIT copyright and contributor attribution. Rust code is additive; no silent deletion or replacement of existing stores.
- Goal: native Rust secure append-only log and a working university-project database example, not a Node.js subprocess wrapper.
- Reuse maintained Rust Hypercore and established cryptographic primitives. Never claim full JS11 manifest/wire/disk compatibility without actual cross-runtime evidence; document bounded compatibility and gaps.
- Use GM anchored workflow; GM MCP unavailable, native rg fallback. Bounded independent lanes with disjoint write scopes are authorized; no recursive spawning.
- Keep project, tools, dependencies, compiler caches and targets on D:. Local build tooling may be supplied by environment; portable repo scripts must not require this machine's paths.
- Do not log/export signing secrets. Replica trust requires an independently supplied public writer key. Proof verification is not encryption or user authorization.
- Log mutation is append-only in this public API; project edits/archive are new events. Validate all untrusted proofs and event payloads before altering accepted state; don't trust an imported signing key as authority.
- Completion gates: cargo fmt/check/clippy/test, adversarial signed proof/persistence/reopen/read-only checks, actual compatibility fixtures, runnable database workflow, user-facing example verification, explicit limitation matrix and recoverable Git delivery.
