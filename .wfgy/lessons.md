# Implementation checkpoints

- Preserve a reproducible JS oracle and distinguish explicit compat signatures,
  manifest-based signatures, disk format and network transport. Shared Merkle
  primitives alone are not evidence of full interoperability. The actual JS11
  constructor can select compat when no manifest is supplied; request it
  explicitly in fixtures to remove ambiguity.
- Inspect transitive source before claiming a minimum Rust version. Hypercore
  0.16.0 uses let chains (Rust 1.88+) despite lacking useful MSRV metadata. This
  workspace requires at least 1.88 and records the actual tested version separately.
- A minimal Linux host may lack both Rust and a C linker. An isolated user-owned
  toolchain/sysroot can be used without editing host configuration. Keep temporary
  files and compiler caches on the intended volume; prove the linker with a real
  compiled executable before attributing build errors to the application.
- Upstream proof verification must be exercised for non-power-of-two forests,
  empty blocks and sparse replicas, not just one-block happy paths. A dependency
  panic is a failed verification gate, not an excuse to remove a failing test.
- Whole-batch cryptographic validation is not crash-atomic storage. Describe
  prevalidation and IO failure semantics separately; do not promise transactions
  that the underlying engine does not provide.
