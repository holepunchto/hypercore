# Bounded SHADW correctness patches

Upstream: `hypercore` **0.16.0**, https://github.com/datrs/hypercore,
https://crates.io/crates/hypercore/0.16.0. Copied from the published crate verified
by Cargo's crates.io checksum; original version, author metadata, MIT and
Apache-2.0 licenses remain included. This is a local patched dependency, not an
upstream release or a claim that upstream reviewed these changes.

Original published crate SHA-256:
`c3a17560230f1d9ad12ceabc03644c29d0380a5f27bfb0000d55a80202c69041`.

Only production source changes:

1. `src/oplog/entry.rs`: decode the tree-upgrade flag with mask **4** and the
   bitfield flag with mask **8**, matching the existing encoder. Upstream tested
   mask 2 for all three fields. A bitfield-only replicated block was available
   immediately but disappeared from the reader's available-block bitmap after
   reopen; `sparse_replication_fill_duplicate_and_extension` reproduces this.
2. `src/oplog/mod.rs`: reconstruct committed entry count and byte offset while
   opening an existing oplog. Subsequent appends must start after recovered
   entries, not overwrite from offset 8192.
3. The same recovery loop now pops the parallel partial-flag and record-size
   vectors when discarding trailing incomplete entries. Previously the unchanged
   final `true` flag caused an infinite loop. Recovery schedules truncation of
   discarded partial/incomplete trailing bytes, so a shorter subsequent append
   cannot leave an orphaned old suffix.

Added `shadw_regression_tests` test modules check all supported flag combinations,
append offsets after replay, and trailing-partial termination. Existing original
source formatting outside these bounded edits is preserved.

The public SHADW adapter separately verifies signed heads before block proofs.
This avoids a further upstream multi-root combined-upgrade byte-offset panic
without modifying the cryptographic verifier. It checks canonical proof fields
and bounds before accepted storage mutation.

These fixes do not make Hypercore 0.16 a full JS11 port or provide general
crash-atomic transactions. The project compatibility and limitation documents
remain authoritative.
