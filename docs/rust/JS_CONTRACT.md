# JavaScript compatibility audit

Audited source: `SolutionsAsService/shadw-core` commit
`db1c818c92fd3882da02b1f190058f4f9dfc5d94`, package `hypercore` **11.37.1**.
This document is an implementation planning contract, not a claim that a Rust
implementation already interoperates. The README's introductory “latest release
is Hypercore 10” sentence is stale; prefer the actual package and source.

## Minimum useful Rust product

A durable single-writer authenticated append log, public-key read-only replicas,
individual block reads, batch appends, signed tree heads, sparse block proofs,
verification before persistence, reopen/recovery, and an educational database
materialized from versioned event blocks. A database projection is separate from
the log; it must be rebuildable from verified events. Do not equate an educational
projection with distributed SQL, concurrent writers, access control, or encryption.

Use a maintained Rust Hypercore implementation if its tested behavior covers
this contract. Keeping the original JavaScript tree and adding an explicitly
scoped Rust workspace preserves working code, historical tests, and credits.

## Cryptographic bytes

Read `lib/caps.js`, `lib/verifier.js`, `lib/merkle-tree.js`, and
`hypercore-crypto@3.2.1/index.js` (the declared minimum dependency).
All hashes below are BLAKE2b with **32-byte output**, not a truncated BLAKE2b-512.
Integers in these hash/signature preimages are fixed-width unsigned 64-bit little
endian; wire compact-encoding integers are a different concern.

| Value | Hash/signature preimage |
| --- | --- |
| Leaf | `0x00 || u64LE(block byte length) || block bytes` |
| Parent | `0x01 || u64LE(left.size + right.size) || left.hash || right.hash` |
| Tree digest | `0x02 || (root.hash || u64LE(root.index) || u64LE(root.size))*` |
| Empty tree digest | hash of the single byte `0x02` |
| Discovery key | keyed BLAKE2b-256(message=`hypercore`, key=core key) |

Flat-tree leaves have indices `2 * blockIndex`; parent inputs are ordered by
flat-tree index and tree roots stay in ascending forest order. Tree digest is
not simply the top binary tree node. Test non-power-of-two counts.

Namespaces: first hash UTF-8 `hypercore` to 32 bytes, append one counter byte,
then hash the resulting 33 bytes. Counters: TREE=0, replication initiator=1,
responder=2, MANIFEST=3, DEFAULT_NAMESPACE=4, DEFAULT_ENCRYPTION=5.
Ed25519 public keys are 32 bytes and detached signatures are 64 bytes; JS
`sodium` secret keys are 64 bytes, while Rust libraries commonly accept a 32-byte
seed. Define the representation explicitly, never copy an undocumented key file.

## Three signature modes must not be confused

1. **Explicit compat mode**, whose core key equals its signer's public key:
   `TREE || treeHash || u64LE(length) || u64LE(fork)` (80 bytes), raw Ed25519
   signature. This is the most practical bounded Rust/JS11 compatibility target.
2. **Non-compat manifest version 0** uses
   `TREE || signer.namespace || treeHash || length || fork` (112 bytes).
3. **Non-compat manifest version 1 and later** uses
   `TREE || manifestHash || treeHash || length || fork` (112 bytes). The core key
   is `hash(MANIFEST || compactEncodedManifest)`, not the signing public key.
   Even a single signer uses a compact-encoded multisignature envelope in
   `state.signature`, not just the raw 64-byte Ed25519 signature.

Constructor nuance: JS11 `lib/core.js` defaults to compat when no manifest is
provided and `compat` is not explicitly false. Manifest normalization defaults
to version 1, but this does not mean an ordinary `new Hypercore(path)` constructor
uses non-compat signing. Fixtures select `compat: true` explicitly to make the
contract unambiguous.

An ultra-legacy no-header compat mode has a 48-byte preimage. Exclude it unless
specifically implemented and tested. Non-compat v1 manifests, quorum multisigning,
patches, prologues, v2 linked keys/userData, block encryption, Noise/protomux live
transport and automatic fork reorganization are separate parity milestones.
Unsupported modes must fail explicitly, not silently reinterpret the key.

**Generator trap:** public `core.signable()` always returns the normal
112-byte `caps.treeSignable(core.key, ...)` form, even for a compat core.
For a compat signed-head vector, call `caps.treeSignableCompat(...)` and compare
against the actual `core.state.signature` using `hypercore-crypto.verify`.

## Proof contract

`await core.proof(options)` already settles the internal `TreeProof` and loads
block bytes. Its result is:

```
{
  fork,
  block: null | { index, value, nodes },
  hash: null | { index, nodes },
  seek: null | { bytes, nodes },
  upgrade: null | { start, length, nodes, additionalNodes, signature },
  manifest: null | manifest
}
```

Each tree node is `{index, size, hash}`. `block.index` is a block index;
`hash.index` is a flat-tree index. An upgrade signs the resulting head, and its
`length` is the number of added blocks after `start`, not necessarily total log
length. `additionalNodes` can extend the signed head beyond a requested subtree.
A proof from `start:0` can establish a head for an empty public replica without
possessing preceding block bytes. A block-only proof relies on already trusted
head state. Verify all node positions, sizes, ordering, root commitments and
signature before storing data. Bound decoded allocations and arithmetic.

Suggested first interop request:
`core.proof({ block: {index: 2, nodes: 0}, upgrade: {start: 0, length: core.length} })`.
Verify it on an empty Rust replica under a pinned public key; confirm only the
requested block is present. Test a head-only upgrade, incremental upgrade from a
nonzero trusted length, and later block-only download separately.

Truncation is an authorized history fork, not ordinary append-only growth:
`truncate(length)` defaults to `fork + 1`, and fork must increase monotonically.
A fork-zero-only initial implementation should reject changed forks clearly.
Claiming truncate/reorganization support requires tests for a truncate followed
by different re-appends, stale proofs, wrong fork, and persisted state recovery.
A valid signature does not alone prove consistency with the previously pinned
head or establish that a received signed head is the newest available head.

## Deterministic JS11 fixture generator scope

Run the checked-out package with pinned/resolved dependencies and record their
versions. Use a fixed, loudly test-only 32-byte seed and byte blocks including
empty, ASCII, UTF-8, and binary zero/255. Never use real database keys.

Export JSON containing hex bytes and integers, plus package version/commit:

- seed/public key, explicit compat core key, discovery key, namespace constants;
- per-leaf hashes and sizes; a parent with reversed arguments too;
- complete forest roots and tree digest for lengths 0, 1, 2, 3, 5;
- 80-byte compat preimage and actual verified signature for each nonempty head;
- full upgrade plus selected-block proof; head-only, incremental and block-only proofs;
- one default v1 manifest encoding/key/preimage/envelope as a documented
  unsupported-mode or future milestone vector, not as evidence of compat support;
- changed-fork example if fork support is part of the chosen Rust implementation.

For bidirectional evidence, have Rust create equivalent blocks and export a
proof which the JS public replica accepts via `applyProof` or
`verifyFullyRemote`. Pure JSON round trips are not replication evidence.
Negative Rust cases should alter a value, node hash/size/index, key, signature,
fork, and head length, and prove the replica state was unchanged on rejection.
Keep cryptographic identity comparisons separate from custom transport encoding.

## Storage, migration and credits

JS11 uses `hypercore-storage@^3.2.0`; README explicitly says random-access-storage
is no longer supported. A Rust crate's older file layout does not imply that it
can open JS11 directories. Keep directories distinct. Do not mutate or open
original JS stores for writing from a new Rust implementation.

A safe migration reads verified JS blocks and auth/head information through JS
APIs, then either applies compatible signed proofs to a Rust replica or creates
a newly identified Rust log with explicit provenance. Re-appending decoded JSON
can change encoded bytes, hashes and identity. A newly signed log is not a
byte-for-byte migration. Do not copy private signing material into an example or
commit generated writable stores.

Retain the repository's MIT license and Mathias Buus copyright (2016), upstream
Hypercore identity, and package contributors Mathias Buus / Andrew Osheroff.
Attribute any Rust Hypercore dependencies and distinguish newly authored adapter
and database code from upstream implementation. This audit executed source
inspection only: no interop fixture or storage-parity test is claimed here.

## Subsequent JavaScript fixture evidence

`scripts/rust/generate-js-fixtures.cjs` has now executed the actual checked-out
JS11 native runtime and produced `fixtures/rust/js11-compat.json`. It verifies
80-byte compat signatures and applies the signed head and all five selected
block proofs to a separately opened public-key JavaScript replica. The fixture
records resolved dependency versions, test-only seed, roots, hashes, signatures,
and a separate non-compat manifest reference. This generation/self-check is
JavaScript evidence; Rust acceptance and reverse Rust-to-JavaScript verification
must be established by their respective tests, not inferred from this file.
