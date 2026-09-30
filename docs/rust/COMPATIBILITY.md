# Compatibility and security contract

This workspace adds a Rust-native product; it does not replace the JavaScript
implementation or its stored databases. The audited JS package is 11.37.1 at
`db1c818c92fd3882da02b1f190058f4f9dfc5d94`. The Rust engine is pinned to
`hypercore 0.16.0` (the datrs Hypercore10-oriented implementation), with
a small documented local persistence patch. See
[patch provenance](../../vendor/hypercore/SHADW_PATCHES.md); this is not an
unmodified upstream release.

## Supported surface

| Capability | Rust implementation |
| --- | --- |
| Create/open durable log, append/batch/get | Native async Rust; exclusively locked store |
| Verified read and full available-block audit | Fresh public-key verifier authenticates actual bytes |
| Public-key read-only replica | Supported; cannot append or import into a writer |
| Sparse proof transfer and filling missing blocks | Supported through versioned SHADW JSON bundles |
| Extension of an accepted history | Prefix consistency checked before accepted changes |
| JavaScript explicit compat-mode Merkle proofs | Bounded cross-runtime fixture target; see verification evidence |
| JS11 on-disk `hypercore-storage`/RocksDB | **Not supported**; distinct Rust directory marker prevents silent opening |
| JS11 non-compat manifest/multisignature features | **Not supported**; raw Ed25519 compat signatures only |
| Noise, Hyperswarm, protomux, JS streams/session API | **Not implemented**; no peer networking is implied by proof transfer |
| Encryption, user accounts, access policies | **Not implemented** |
| Forks, truncate, clear, rewrite, multi-writer merge | **Not exposed**; fork-zero append-only contract |
| Latest-head discovery / whole-store rollback protection | Needs externally pinned checkpoints; not implemented |
| Project database | Verified event-sourced projection, not SQL or a distributed transaction engine |

JS11 constructors can select compat mode by default when no manifest is supplied;
we nevertheless request `compat: true` explicitly in interoperability fixtures.
This is not a claim that every JS11 core is incompatible or that every JS11 core
is supported. Non-compat manifest v1 signing and JS11 disk layout are separate
features. The [source contract](JS_CONTRACT.md) details hash/signature bytes.

## Storage and trust

Create requires an absent directory. Open requires an existing Rust format marker
and storage files; it does not create a missing database. Both writer and reader
stores hold an exclusive OS file lock while open. The writer's local oplog contains
its signing secret: restrict the directory to the intended OS user and use
appropriate secure backups. Do not commit generated stores. Replica bundles
contain public data and signatures, not the private key.

A replica must receive its trusted writer key independently of the data being
imported. A supplied bundle key only identifies a claimed writer and is checked
against that pin. Application event schemas and transitions are checked in
addition to signature verification; a valid signature is not sufficient for a
valid database event.

Imports stage all bounded proofs and rehearse mutations in memory before changing
accepted disk state. Cryptographically invalid batches are rejected without
accepted mutation. OS failures during the subsequent commit are **not atomic**:
a valid partial import may remain. Repair the underlying IO problem, reopen/audit
and retry missing blocks. The same caveat applies to catastrophic storage failures
during append; this is not a transactional storage replacement or a power-loss
certification.

A filesystem attacker who can replace an entire store/marker or steal a signing
secret exceeds these local integrity guarantees. Pin checkpoints externally when
rollback detection matters. Keys do not establish a real-world identity by
themselves. Nothing here conceals data, authenticates browser users, authorizes
project edits, validates research claims or provides university compliance.

## Bounded educational database

Core limits: 1 MiB blocks, 4096 blocks per batch/bundle, 16 MiB encoded proof budget,
one million log blocks and 64 nodes per proof list. The database uses stricter
64 KiB event blocks. Its full-history export must fit a single bounded bundle;
large databases need a future paginated checkpoint/consistency transfer API.
Audits and startup replay read history; no constant-time or high-throughput claim
is made. The in-memory projection is derived, not an independent source of truth.

The local app listens only on IPv4 loopback, checks Host/Origin and sends no CORS
allowance. This reduces accidental browser cross-origin access but is not user
authentication. Use fictional records and do not expose it as a public service.
