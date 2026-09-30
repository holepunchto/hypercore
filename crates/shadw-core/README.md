# shadw-core (native Rust)

A native Rust SHADW append-only API using maintained `hypercore = 0.16.0`,
BLAKE2b Merkle proofs and Ed25519 signatures. No Node.js runtime is used.
This is an additive native-first implementation, **not full JavaScript
Hypercore 11.37.1 API, disk, manifest or wire compatibility**.

```rust
use shadw_core::Core;
use std::path::Path;
#[tokio::main]
async fn main() -> shadw_core::Result<()> {
let mut writer = Core::create(Path::new("new-writer-directory")).await?;
let index = writer.append(b"university project event").await?;
let trusted_key = writer.public_key(); // Convey independently to the reader.
let bundle = writer.export_bundle(&[index]).await?;
let mut reader = Core::create_replica(Path::new("new-reader-directory"), trusted_key).await?;
reader.import_bundle(&bundle).await?;
assert_eq!(reader.get(index).await?, Some(b"university project event".to_vec()));
assert_eq!(reader.audit().await?.verified_blocks, 1);
Ok(())
}
```

## Guarantees and boundaries

- `create` only creates a previously absent directory. `open` never creates a
  missing store and requires the Rust format marker and all four store files.
  Existing JavaScript/RocksDB stores are not opened, overwritten or migrated.
- An exclusive file lock covers the store lifetime, including readers. This
  deliberately rejects concurrent processes instead of risking stale writers.
- Public mutation is append-only: there is no truncate, clear, fork or delete.
  Read-only replicas cannot append and writers cannot import replica bundles.
- `get` verifies the returned bytes with a fresh public-key-only Hypercore
  verifier. `audit` authenticates the signed head and all available blocks;
  sparse gaps are counted separately. Empty logs have no signed head.
- Export contains no signing secret. Every bundle is checked against the
  independently pinned reader key; its `public_key` is descriptive, not trust.
- Import bounds and validates all proofs, checks accepted-prefix consistency,
  and rehearses the entire batch in memory before changing accepted disk state.
  A cryptographically invalid batch therefore makes no accepted mutation.
  This is **not an OS-failure/crash-atomic transaction**: an IO failure during
  the later disk commit can leave a valid partial import. Reopen/audit and
  retry the remaining blocks after repairing the IO problem.
- Valid signed same-head duplicate-only imports and stale-head rollback are
  rejected explicitly. Same-head sparse fills are supported. Extending a
  sparse log requires sufficient prefix-consistency nodes; exporting the full
  history supplies them. An insufficient proof returns an error without mutation.
- Bounds: 1 MiB per block, 4096 blocks per batch/bundle, 16 MiB encoded proof
  budget, 64 nodes per proof list, and one million blocks per log. For larger
  histories use bounded transfers; full-history extension beyond a single
  bundle requires a future explicit checkpoint/consistency API.
- Store signing secrets remain in Hypercore's local oplog. Protect the directory
  with OS permissions. Signatures provide integrity/authenticity, **not** data
  encryption, access control, truthful content or compromised-writer protection.
- A locally trusted marker/head can itself be replaced by an attacker with full
  filesystem access. External pinned checkpoints are needed for rollback
  detection across whole-store replacement; this API does not claim that feature.

`ReplicationBundle` is a versioned SHADW JSON proof format. It is not the
Hypercore network protocol and does not implement Noise, Hyperswarm, encryption,
JS11 manifests, multi-writer conflict resolution or automatic networking.

## Upstream credits

Cryptographic tree/storage work is supplied by [datrs/hypercore](https://github.com/datrs/hypercore)
and [hypercore_schema](https://crates.io/crates/hypercore_schema), MIT OR Apache-2.0,
building on the original MIT-licensed Hypercore work. Original repository
JavaScript source and attribution remain untouched.
