//! Native append-only log backed by Rust Hypercore 0.16.0.
//!
//! Every returned block is checked against the signed Merkle head. Proof bundles
//! are SHADW JSON interchange, not the Hypercore 11 wire or disk format.
#![forbid(unsafe_code)]

use fs2::FileExt;
use hypercore::{Hypercore, HypercoreBuilder, PartialKeypair, Storage, VerifyingKey};
use hypercore_schema::{DataBlock, DataUpgrade, Node, Proof, RequestBlock, RequestUpgrade};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    path::Path,
};

pub const MAX_BLOCK_BYTES: usize = 1024 * 1024;
pub const MAX_BUNDLE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_BUNDLE_BLOCKS: usize = 4096;
pub const MAX_LOG_BLOCKS: u64 = 1_000_000;
const FORMAT: &str = "shadw-proof-bundle-v1";
const STORE_FORMAT: &str = "shadw-rust-hypercore-0.16-v1";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("storage IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("Hypercore integrity/storage error: {0}")]
    Hypercore(#[from] hypercore::HypercoreError),
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Invalid(String),
}
pub type Result<T> = std::result::Result<T, Error>;
fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoreInfo {
    pub public_key: String,
    pub length: u64,
    pub byte_length: u64,
    pub contiguous_length: u64,
    pub fork: u64,
    pub writable: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditReport {
    pub public_key: String,
    pub length: u64,
    pub verified_blocks: u64,
    pub missing_blocks: u64,
    pub verified_bytes: u64,
    pub signed_head_verified: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportReport {
    pub imported_blocks: u64,
    pub previous_length: u64,
    pub length: u64,
    pub contiguous_length: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProofNode {
    pub index: u64,
    pub hash: String,
    pub length: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BlockProof {
    pub index: u64,
    pub value: String,
    pub nodes: Vec<ProofNode>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpgradeProof {
    pub start: u64,
    pub length: u64,
    pub nodes: Vec<ProofNode>,
    pub additional_nodes: Vec<ProofNode>,
    pub signature: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SignedProof {
    pub fork: u64,
    pub block: Option<BlockProof>,
    pub upgrade: UpgradeProof,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicationBundle {
    pub format: String,
    /// Descriptive only: the reader's independently pinned key is authoritative.
    pub public_key: String,
    pub length: u64,
    pub head: Option<SignedProof>,
    pub blocks: Vec<SignedProof>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoreMarker {
    format: String,
    public_key: String,
}

/// A process-exclusively locked store. Debug deliberately omits the underlying
/// Hypercore, whose internal key pair includes signing material.
pub struct Core {
    inner: Hypercore,
    key: [u8; 32],
    _lock: File,
}
impl std::fmt::Debug for Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Core").field("info", &self.info()).finish()
    }
}

impl Core {
    /// Create a new writer in a non-existing directory; never overwrite a store.
    pub async fn create(path: &Path) -> Result<Self> {
        Self::create_inner(path, None).await
    }
    /// Create a read-only replica pinned to a key received independently.
    pub async fn create_replica(path: &Path, public_key: [u8; 32]) -> Result<Self> {
        verifying_key(public_key)?;
        Self::create_inner(path, Some(public_key)).await
    }
    async fn create_inner(path: &Path, key: Option<[u8; 32]>) -> Result<Self> {
        let mut directory = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            directory.mode(0o700);
        }
        directory.create(path)?;
        let lock = lock_store(path)?;
        let storage = Storage::new_disk(&path.to_path_buf(), false).await?;
        let mut builder = HypercoreBuilder::new(storage);
        if let Some(public) = key {
            builder = builder.key_pair(PartialKeypair {
                public: verifying_key(public)?,
                secret: None,
            });
        }
        let inner = builder.build().await?;
        let key = *inner.key_pair().public.as_bytes();
        let marker = StoreMarker {
            format: STORE_FORMAT.into(),
            public_key: hex::encode(key),
        };
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path.join("shadw-store.json"))?;
        serde_json::to_writer(&mut file, &marker)?;
        file.sync_all()?;
        Ok(Self {
            inner,
            key,
            _lock: lock,
        })
    }
    /// Open only an identified SHADW Rust store. Missing/foreign stores are errors.
    pub async fn open(path: &Path) -> Result<Self> {
        if !path.is_dir() {
            return Err(invalid("store does not exist"));
        }
        let marker_path = path.join("shadw-store.json");
        if !marker_path.is_file() {
            return Err(invalid(
                "not a SHADW Rust store; JavaScript stores are not opened or migrated implicitly",
            ));
        }
        if fs::metadata(&marker_path)?.len() > 1024 {
            return Err(invalid("invalid store marker"));
        }
        let marker: StoreMarker = serde_json::from_slice(&fs::read(marker_path)?)?;
        if marker.format != STORE_FORMAT {
            return Err(invalid("unsupported store format"));
        }
        let key = decode_key(&marker.public_key)?;
        let lock = lock_store(path)?;
        for name in ["data", "tree", "bitfield", "oplog"] {
            if !path.join(name).is_file() {
                return Err(invalid(
                    "incomplete Rust store: required storage file is missing",
                ));
            }
        }
        let storage = Storage::new_disk(&path.to_path_buf(), false).await?;
        let inner = HypercoreBuilder::new(storage).open(true).build().await?;
        if inner.key_pair().public.as_bytes() != &key
            || inner.info().fork != 0
            || inner.info().length > MAX_LOG_BLOCKS
        {
            return Err(invalid(
                "store key, fork or size does not match the accepted format",
            ));
        }
        Ok(Self {
            inner,
            key,
            _lock: lock,
        })
    }
    pub fn public_key(&self) -> [u8; 32] {
        self.key
    }
    pub fn info(&self) -> CoreInfo {
        let i = self.inner.info();
        CoreInfo {
            public_key: hex::encode(self.key),
            length: i.length,
            byte_length: i.byte_length,
            contiguous_length: i.contiguous_length,
            fork: i.fork,
            writable: i.writeable,
        }
    }
    pub async fn append(&mut self, data: &[u8]) -> Result<u64> {
        self.check_append(1, data.len())?;
        let index = self.inner.info().length;
        self.inner.append(data).await?;
        Ok(index)
    }
    pub async fn append_batch(&mut self, data: &[Vec<u8>]) -> Result<Vec<u64>> {
        if data.len() > MAX_BUNDLE_BLOCKS || data.iter().any(|b| b.len() > MAX_BLOCK_BYTES) {
            return Err(invalid("batch exceeds bounded block limits"));
        }
        self.check_append(data.len(), data.iter().map(Vec::len).sum())?;
        let start = self.inner.info().length;
        self.inner.append_batch(data).await?;
        Ok((start..start + data.len() as u64).collect())
    }
    fn check_append(&self, count: usize, bytes: usize) -> Result<()> {
        if !self.inner.info().writeable {
            return Err(invalid("read-only replica cannot append"));
        }
        if bytes > MAX_BUNDLE_BYTES
            || (count == 1 && bytes > MAX_BLOCK_BYTES)
            || self.inner.info().length + count as u64 > MAX_LOG_BLOCKS
        {
            return Err(invalid("append exceeds bounded storage limits"));
        }
        Ok(())
    }
    /// Authenticate a locally available block against a fresh pinned-key verifier.
    pub async fn get(&mut self, index: u64) -> Result<Option<Vec<u8>>> {
        if index >= self.inner.info().length || !self.inner.has(index) {
            return Ok(None);
        }
        let mut proof = self.full_proof(Some(index)).await?;
        let mut verifier = memory_replica(self.key).await?;
        // Upstream 0.16.0 computes a later forest root's byte offset using the
        // old roots during a combined first upgrade+block. Accept the signed
        // head first, then authenticate membership against those roots.
        apply(&mut verifier, &self.full_proof(None).await?).await?;
        proof.upgrade = None;
        apply(&mut verifier, &proof).await?;
        Ok(proof.block.map(|b| b.value))
    }
    /// Verify the signed head and every locally available block (sparse gaps count
    /// as missing, not corruption). An empty log has no signed head yet.
    pub async fn audit(&mut self) -> Result<AuditReport> {
        let length = self.inner.info().length;
        if length > 0 {
            let proof = self.full_proof(None).await?;
            apply(&mut memory_replica(self.key).await?, &proof).await?;
        }
        let mut report = AuditReport {
            public_key: hex::encode(self.key),
            length,
            verified_blocks: 0,
            missing_blocks: 0,
            verified_bytes: 0,
            signed_head_verified: length > 0,
        };
        for index in 0..length {
            if let Some(bytes) = self.get(index).await? {
                report.verified_blocks += 1;
                report.verified_bytes += bytes.len() as u64;
            } else {
                report.missing_blocks += 1;
            }
        }
        Ok(report)
    }
    async fn full_proof(&mut self, index: Option<u64>) -> Result<Proof> {
        let length = self.inner.info().length;
        if length == 0 {
            return Err(invalid("empty log has no signed head"));
        }
        self.inner
            .create_proof(
                index.map(|index| RequestBlock { index, nodes: 0 }),
                None,
                None,
                Some(RequestUpgrade { start: 0, length }),
            )
            .await?
            .ok_or_else(|| invalid("requested proof block is unavailable"))
    }
    pub async fn export_bundle(&mut self, indices: &[u64]) -> Result<ReplicationBundle> {
        if indices.len() > MAX_BUNDLE_BLOCKS {
            return Err(invalid("too many requested blocks"));
        }
        let mut seen = HashSet::new();
        let length = self.inner.info().length;
        for &i in indices {
            if i >= length || !seen.insert(i) || !self.inner.has(i) {
                return Err(invalid(
                    "export indices must be unique, in range and locally available",
                ));
            }
        }
        let head = if length > 0 {
            Some(encode_proof(self.full_proof(None).await?)?)
        } else {
            None
        };
        let mut blocks = Vec::with_capacity(indices.len());
        let mut encoded_bytes = serde_json::to_vec(&head)?.len();
        for &i in indices {
            let proof = encode_proof(self.full_proof(Some(i)).await?)?;
            encoded_bytes = encoded_bytes
                .checked_add(serde_json::to_vec(&proof)?.len())
                .ok_or_else(|| invalid("export size overflow"))?;
            if encoded_bytes > MAX_BUNDLE_BYTES {
                return Err(invalid("export exceeds 16 MiB; request fewer blocks"));
            }
            blocks.push(proof);
        }
        let bundle = ReplicationBundle {
            format: FORMAT.into(),
            public_key: hex::encode(self.key),
            length,
            head,
            blocks,
        };
        validate_bundle(&bundle, self.key)?;
        // A corrupted disk block must never be exported as an accepted proof.
        let _ = stage_bundle(&bundle, self.key).await?;
        Ok(bundle)
    }
    /// Validate every proof and current-prefix consistency in memory before any
    /// accepted disk mutation. OS/disk failures during commit are not a transaction.
    pub async fn import_bundle(&mut self, bundle: &ReplicationBundle) -> Result<ImportReport> {
        validate_bundle(bundle, self.key)?;
        if self.inner.info().writeable {
            return Err(invalid("import requires a read-only replica"));
        }
        let previous = self.inner.info().length;
        if bundle.length < previous {
            return Err(invalid(
                "stale signed head would roll back accepted history",
            ));
        }
        let mut staged = stage_bundle(bundle, self.key).await?;
        if previous > 0 {
            let old = self.full_proof(None).await?;
            let old_nodes = &old
                .upgrade
                .as_ref()
                .ok_or_else(|| invalid("missing local signed head"))?
                .nodes;
            let prefix = staged
                .create_proof(
                    None,
                    None,
                    None,
                    Some(RequestUpgrade {
                        start: 0,
                        length: previous,
                    }),
                )
                .await
                .map_err(|_| {
                    invalid("bundle lacks prefix consistency nodes; export the complete history")
                })?
                .ok_or_else(|| invalid("bundle lacks prefix consistency nodes"))?;
            let prefix_nodes = &prefix
                .upgrade
                .as_ref()
                .ok_or_else(|| invalid("missing prefix proof"))?
                .nodes;
            if old_nodes != prefix_nodes {
                return Err(invalid(
                    "conflicting signed history: accepted prefix differs",
                ));
            }
        }
        let mut imports = Vec::new();
        for proof in &bundle.blocks {
            let block = proof
                .block
                .as_ref()
                .ok_or_else(|| invalid("missing block"))?;
            if self.inner.has(block.index) {
                let old = self
                    .get(block.index)
                    .await?
                    .ok_or_else(|| invalid("accepted block unavailable"))?;
                if old
                    != hex::decode(&block.value).map_err(|_| invalid("invalid block encoding"))?
                {
                    return Err(invalid("conflicting duplicate block"));
                }
            } else {
                imports.push(block.index);
            }
        }
        if imports.is_empty() && previous == bundle.length && bundle.length > 0 {
            return Err(invalid("duplicate bundle: no new blocks or signed head"));
        }
        let upgrade = if bundle.length > previous {
            Some(
                staged
                    .create_proof(
                        None,
                        None,
                        None,
                        Some(RequestUpgrade {
                            start: previous,
                            length: bundle.length - previous,
                        }),
                    )
                    .await?
                    .ok_or_else(|| invalid("missing consistency upgrade"))?,
            )
        } else {
            None
        };
        let mut block_proofs = Vec::with_capacity(imports.len());
        for index in &imports {
            let source = bundle
                .blocks
                .iter()
                .find(|p| p.block.as_ref().is_some_and(|b| b.index == *index))
                .ok_or_else(|| invalid("missing staged block"))?;
            let mut proof = decode_proof(source)?;
            proof.upgrade = None;
            block_proofs.push(proof);
        }
        // Rehearse exactly the target mutations against the accepted signed head.
        let mut rehearsal = memory_replica(self.key).await?;
        if previous > 0 {
            apply(&mut rehearsal, &self.full_proof(None).await?).await?;
        }
        if let Some(proof) = &upgrade {
            apply(&mut rehearsal, proof).await?;
        }
        for proof in &block_proofs {
            apply(&mut rehearsal, proof).await?;
        }
        if let Some(proof) = &upgrade {
            apply(&mut self.inner, proof).await?;
        }
        for proof in &block_proofs {
            apply(&mut self.inner, proof).await?;
        }
        Ok(ImportReport {
            imported_blocks: imports.len() as u64,
            previous_length: previous,
            length: self.inner.info().length,
            contiguous_length: self.inner.info().contiguous_length,
        })
    }
}

fn lock_store(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path.join("shadw.lock"))?;
    file.try_lock_exclusive()
        .map_err(|_| invalid("store is already open; concurrent access is not allowed"))?;
    Ok(file)
}
fn decode_key(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 {
        return Err(invalid("public key must be 64 hexadecimal characters"));
    }
    let bytes = hex::decode(value).map_err(|_| invalid("public key must be hexadecimal"))?;
    let key = bytes
        .try_into()
        .map_err(|_| invalid("public key must be 32 bytes"))?;
    verifying_key(key)?;
    Ok(key)
}
fn verifying_key(key: [u8; 32]) -> Result<VerifyingKey> {
    let public =
        VerifyingKey::from_bytes(&key).map_err(|_| invalid("invalid Ed25519 public key"))?;
    if public.is_weak() {
        return Err(invalid("weak Ed25519 public keys are not accepted"));
    }
    Ok(public)
}
async fn memory_replica(key: [u8; 32]) -> Result<Hypercore> {
    Ok(HypercoreBuilder::new(Storage::new_memory().await?)
        .key_pair(PartialKeypair {
            public: verifying_key(key)?,
            secret: None,
        })
        .build()
        .await?)
}
async fn apply(core: &mut Hypercore, proof: &Proof) -> Result<()> {
    if !core.verify_and_apply_proof(proof).await? {
        return Err(invalid("proof cannot be applied to accepted head"));
    }
    Ok(())
}
fn encode_nodes(nodes: Vec<Node>) -> Vec<ProofNode> {
    nodes
        .into_iter()
        .map(|n| ProofNode {
            index: n.index,
            hash: hex::encode(n.hash),
            length: n.length,
        })
        .collect()
}
fn encode_proof(proof: Proof) -> Result<SignedProof> {
    let u = proof
        .upgrade
        .ok_or_else(|| invalid("signed upgrade is required"))?;
    Ok(SignedProof {
        fork: proof.fork,
        block: proof.block.map(|b| BlockProof {
            index: b.index,
            value: hex::encode(b.value),
            nodes: encode_nodes(b.nodes),
        }),
        upgrade: UpgradeProof {
            start: u.start,
            length: u.length,
            nodes: encode_nodes(u.nodes),
            additional_nodes: encode_nodes(u.additional_nodes),
            signature: hex::encode(u.signature),
        },
    })
}
fn decode_nodes(nodes: &[ProofNode]) -> Result<Vec<Node>> {
    nodes
        .iter()
        .map(|n| {
            Ok(Node::new(
                n.index,
                hex::decode(&n.hash).map_err(|_| invalid("invalid node hash"))?,
                n.length,
            ))
        })
        .collect()
}
fn decode_proof(p: &SignedProof) -> Result<Proof> {
    Ok(Proof {
        fork: p.fork,
        block: p
            .block
            .as_ref()
            .map(|b| {
                Ok::<_, Error>(DataBlock {
                    index: b.index,
                    value: hex::decode(&b.value).map_err(|_| invalid("invalid block encoding"))?,
                    nodes: decode_nodes(&b.nodes)?,
                })
            })
            .transpose()?,
        hash: None,
        seek: None,
        upgrade: Some(DataUpgrade {
            start: p.upgrade.start,
            length: p.upgrade.length,
            nodes: decode_nodes(&p.upgrade.nodes)?,
            additional_nodes: decode_nodes(&p.upgrade.additional_nodes)?,
            signature: hex::decode(&p.upgrade.signature)
                .map_err(|_| invalid("invalid signature encoding"))?,
        }),
    })
}
fn validate_bundle(bundle: &ReplicationBundle, key: [u8; 32]) -> Result<()> {
    if bundle.format != FORMAT || decode_key(&bundle.public_key)? != key {
        return Err(invalid(
            "bundle format or public key does not match pinned reader",
        ));
    }
    if bundle.length > MAX_LOG_BLOCKS || bundle.blocks.len() > MAX_BUNDLE_BLOCKS {
        return Err(invalid("bundle exceeds bounded size"));
    }
    if bundle.length == 0 {
        if bundle.head.is_some() || !bundle.blocks.is_empty() {
            return Err(invalid("empty bundle cannot contain proofs"));
        }
        return Ok(());
    }
    let head = bundle
        .head
        .as_ref()
        .ok_or_else(|| invalid("nonempty bundle requires signed head"))?;
    if head.block.is_some() {
        return Err(invalid("head proof must not contain a block"));
    }
    let mut seen = HashSet::new();
    let mut total = 0usize;
    for (is_head, p) in
        std::iter::once((true, head)).chain(bundle.blocks.iter().map(|p| (false, p)))
    {
        if p.fork != 0
            || p.upgrade.start != 0
            || p.upgrade.length != bundle.length
            || !p.upgrade.additional_nodes.is_empty()
            || p.upgrade.signature.len() != 128
            || p.upgrade.signature != head.upgrade.signature
            || hex::decode(&p.upgrade.signature).is_err()
        {
            return Err(invalid("invalid or inconsistent signed head"));
        }
        if !is_head && p.block.is_none() {
            return Err(invalid("block proof is missing block"));
        }
        if let Some(b) = &p.block {
            if b.index >= bundle.length
                || !seen.insert(b.index)
                || b.value.len() > MAX_BLOCK_BYTES * 2
                || !b.value.len().is_multiple_of(2)
            {
                return Err(invalid("duplicate, out-of-range or oversized block"));
            }
            total = total
                .checked_add(b.value.len())
                .ok_or_else(|| invalid("bundle size overflow"))?;
        }
        for nodes in [&p.upgrade.nodes, &p.upgrade.additional_nodes]
            .into_iter()
            .chain(p.block.as_ref().map(|b| &b.nodes))
        {
            if nodes.len() > 64 {
                return Err(invalid("too many Merkle proof nodes"));
            }
            let mut unique = HashSet::new();
            for n in nodes {
                if n.index >= bundle.length * 2
                    || n.hash.len() != 64
                    || hex::decode(&n.hash).is_err()
                    || n.length > MAX_LOG_BLOCKS * MAX_BLOCK_BYTES as u64
                    || !unique.insert(n.index)
                {
                    return Err(invalid("invalid Merkle proof node"));
                }
            }
            total = total
                .checked_add(nodes.len() * 96)
                .ok_or_else(|| invalid("bundle size overflow"))?;
        }
    }
    if total > MAX_BUNDLE_BYTES {
        return Err(invalid("bundle exceeds 16 MiB encoded proof limit"));
    }
    // Structural bounds above cap allocation first; include JSON field names
    // and punctuation in the actual interchange budget as well.
    if serde_json::to_vec(bundle)?.len() > MAX_BUNDLE_BYTES {
        return Err(invalid("bundle exceeds 16 MiB serialized limit"));
    }
    Ok(())
}
async fn stage_bundle(bundle: &ReplicationBundle, key: [u8; 32]) -> Result<Hypercore> {
    let mut stage = memory_replica(key).await?;
    if let Some(head) = &bundle.head {
        apply(&mut stage, &decode_proof(head)?).await?;
        let canonical = stage
            .create_proof(
                None,
                None,
                None,
                Some(RequestUpgrade {
                    start: 0,
                    length: bundle.length,
                }),
            )
            .await?
            .ok_or_else(|| invalid("missing verified head"))?;
        if encode_proof(canonical)? != *head {
            return Err(invalid("noncanonical or redundant head proof nodes"));
        }
    }
    for p in &bundle.blocks {
        let mut proof = decode_proof(p)?;
        // The separately verified signed head is authoritative. Authenticate
        // each block path against it rather than combining initial upgrade
        // and block writes (an upstream multi-root offset bug).
        proof.upgrade = None;
        apply(&mut stage, &proof).await?;
        // Reject unused/tampered redundant nodes too: interchange proofs use
        // the canonical full-head representation produced by both runtimes.
        let index = p
            .block
            .as_ref()
            .ok_or_else(|| invalid("missing block"))?
            .index;
        let canonical = stage
            .create_proof(
                Some(RequestBlock { index, nodes: 0 }),
                None,
                None,
                Some(RequestUpgrade {
                    start: 0,
                    length: bundle.length,
                }),
            )
            .await?
            .ok_or_else(|| invalid("missing verified block proof"))?;
        if encode_proof(canonical)? != *p {
            return Err(invalid("noncanonical or redundant block proof nodes"));
        }
    }
    if stage.info().length != bundle.length {
        return Err(invalid("verified head length differs from bundle"));
    }
    Ok(stage)
}
