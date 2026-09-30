//! Hypercore's main abstraction. Exposes an append-only, secure log structure.
use ed25519_dalek::Signature;
use futures::future::Either;
use std::convert::TryFrom;
use std::fmt::Debug;
use tracing::instrument;

#[cfg(feature = "cache")]
use crate::common::cache::CacheOptions;
use crate::{
    bitfield::Bitfield,
    common::{BitfieldUpdate, HypercoreError, NodeByteRange, StoreInfo, ValuelessProof},
    crypto::{PartialKeypair, generate_signing_key},
    data::BlockStore,
    oplog::{Header, MAX_OPLOG_ENTRIES_BYTE_SIZE, Oplog},
    storage::Storage,
    tree::{MerkleTree, MerkleTreeChangeset},
};

use hypercore_schema::{Proof, RequestBlock, RequestSeek, RequestUpgrade};

#[derive(Debug)]
pub(crate) struct HypercoreOptions {
    pub(crate) key_pair: Option<PartialKeypair>,
    pub(crate) open: bool,
    #[cfg(feature = "cache")]
    pub(crate) node_cache_options: Option<CacheOptions>,
}

impl HypercoreOptions {
    pub(crate) fn new() -> Self {
        Self {
            key_pair: None,
            open: false,
            #[cfg(feature = "cache")]
            node_cache_options: None,
        }
    }
}

/// Hypercore is an append-only log structure.
#[derive(Debug)]
pub struct Hypercore {
    pub(crate) key_pair: PartialKeypair,
    pub(crate) storage: Storage,
    pub(crate) oplog: Oplog,
    pub(crate) tree: MerkleTree,
    pub(crate) block_store: BlockStore,
    pub(crate) bitfield: Bitfield,
    skip_flush_count: u8, // autoFlush in Javascript
    header: Header,
    #[cfg(feature = "replication")]
    events: crate::replication::events::Events,
}

/// Response from append, matches that of the Javascript result
#[derive(Debug, PartialEq)]
pub struct AppendOutcome {
    /// Length of the hypercore after append
    pub length: u64,
    /// Byte length of the hypercore after append
    pub byte_length: u64,
}

/// Info about the hypercore
#[derive(Debug, PartialEq)]
pub struct Info {
    /// Length of the hypercore
    pub length: u64,
    /// Byte length of the hypercore
    pub byte_length: u64,
    /// Continuous length of entries in the hypercore with data
    /// starting from index 0
    pub contiguous_length: u64,
    /// Fork index. 0 if hypercore not forked.
    pub fork: u64,
    /// True if hypercore is writeable, false if read-only
    pub writeable: bool,
}

impl Hypercore {
    /// Creates/opens new hypercore using given storage and options
    pub(crate) async fn new(
        mut storage: Storage,
        mut options: HypercoreOptions,
    ) -> Result<Hypercore, HypercoreError> {
        let key_pair: Option<PartialKeypair> = if options.open {
            if options.key_pair.is_some() {
                return Err(HypercoreError::BadArgument {
                    context: "Key pair can not be used when building an openable hypercore"
                        .to_string(),
                });
            }
            None
        } else {
            Some(options.key_pair.take().unwrap_or_else(|| {
                let signing_key = generate_signing_key();
                PartialKeypair {
                    public: signing_key.verifying_key(),
                    secret: Some(signing_key),
                }
            }))
        };

        // Open/create oplog
        let mut oplog_open_outcome = match Oplog::open(&key_pair, None)? {
            Either::Right(value) => value,
            Either::Left(instruction) => {
                let info = storage.read_info(instruction).await?;
                match Oplog::open(&key_pair, Some(info))? {
                    Either::Right(value) => value,
                    Either::Left(_) => {
                        return Err(HypercoreError::InvalidOperation {
                            context: "Could not open oplog".to_string(),
                        });
                    }
                }
            }
        };
        storage
            .flush_infos(&oplog_open_outcome.infos_to_flush)
            .await?;

        // Open/create tree
        let mut tree = match MerkleTree::open(
            &oplog_open_outcome.header.tree,
            None,
            #[cfg(feature = "cache")]
            &options.node_cache_options,
        )? {
            Either::Right(value) => value,
            Either::Left(instructions) => {
                let infos = storage.read_infos(&instructions).await?;
                match MerkleTree::open(
                    &oplog_open_outcome.header.tree,
                    Some(&infos),
                    #[cfg(feature = "cache")]
                    &options.node_cache_options,
                )? {
                    Either::Right(value) => value,
                    Either::Left(_) => {
                        return Err(HypercoreError::InvalidOperation {
                            context: "Could not open tree".to_string(),
                        });
                    }
                }
            }
        };

        // Create block store instance
        let block_store = BlockStore::default();

        // Open bitfield
        let mut bitfield = match Bitfield::open(None) {
            Either::Right(value) => value,
            Either::Left(instruction) => {
                let info = storage.read_info(instruction).await?;
                match Bitfield::open(Some(info)) {
                    Either::Right(value) => value,
                    Either::Left(instruction) => {
                        let info = storage.read_info(instruction).await?;
                        match Bitfield::open(Some(info)) {
                            Either::Right(value) => value,
                            Either::Left(_) => {
                                return Err(HypercoreError::InvalidOperation {
                                    context: "Could not open bitfield".to_string(),
                                });
                            }
                        }
                    }
                }
            }
        };

        // Process entries stored only to the oplog and not yet flushed into bitfield or tree
        if let Some(entries) = oplog_open_outcome.entries {
            for entry in entries.iter() {
                for node in &entry.tree_nodes {
                    tree.add_node(node.clone());
                }

                if let Some(bitfield_update) = &entry.bitfield {
                    bitfield.update(bitfield_update);
                    update_contiguous_length(
                        &mut oplog_open_outcome.header,
                        &bitfield,
                        bitfield_update,
                    );
                }
                if let Some(tree_upgrade) = &entry.tree_upgrade {
                    // TODO: Generalize Either response stack
                    let mut changeset =
                        match tree.truncate(tree_upgrade.length, tree_upgrade.fork, None)? {
                            Either::Right(value) => value,
                            Either::Left(instructions) => {
                                let infos = storage.read_infos(&instructions).await?;
                                match tree.truncate(
                                    tree_upgrade.length,
                                    tree_upgrade.fork,
                                    Some(&infos),
                                )? {
                                    Either::Right(value) => value,
                                    Either::Left(_) => {
                                        return Err(HypercoreError::InvalidOperation {
                                            context: format!(
                                                "Could not truncate tree to length {}",
                                                tree_upgrade.length
                                            ),
                                        });
                                    }
                                }
                            }
                        };
                    changeset.ancestors = tree_upgrade.ancestors;
                    changeset.hash = Some(changeset.hash());
                    changeset.signature =
                        Some(Signature::try_from(&*tree_upgrade.signature).map_err(|_| {
                            HypercoreError::InvalidSignature {
                                context: "Could not parse changeset signature".to_string(),
                            }
                        })?);

                    // Update the header with this changeset to make in-memory value match that
                    // of the stored value.
                    oplog_open_outcome.oplog.update_header_with_changeset(
                        &changeset,
                        None,
                        &mut oplog_open_outcome.header,
                    )?;

                    // TODO: Skip reorg hints for now, seems to only have to do with replication
                    // addReorgHint(header.hints.reorgs, tree, batch)

                    // Commit changeset to in-memory tree
                    tree.commit(changeset)?;
                }
            }
        }

        let oplog = oplog_open_outcome.oplog;
        let header = oplog_open_outcome.header;
        let key_pair = header.key_pair.clone();

        Ok(Hypercore {
            key_pair,
            storage,
            oplog,
            tree,
            block_store,
            bitfield,
            header,
            skip_flush_count: 0,
            #[cfg(feature = "replication")]
            events: crate::replication::events::Events::new(),
        })
    }

    /// Gets basic info about the Hypercore
    pub fn info(&self) -> Info {
        Info {
            length: self.tree.length,
            byte_length: self.tree.byte_length,
            contiguous_length: self.header.hints.contiguous_length,
            fork: self.tree.fork,
            writeable: self.key_pair.secret.is_some(),
        }
    }

    /// Appends a data slice to the hypercore.
    #[instrument(err, skip_all, fields(data_len = data.len()))]
    pub async fn append(&mut self, data: &[u8]) -> Result<AppendOutcome, HypercoreError> {
        self.append_batch(&[data]).await
    }

    /// Appends a given batch of data slices to the hypercore.
    #[instrument(err, skip_all, fields(batch_len = batch.as_ref().len()))]
    pub async fn append_batch<A: AsRef<[u8]>, B: AsRef<[A]>>(
        &mut self,
        batch: B,
    ) -> Result<AppendOutcome, HypercoreError> {
        let secret_key = match &self.key_pair.secret {
            Some(key) => key,
            None => return Err(HypercoreError::NotWritable),
        };

        if !batch.as_ref().is_empty() {
            // Create a changeset for the tree
            let mut changeset = self.tree.changeset();
            let mut batch_length: usize = 0;
            for data in batch.as_ref().iter() {
                batch_length += changeset.append(data.as_ref());
            }
            changeset.hash_and_sign(secret_key);

            // Write the received data to the block store
            let info =
                self.block_store
                    .append_batch(batch.as_ref(), batch_length, self.tree.byte_length);
            self.storage.flush_info(info).await?;

            // Append the changeset to the Oplog
            let bitfield_update = BitfieldUpdate {
                drop: false,
                start: changeset.ancestors,
                length: changeset.batch_length,
            };
            let outcome = self.oplog.append_changeset(
                &changeset,
                Some(bitfield_update.clone()),
                false,
                &self.header,
            )?;
            self.storage.flush_infos(&outcome.infos_to_flush).await?;
            self.header = outcome.header;

            // Write to bitfield
            self.bitfield.update(&bitfield_update);

            // Contiguous length is known only now
            update_contiguous_length(&mut self.header, &self.bitfield, &bitfield_update);

            // Commit changeset to in-memory tree
            self.tree.commit(changeset)?;

            // Now ready to flush
            if self.should_flush_bitfield_and_tree_and_oplog() {
                self.flush_bitfield_and_tree_and_oplog(false).await?;
            }

            #[cfg(feature = "replication")]
            {
                use tracing::trace;

                trace!(bitfield_update = ?bitfield_update, "Hppercore.append_batch emit DataUpgrade & Have");
                let _ = self.events.send(crate::replication::events::DataUpgrade {});
                let _ = self
                    .events
                    .send(crate::replication::events::Have::from(&bitfield_update));
            }
        }

        // Return the new value
        Ok(AppendOutcome {
            length: self.tree.length,
            byte_length: self.tree.byte_length,
        })
    }

    #[cfg(feature = "replication")]
    /// Subscribe to core events relevant to replication
    pub fn event_subscribe(&self) -> async_broadcast::Receiver<crate::replication::events::Event> {
        self.events.channel.new_receiver()
    }

    /// Check if core has the block at the given `index` locally
    #[instrument(ret, skip(self))]
    pub fn has(&self, index: u64) -> bool {
        self.bitfield.get(index)
    }

    /// Read value at given index, if any.
    #[instrument(err, skip(self))]
    pub async fn get(&mut self, index: u64) -> Result<Option<Vec<u8>>, HypercoreError> {
        if !self.bitfield.get(index) {
            #[cfg(feature = "replication")]
            // if not in this core, emit Event::Get(index)
            {
                use tracing::trace;

                trace!(index = index, "Hppercore emit 'get' event");
                self.events.send_on_get(index);
            }
            return Ok(None);
        }

        let byte_range = self.byte_range(index, None).await?;

        // TODO: Generalize Either response stack
        let data = match self.block_store.read(&byte_range, None) {
            Either::Right(value) => value,
            Either::Left(instruction) => {
                let info = self.storage.read_info(instruction).await?;
                match self.block_store.read(&byte_range, Some(info)) {
                    Either::Right(value) => value,
                    Either::Left(_) => {
                        return Err(HypercoreError::InvalidOperation {
                            context: "Could not read block storage range".to_string(),
                        });
                    }
                }
            }
        };

        Ok(Some(data.to_vec()))
    }

    /// Clear data for entries between start and end (exclusive) indexes.
    #[instrument(err, skip(self))]
    pub async fn clear(&mut self, start: u64, end: u64) -> Result<(), HypercoreError> {
        if start >= end {
            // NB: This is what javascript does, so we mimic that here
            return Ok(());
        }
        // Write to oplog
        let infos_to_flush = self.oplog.clear(start, end)?;
        self.storage.flush_infos(&infos_to_flush).await?;

        // Set bitfield
        self.bitfield.set_range(start, end - start, false);

        // Set contiguous length
        if start < self.header.hints.contiguous_length {
            self.header.hints.contiguous_length = start;
        }

        // Find the biggest hole that can be punched into the data
        let start = if let Some(index) = self.bitfield.last_index_of(true, start) {
            index + 1
        } else {
            0
        };
        let end = if let Some(index) = self.bitfield.index_of(true, end) {
            index
        } else {
            self.tree.length
        };

        // Find byte offset for first value
        let mut infos: Vec<StoreInfo> = Vec::new();
        let clear_offset = match self.tree.byte_offset(start, None)? {
            Either::Right(value) => value,
            Either::Left(instructions) => {
                let new_infos = self.storage.read_infos_to_vec(&instructions).await?;
                infos.extend(new_infos);
                match self.tree.byte_offset(start, Some(&infos))? {
                    Either::Right(value) => value,
                    Either::Left(_) => {
                        return Err(HypercoreError::InvalidOperation {
                            context: format!("Could not read offset for index {start} from tree"),
                        });
                    }
                }
            }
        };

        // Find byte range for last value
        let last_byte_range = self.byte_range(end - 1, Some(&infos)).await?;

        let clear_length = (last_byte_range.index + last_byte_range.length) - clear_offset;

        // Clear blocks
        let info_to_flush = self.block_store.clear(clear_offset, clear_length);
        self.storage.flush_info(info_to_flush).await?;

        // Now ready to flush
        if self.should_flush_bitfield_and_tree_and_oplog() {
            self.flush_bitfield_and_tree_and_oplog(false).await?;
        }

        Ok(())
    }

    /// Access the key pair.
    pub fn key_pair(&self) -> &PartialKeypair {
        &self.key_pair
    }

    /// Create a proof for given request
    #[instrument(err, skip_all)]
    pub async fn create_proof(
        &mut self,
        block: Option<RequestBlock>,
        hash: Option<RequestBlock>,
        seek: Option<RequestSeek>,
        upgrade: Option<RequestUpgrade>,
    ) -> Result<Option<Proof>, HypercoreError> {
        let valueless_proof = self
            .create_valueless_proof(block, hash, seek, upgrade)
            .await?;
        let value: Option<Vec<u8>> = if let Some(block) = valueless_proof.block.as_ref() {
            let value = self.get(block.index).await?;
            if value.is_none() {
                // The data value requested in the proof can not be read, we return None here
                // and let the party requesting figure out what to do.
                return Ok(None);
            }
            value
        } else {
            None
        };
        Ok(Some(valueless_proof.into_proof(value)))
    }

    /// Verify and apply proof received from peer, returns true if changed, false if not
    /// possible to apply.
    #[instrument(skip_all)]
    pub async fn verify_and_apply_proof(&mut self, proof: &Proof) -> Result<bool, HypercoreError> {
        if proof.fork != self.tree.fork {
            return Ok(false);
        }
        let changeset = self.verify_proof(proof).await?;
        if !self.tree.commitable(&changeset) {
            return Ok(false);
        }

        // In javascript there's _verifyExclusive and _verifyShared based on changeset.upgraded, but
        // here we do only one. _verifyShared groups together many subsequent changesets into a single
        // oplog push, and then flushes in the end only for the whole group.
        let bitfield_update: Option<BitfieldUpdate> = if let Some(block) = &proof.block.as_ref() {
            let byte_offset =
                match self
                    .tree
                    .byte_offset_in_changeset(block.index, &changeset, None)?
                {
                    Either::Right(value) => value,
                    Either::Left(instructions) => {
                        let infos = self.storage.read_infos_to_vec(&instructions).await?;
                        match self.tree.byte_offset_in_changeset(
                            block.index,
                            &changeset,
                            Some(&infos),
                        )? {
                            Either::Right(value) => value,
                            Either::Left(_) => {
                                return Err(HypercoreError::InvalidOperation {
                                    context: format!(
                                        "Could not read offset for index {} from tree",
                                        block.index
                                    ),
                                });
                            }
                        }
                    }
                };

            // Write the value to the block store
            let info_to_flush = self.block_store.put(&block.value, byte_offset);
            self.storage.flush_info(info_to_flush).await?;

            // Return a bitfield update for the given value
            Some(BitfieldUpdate {
                drop: false,
                start: block.index,
                length: 1,
            })
        } else {
            // Only from DataBlock can there be changes to the bitfield
            None
        };

        // Append the changeset to the Oplog
        let outcome = self.oplog.append_changeset(
            &changeset,
            bitfield_update.clone(),
            false,
            &self.header,
        )?;
        self.storage.flush_infos(&outcome.infos_to_flush).await?;
        self.header = outcome.header;

        if let Some(bitfield_update) = &bitfield_update {
            // Write to bitfield
            self.bitfield.update(bitfield_update);

            // Contiguous length is known only now
            update_contiguous_length(&mut self.header, &self.bitfield, bitfield_update);
        }

        // Commit changeset to in-memory tree
        self.tree.commit(changeset)?;

        // Now ready to flush
        if self.should_flush_bitfield_and_tree_and_oplog() {
            self.flush_bitfield_and_tree_and_oplog(false).await?;
        }

        #[cfg(feature = "replication")]
        {
            if proof.upgrade.is_some() {
                // Notify replicator if we receieved an upgrade
                let _ = self.events.send(crate::replication::events::DataUpgrade {});
            }

            // Notify replicator if we receieved a bitfield update
            if let Some(ref bitfield) = bitfield_update {
                let _ = self
                    .events
                    .send(crate::replication::events::Have::from(bitfield));
            }
        }
        Ok(true)
    }

    /// Used to fill the nodes field of a `RequestBlock` during
    /// synchronization.
    #[instrument(err, skip(self))]
    pub async fn missing_nodes(&mut self, index: u64) -> Result<u64, HypercoreError> {
        self.missing_nodes_from_merkle_tree_index(index * 2).await
    }

    /// Get missing nodes using a merkle tree index. Advanced variant of missing_nodex
    /// that allow for special cases of searching directly from the merkle tree.
    #[instrument(err, skip(self))]
    pub async fn missing_nodes_from_merkle_tree_index(
        &mut self,
        merkle_tree_index: u64,
    ) -> Result<u64, HypercoreError> {
        match self.tree.missing_nodes(merkle_tree_index, None)? {
            Either::Right(value) => Ok(value),
            Either::Left(instructions) => {
                let mut instructions = instructions;
                let mut infos: Vec<StoreInfo> = vec![];
                loop {
                    infos.extend(self.storage.read_infos_to_vec(&instructions).await?);
                    match self.tree.missing_nodes(merkle_tree_index, Some(&infos))? {
                        Either::Right(value) => {
                            return Ok(value);
                        }
                        Either::Left(new_instructions) => {
                            instructions = new_instructions;
                        }
                    }
                }
            }
        }
    }

    /// Makes the hypercore read-only by deleting the secret key. Returns true if the
    /// hypercore was changed, false if the hypercore was already read-only. This is useful
    /// in scenarios where a hypercore should be made immutable after initial values have
    /// been stored.
    #[instrument(err, skip_all)]
    pub async fn make_read_only(&mut self) -> Result<bool, HypercoreError> {
        if self.key_pair.secret.is_some() {
            self.key_pair.secret = None;
            self.header.key_pair.secret = None;
            // Need to flush clearing traces to make sure both oplog slots are cleared
            self.flush_bitfield_and_tree_and_oplog(true).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn byte_range(
        &mut self,
        index: u64,
        initial_infos: Option<&[StoreInfo]>,
    ) -> Result<NodeByteRange, HypercoreError> {
        match self.tree.byte_range(index, initial_infos)? {
            Either::Right(value) => Ok(value),
            Either::Left(instructions) => {
                let mut instructions = instructions;
                let mut infos: Vec<StoreInfo> = vec![];
                loop {
                    infos.extend(self.storage.read_infos_to_vec(&instructions).await?);
                    match self.tree.byte_range(index, Some(&infos))? {
                        Either::Right(value) => {
                            return Ok(value);
                        }
                        Either::Left(new_instructions) => {
                            instructions = new_instructions;
                        }
                    }
                }
            }
        }
    }

    async fn create_valueless_proof(
        &mut self,
        block: Option<RequestBlock>,
        hash: Option<RequestBlock>,
        seek: Option<RequestSeek>,
        upgrade: Option<RequestUpgrade>,
    ) -> Result<ValuelessProof, HypercoreError> {
        match self.tree.create_valueless_proof(
            block.as_ref(),
            hash.as_ref(),
            seek.as_ref(),
            upgrade.as_ref(),
            None,
        )? {
            Either::Right(value) => Ok(value),
            Either::Left(instructions) => {
                let mut instructions = instructions;
                let mut infos: Vec<StoreInfo> = vec![];
                loop {
                    infos.extend(self.storage.read_infos_to_vec(&instructions).await?);
                    match self.tree.create_valueless_proof(
                        block.as_ref(),
                        hash.as_ref(),
                        seek.as_ref(),
                        upgrade.as_ref(),
                        Some(&infos),
                    )? {
                        Either::Right(value) => {
                            return Ok(value);
                        }
                        Either::Left(new_instructions) => {
                            instructions = new_instructions;
                        }
                    }
                }
            }
        }
    }

    /// Verify a proof received from a peer. Returns a changeset that should be
    /// applied.
    async fn verify_proof(&mut self, proof: &Proof) -> Result<MerkleTreeChangeset, HypercoreError> {
        match self.tree.verify_proof(proof, &self.key_pair.public, None)? {
            Either::Right(value) => Ok(value),
            Either::Left(instructions) => {
                let infos = self.storage.read_infos_to_vec(&instructions).await?;
                match self
                    .tree
                    .verify_proof(proof, &self.key_pair.public, Some(&infos))?
                {
                    Either::Right(value) => Ok(value),
                    Either::Left(_) => Err(HypercoreError::InvalidOperation {
                        context: "Could not verify proof from tree".to_string(),
                    }),
                }
            }
        }
    }

    fn should_flush_bitfield_and_tree_and_oplog(&mut self) -> bool {
        if self.skip_flush_count == 0
            || self.oplog.entries_byte_length >= MAX_OPLOG_ENTRIES_BYTE_SIZE
        {
            self.skip_flush_count = 3;
            true
        } else {
            self.skip_flush_count -= 1;
            false
        }
    }

    async fn flush_bitfield_and_tree_and_oplog(
        &mut self,
        clear_traces: bool,
    ) -> Result<(), HypercoreError> {
        let infos = self.bitfield.flush();
        self.storage.flush_infos(&infos).await?;
        let infos = self.tree.flush();
        self.storage.flush_infos(&infos).await?;
        let infos = self.oplog.flush(&self.header, clear_traces)?;
        self.storage.flush_infos(&infos).await?;
        Ok(())
    }
}

fn update_contiguous_length(
    header: &mut Header,
    bitfield: &Bitfield,
    bitfield_update: &BitfieldUpdate,
) {
    let end = bitfield_update.start + bitfield_update.length;
    let mut c = header.hints.contiguous_length;
    if bitfield_update.drop {
        if c <= end && c > bitfield_update.start {
            c = bitfield_update.start;
        }
    } else if c <= end && c >= bitfield_update.start {
        c = end;
        while bitfield.get(c) {
            c += 1;
        }
    }

    if c != header.hints.contiguous_length {
        header.hints.contiguous_length = c;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[async_std::test]
    async fn core_create_proof_block_only() -> Result<(), HypercoreError> {
        let mut hypercore = create_hypercore_with_data(10).await?;

        let proof = hypercore
            .create_proof(Some(RequestBlock { index: 4, nodes: 2 }), None, None, None)
            .await?
            .unwrap();
        let block = proof.block.unwrap();
        assert_eq!(proof.upgrade, None);
        assert_eq!(proof.seek, None);
        assert_eq!(block.index, 4);
        assert_eq!(block.nodes.len(), 2);
        assert_eq!(block.nodes[0].index, 10);
        assert_eq!(block.nodes[1].index, 13);
        Ok(())
    }

    #[async_std::test]
    async fn core_create_proof_block_and_upgrade() -> Result<(), HypercoreError> {
        let mut hypercore = create_hypercore_with_data(10).await?;
        let proof = hypercore
            .create_proof(
                Some(RequestBlock { index: 4, nodes: 0 }),
                None,
                None,
                Some(RequestUpgrade {
                    start: 0,
                    length: 10,
                }),
            )
            .await?
            .unwrap();
        let block = proof.block.unwrap();
        let upgrade = proof.upgrade.unwrap();
        assert_eq!(proof.seek, None);
        assert_eq!(block.index, 4);
        assert_eq!(block.nodes.len(), 3);
        assert_eq!(block.nodes[0].index, 10);
        assert_eq!(block.nodes[1].index, 13);
        assert_eq!(block.nodes[2].index, 3);
        assert_eq!(upgrade.start, 0);
        assert_eq!(upgrade.length, 10);
        assert_eq!(upgrade.nodes.len(), 1);
        assert_eq!(upgrade.nodes[0].index, 17);
        assert_eq!(upgrade.additional_nodes.len(), 0);
        Ok(())
    }

    #[async_std::test]
    async fn core_create_proof_block_and_upgrade_and_additional() -> Result<(), HypercoreError> {
        let mut hypercore = create_hypercore_with_data(10).await?;
        let proof = hypercore
            .create_proof(
                Some(RequestBlock { index: 4, nodes: 0 }),
                None,
                None,
                Some(RequestUpgrade {
                    start: 0,
                    length: 8,
                }),
            )
            .await?
            .unwrap();
        let block = proof.block.unwrap();
        let upgrade = proof.upgrade.unwrap();
        assert_eq!(proof.seek, None);
        assert_eq!(block.index, 4);
        assert_eq!(block.nodes.len(), 3);
        assert_eq!(block.nodes[0].index, 10);
        assert_eq!(block.nodes[1].index, 13);
        assert_eq!(block.nodes[2].index, 3);
        assert_eq!(upgrade.start, 0);
        assert_eq!(upgrade.length, 8);
        assert_eq!(upgrade.nodes.len(), 0);
        assert_eq!(upgrade.additional_nodes.len(), 1);
        assert_eq!(upgrade.additional_nodes[0].index, 17);
        Ok(())
    }

    #[async_std::test]
    async fn core_create_proof_block_and_upgrade_from_existing_state() -> Result<(), HypercoreError>
    {
        let mut hypercore = create_hypercore_with_data(10).await?;
        let proof = hypercore
            .create_proof(
                Some(RequestBlock { index: 1, nodes: 0 }),
                None,
                None,
                Some(RequestUpgrade {
                    start: 1,
                    length: 9,
                }),
            )
            .await?
            .unwrap();
        let block = proof.block.unwrap();
        let upgrade = proof.upgrade.unwrap();
        assert_eq!(proof.seek, None);
        assert_eq!(block.index, 1);
        assert_eq!(block.nodes.len(), 0);
        assert_eq!(upgrade.start, 1);
        assert_eq!(upgrade.length, 9);
        assert_eq!(upgrade.nodes.len(), 3);
        assert_eq!(upgrade.nodes[0].index, 5);
        assert_eq!(upgrade.nodes[1].index, 11);
        assert_eq!(upgrade.nodes[2].index, 17);
        assert_eq!(upgrade.additional_nodes.len(), 0);
        Ok(())
    }

    #[async_std::test]
    async fn core_create_proof_block_and_upgrade_from_existing_state_with_additional()
    -> Result<(), HypercoreError> {
        let mut hypercore = create_hypercore_with_data(10).await?;
        let proof = hypercore
            .create_proof(
                Some(RequestBlock { index: 1, nodes: 0 }),
                None,
                None,
                Some(RequestUpgrade {
                    start: 1,
                    length: 5,
                }),
            )
            .await?
            .unwrap();
        let block = proof.block.unwrap();
        let upgrade = proof.upgrade.unwrap();
        assert_eq!(proof.seek, None);
        assert_eq!(block.index, 1);
        assert_eq!(block.nodes.len(), 0);
        assert_eq!(upgrade.start, 1);
        assert_eq!(upgrade.length, 5);
        assert_eq!(upgrade.nodes.len(), 2);
        assert_eq!(upgrade.nodes[0].index, 5);
        assert_eq!(upgrade.nodes[1].index, 9);
        assert_eq!(upgrade.additional_nodes.len(), 2);
        assert_eq!(upgrade.additional_nodes[0].index, 13);
        assert_eq!(upgrade.additional_nodes[1].index, 17);
        Ok(())
    }

    #[async_std::test]
    async fn core_create_proof_block_and_seek_1_no_upgrade() -> Result<(), HypercoreError> {
        let mut hypercore = create_hypercore_with_data(10).await?;
        let proof = hypercore
            .create_proof(
                Some(RequestBlock { index: 4, nodes: 2 }),
                None,
                Some(RequestSeek { bytes: 8 }),
                None,
            )
            .await?
            .unwrap();
        let block = proof.block.unwrap();
        assert_eq!(proof.seek, None); // seek included in block
        assert_eq!(proof.upgrade, None);
        assert_eq!(block.index, 4);
        assert_eq!(block.nodes.len(), 2);
        assert_eq!(block.nodes[0].index, 10);
        assert_eq!(block.nodes[1].index, 13);
        Ok(())
    }

    #[async_std::test]
    async fn core_create_proof_block_and_seek_2_no_upgrade() -> Result<(), HypercoreError> {
        let mut hypercore = create_hypercore_with_data(10).await?;
        let proof = hypercore
            .create_proof(
                Some(RequestBlock { index: 4, nodes: 2 }),
                None,
                Some(RequestSeek { bytes: 10 }),
                None,
            )
            .await?
            .unwrap();
        let block = proof.block.unwrap();
        assert_eq!(proof.seek, None); // seek included in block
        assert_eq!(proof.upgrade, None);
        assert_eq!(block.index, 4);
        assert_eq!(block.nodes.len(), 2);
        assert_eq!(block.nodes[0].index, 10);
        assert_eq!(block.nodes[1].index, 13);
        Ok(())
    }

    #[async_std::test]
    async fn core_create_proof_block_and_seek_3_no_upgrade() -> Result<(), HypercoreError> {
        let mut hypercore = create_hypercore_with_data(10).await?;
        let proof = hypercore
            .create_proof(
                Some(RequestBlock { index: 4, nodes: 2 }),
                None,
                Some(RequestSeek { bytes: 13 }),
                None,
            )
            .await?
            .unwrap();
        let block = proof.block.unwrap();
        let seek = proof.seek.unwrap();
        assert_eq!(proof.upgrade, None);
        assert_eq!(block.index, 4);
        assert_eq!(block.nodes.len(), 1);
        assert_eq!(block.nodes[0].index, 10);
        assert_eq!(seek.nodes.len(), 2);
        assert_eq!(seek.nodes[0].index, 12);
        assert_eq!(seek.nodes[1].index, 14);
        Ok(())
    }

    #[async_std::test]
    async fn core_create_proof_block_and_seek_to_tree_no_upgrade() -> Result<(), HypercoreError> {
        let mut hypercore = create_hypercore_with_data(16).await?;
        let proof = hypercore
            .create_proof(
                Some(RequestBlock { index: 0, nodes: 4 }),
                None,
                Some(RequestSeek { bytes: 26 }),
                None,
            )
            .await?
            .unwrap();
        let block = proof.block.unwrap();
        let seek = proof.seek.unwrap();
        assert_eq!(proof.upgrade, None);
        assert_eq!(block.nodes.len(), 3);
        assert_eq!(block.nodes[0].index, 2);
        assert_eq!(block.nodes[1].index, 5);
        assert_eq!(block.nodes[2].index, 11);
        assert_eq!(seek.nodes.len(), 2);
        assert_eq!(seek.nodes[0].index, 19);
        assert_eq!(seek.nodes[1].index, 27);
        Ok(())
    }

    #[async_std::test]
    async fn core_create_proof_block_and_seek_with_upgrade() -> Result<(), HypercoreError> {
        let mut hypercore = create_hypercore_with_data(10).await?;
        let proof = hypercore
            .create_proof(
                Some(RequestBlock { index: 4, nodes: 2 }),
                None,
                Some(RequestSeek { bytes: 13 }),
                Some(RequestUpgrade {
                    start: 8,
                    length: 2,
                }),
            )
            .await?
            .unwrap();
        let block = proof.block.unwrap();
        let seek = proof.seek.unwrap();
        let upgrade = proof.upgrade.unwrap();
        assert_eq!(block.index, 4);
        assert_eq!(block.nodes.len(), 1);
        assert_eq!(block.nodes[0].index, 10);
        assert_eq!(seek.nodes.len(), 2);
        assert_eq!(seek.nodes[0].index, 12);
        assert_eq!(seek.nodes[1].index, 14);
        assert_eq!(upgrade.nodes.len(), 1);
        assert_eq!(upgrade.nodes[0].index, 17);
        assert_eq!(upgrade.additional_nodes.len(), 0);
        Ok(())
    }

    #[async_std::test]
    async fn core_create_proof_seek_with_upgrade() -> Result<(), HypercoreError> {
        let mut hypercore = create_hypercore_with_data(10).await?;
        let proof = hypercore
            .create_proof(
                None,
                None,
                Some(RequestSeek { bytes: 13 }),
                Some(RequestUpgrade {
                    start: 0,
                    length: 10,
                }),
            )
            .await?
            .unwrap();
        let seek = proof.seek.unwrap();
        let upgrade = proof.upgrade.unwrap();
        assert_eq!(proof.block, None);
        assert_eq!(seek.nodes.len(), 4);
        assert_eq!(seek.nodes[0].index, 12);
        assert_eq!(seek.nodes[1].index, 14);
        assert_eq!(seek.nodes[2].index, 9);
        assert_eq!(seek.nodes[3].index, 3);
        assert_eq!(upgrade.nodes.len(), 1);
        assert_eq!(upgrade.nodes[0].index, 17);
        assert_eq!(upgrade.additional_nodes.len(), 0);
        Ok(())
    }

    #[async_std::test]
    async fn core_verify_proof_invalid_signature() -> Result<(), HypercoreError> {
        let mut hypercore = create_hypercore_with_data(10).await?;
        // Invalid clone hypercore with a different public key
        let mut hypercore_clone = create_hypercore_with_data(0).await?;
        let proof = hypercore
            .create_proof(
                None,
                Some(RequestBlock { index: 6, nodes: 0 }),
                None,
                Some(RequestUpgrade {
                    start: 0,
                    length: 10,
                }),
            )
            .await?
            .unwrap();
        assert!(
            hypercore_clone
                .verify_and_apply_proof(&proof)
                .await
                .is_err()
        );
        Ok(())
    }

    #[async_std::test]
    async fn core_verify_and_apply_proof() -> Result<(), HypercoreError> {
        let mut main = create_hypercore_with_data(10).await?;
        let mut clone = create_hypercore_with_data_and_key_pair(
            0,
            PartialKeypair {
                public: main.key_pair.public,
                secret: None,
            },
        )
        .await?;
        let index = 6;
        let nodes = clone.missing_nodes(index).await?;
        let proof = main
            .create_proof(
                None,
                Some(RequestBlock { index, nodes }),
                None,
                Some(RequestUpgrade {
                    start: 0,
                    length: 10,
                }),
            )
            .await?
            .unwrap();
        assert!(clone.verify_and_apply_proof(&proof).await?);
        let main_info = main.info();
        let clone_info = clone.info();
        assert_eq!(main_info.byte_length, clone_info.byte_length);
        assert_eq!(main_info.length, clone_info.length);
        assert!(main.get(6).await?.is_some());
        assert!(clone.get(6).await?.is_none());

        // Fetch data for index 6 and verify it is found
        let index = 6;
        let nodes = clone.missing_nodes(index).await?;
        let proof = main
            .create_proof(Some(RequestBlock { index, nodes }), None, None, None)
            .await?
            .unwrap();
        assert!(clone.verify_and_apply_proof(&proof).await?);
        Ok(())
    }

    pub(crate) async fn create_hypercore_with_data(
        length: u64,
    ) -> Result<Hypercore, HypercoreError> {
        let signing_key = generate_signing_key();
        create_hypercore_with_data_and_key_pair(
            length,
            PartialKeypair {
                public: signing_key.verifying_key(),
                secret: Some(signing_key),
            },
        )
        .await
    }

    pub(crate) async fn create_hypercore_with_data_and_key_pair(
        length: u64,
        key_pair: PartialKeypair,
    ) -> Result<Hypercore, HypercoreError> {
        let storage = Storage::new_memory().await?;
        let mut hypercore = Hypercore::new(
            storage,
            HypercoreOptions {
                key_pair: Some(key_pair),
                open: false,
                #[cfg(feature = "cache")]
                node_cache_options: None,
            },
        )
        .await?;
        for i in 0..length {
            hypercore.append(format!("#{}", i).as_bytes()).await?;
        }
        Ok(hypercore)
    }
}
