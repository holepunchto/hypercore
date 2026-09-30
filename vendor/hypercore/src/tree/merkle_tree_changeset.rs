use ed25519_dalek::{Signature, SigningKey, VerifyingKey};
use std::convert::TryFrom;

use crate::{
    HypercoreError,
    crypto::{signable_tree, verify},
    sign,
};
use hypercore_schema::{Hash, Node};

/// Changeset for a `MerkleTree`. This allows to incrementally change a `MerkleTree` in two steps:
/// first create the changes to this changeset, get out information from this to put to the oplog,
/// and the commit the changeset to the tree.
///
/// This is called "MerkleTreeBatch" in Javascript, source
/// [here](https://github.com/holepunchto/hypercore/blob/88a1a2f1ebe6e33102688225516c4e882873f710/lib/merkle-tree.js#L44).
#[derive(Debug)]
pub(crate) struct MerkleTreeChangeset {
    pub(crate) length: u64,
    pub(crate) ancestors: u64,
    pub(crate) byte_length: u64,
    pub(crate) batch_length: u64,
    pub(crate) fork: u64,
    pub(crate) roots: Vec<Node>,
    pub(crate) nodes: Vec<Node>,
    pub(crate) hash: Option<Box<[u8]>>,
    pub(crate) signature: Option<Signature>,
    pub(crate) upgraded: bool,

    // Safeguarding values
    pub(crate) original_tree_length: u64,
    pub(crate) original_tree_fork: u64,
}

impl MerkleTreeChangeset {
    pub(crate) fn new(
        length: u64,
        byte_length: u64,
        fork: u64,
        roots: Vec<Node>,
    ) -> MerkleTreeChangeset {
        Self {
            length,
            ancestors: length,
            byte_length,
            batch_length: 0,
            fork,
            roots,
            nodes: vec![],
            hash: None,
            signature: None,
            upgraded: false,
            original_tree_length: length,
            original_tree_fork: fork,
        }
    }

    pub(crate) fn append(&mut self, data: &[u8]) -> usize {
        let len = data.len();
        let head = self.length * 2;
        let mut iter = flat_tree::Iterator::new(head);
        let node = Node::new(head, Hash::data(data).as_bytes().to_vec(), len as u64);
        self.append_root(node, &mut iter);
        self.batch_length += 1;
        len
    }

    pub(crate) fn append_root(&mut self, node: Node, iter: &mut flat_tree::Iterator) {
        self.upgraded = true;
        self.length += iter.factor() / 2;
        self.byte_length += node.length;
        self.roots.push(node.clone());
        self.nodes.push(node);

        while self.roots.len() > 1 {
            let a = &self.roots[self.roots.len() - 1];
            let b = &self.roots[self.roots.len() - 2];
            if iter.sibling() != b.index {
                iter.sibling(); // unset so it always points to last root
                break;
            }

            let node = Node::new(
                iter.parent(),
                Hash::parent(a, b).as_bytes().into(),
                a.length + b.length,
            );
            let _ = &self.nodes.push(node.clone());
            let _ = &self.roots.pop();
            let _ = &self.roots.pop();
            let _ = &self.roots.push(node);
        }
    }

    /// Hashes and signs the changeset
    pub(crate) fn hash_and_sign(&mut self, signing_key: &SigningKey) {
        let hash = self.hash();
        let signable = self.signable(&hash);
        let signature = sign(signing_key, &signable);
        self.hash = Some(hash);
        self.signature = Some(signature);
    }

    /// Verify and set signature with given public key
    pub(crate) fn verify_and_set_signature(
        &mut self,
        signature: &[u8],
        public_key: &VerifyingKey,
    ) -> Result<(), HypercoreError> {
        // Verify that the received signature matches the public key
        let signature =
            Signature::try_from(signature).map_err(|_| HypercoreError::InvalidSignature {
                context: "Could not parse signature".to_string(),
            })?;
        let hash = self.hash();
        verify(public_key, &self.signable(&hash), Some(&signature))?;

        // Set values to changeset
        self.hash = Some(hash);
        self.signature = Some(signature);
        Ok(())
    }

    /// Calculates a hash of the current set of roots
    pub(crate) fn hash(&self) -> Box<[u8]> {
        Hash::tree(&self.roots).as_bytes().into()
    }

    /// Creates a signable slice from given hash
    pub(crate) fn signable(&self, hash: &[u8]) -> Box<[u8]> {
        signable_tree(hash, self.length, self.fork)
    }
}
