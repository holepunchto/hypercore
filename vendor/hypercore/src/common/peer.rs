//! Types needed for passing information with with peers.
//! hypercore-protocol-rs uses these types and wraps them
//! into wire messages.

use hypercore_schema::{DataBlock, DataHash, DataSeek, DataUpgrade, Proof};

#[derive(Debug, Clone, PartialEq)]
/// Valueless proof generated from corresponding requests
pub(crate) struct ValuelessProof {
    pub(crate) fork: u64,
    /// Data block. NB: The ValuelessProof struct uses the Hash type because
    /// the stored binary value is processed externally to the proof.
    pub(crate) block: Option<DataHash>,
    pub(crate) hash: Option<DataHash>,
    pub(crate) seek: Option<DataSeek>,
    pub(crate) upgrade: Option<DataUpgrade>,
}

impl ValuelessProof {
    pub(crate) fn into_proof(mut self, block_value: Option<Vec<u8>>) -> Proof {
        let block = self.block.take().map(|block| DataBlock {
            index: block.index,
            nodes: block.nodes,
            value: block_value.expect("Data block needs to be given"),
        });
        Proof {
            fork: self.fork,
            block,
            hash: self.hash.take(),
            seek: self.seek.take(),
            upgrade: self.upgrade.take(),
        }
    }
}
