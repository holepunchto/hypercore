//! Hypercore to Hypercore replication
pub mod events;
#[cfg(feature = "shared-core")]
pub mod shared_core;

#[cfg(feature = "shared-core")]
pub use shared_core::SharedCore;

use crate::{AppendOutcome, HypercoreError, Info, PartialKeypair};

use hypercore_schema::{Proof, RequestBlock, RequestSeek, RequestUpgrade};

pub use events::Event;

use async_broadcast::Receiver;
use std::future::Future;

/// Methods related to just this core's information
pub trait CoreInfo {
    /// Get core info (see: [`crate::Hypercore::info`]
    fn info(&self) -> impl Future<Output = Info> + Send;
    /// Get the key_pair (see: [`crate::Hypercore::key_pair`]
    fn key_pair(&self) -> impl Future<Output = PartialKeypair> + Send;
}

/// Error for ReplicationMethods trait
#[derive(thiserror::Error, Debug)]
pub enum ReplicationMethodsError {
    /// Error from hypercore
    #[error("Got a hypercore error: [{0}]")]
    HypercoreError(#[from] HypercoreError),
    /// Error from CoreMethods
    #[error("Got a CoreMethods error: [{0}]")]
    CoreMethodsError(#[from] CoreMethodsError),
}

/// Methods needed for replication
pub trait ReplicationMethods: CoreInfo + Send {
    /// ref Core::verify_and_apply_proof
    fn verify_and_apply_proof(
        &self,
        proof: &Proof,
    ) -> impl Future<Output = Result<bool, ReplicationMethodsError>> + Send;
    /// ref Core::missing_nodes
    fn missing_nodes(
        &self,
        index: u64,
    ) -> impl Future<Output = Result<u64, ReplicationMethodsError>> + Send;
    /// ref Core::create_proof
    fn create_proof(
        &self,
        block: Option<RequestBlock>,
        hash: Option<RequestBlock>,
        seek: Option<RequestSeek>,
        upgrade: Option<RequestUpgrade>,
    ) -> impl Future<Output = Result<Option<Proof>, ReplicationMethodsError>> + Send;
    /// subscribe to core events
    fn event_subscribe(&self) -> impl Future<Output = Receiver<Event>>;
}

/// Error for CoreMethods trait
#[derive(thiserror::Error, Debug)]
pub enum CoreMethodsError {
    /// Error from hypercore
    #[error("Got a hypercore error [{0}]")]
    HypercoreError(#[from] HypercoreError),
}

/// Trait for things that consume [`crate::Hypercore`] can instead use this trait
/// so they can use all Hypercore-like things such as `SharedCore`.
pub trait CoreMethods: CoreInfo {
    /// Check if the core has the block at the given index locally
    fn has(&self, index: u64) -> impl Future<Output = bool> + Send;

    /// get a block
    fn get(
        &self,
        index: u64,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, CoreMethodsError>> + Send;

    /// Append data to the core
    fn append(
        &self,
        data: &[u8],
    ) -> impl Future<Output = Result<AppendOutcome, CoreMethodsError>> + Send;

    /// Append a batch of data to the core
    fn append_batch<A: AsRef<[u8]>, B: AsRef<[A]> + Send>(
        &self,
        batch: B,
    ) -> impl Future<Output = Result<AppendOutcome, CoreMethodsError>> + Send;
}
