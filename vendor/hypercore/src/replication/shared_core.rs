//! Implementation of a Hypercore that can have multiple owners. Along with implementations of all
//! the hypercore traits.
use crate::{AppendOutcome, Hypercore, Info, PartialKeypair};
use async_broadcast::Receiver;
use async_lock::Mutex;
use hypercore_schema::{Proof, RequestBlock, RequestSeek, RequestUpgrade};
use std::sync::Arc;

use super::{
    CoreInfo, CoreMethods, CoreMethodsError, Event, ReplicationMethods, ReplicationMethodsError,
};

/// Hypercore that can have multiple owners
#[derive(Debug, Clone)]
pub struct SharedCore(pub Arc<Mutex<Hypercore>>);

impl From<Hypercore> for SharedCore {
    fn from(core: Hypercore) -> Self {
        SharedCore(Arc::new(Mutex::new(core)))
    }
}
impl SharedCore {
    /// Create a shared core from a [`Hypercore`]
    pub fn from_hypercore(core: Hypercore) -> Self {
        SharedCore(Arc::new(Mutex::new(core)))
    }
}

impl CoreInfo for SharedCore {
    async fn info(&self) -> Info {
        let core = &self.0.lock().await;
        core.info()
    }

    async fn key_pair(&self) -> PartialKeypair {
        let core = &self.0.lock().await;
        core.key_pair().clone()
    }
}

impl ReplicationMethods for SharedCore {
    async fn verify_and_apply_proof(&self, proof: &Proof) -> Result<bool, ReplicationMethodsError> {
        Ok(self.0.lock().await.verify_and_apply_proof(proof).await?)
    }

    async fn missing_nodes(&self, index: u64) -> Result<u64, ReplicationMethodsError> {
        Ok(self.0.lock().await.missing_nodes(index).await?)
    }

    async fn create_proof(
        &self,
        block: Option<RequestBlock>,
        hash: Option<RequestBlock>,
        seek: Option<RequestSeek>,
        upgrade: Option<RequestUpgrade>,
    ) -> Result<Option<Proof>, ReplicationMethodsError> {
        Ok(self
            .0
            .lock()
            .await
            .create_proof(block, hash, seek, upgrade)
            .await?)
    }

    async fn event_subscribe(&self) -> Receiver<Event> {
        self.0.lock().await.event_subscribe()
    }
}

impl CoreMethods for SharedCore {
    async fn has(&self, index: u64) -> bool {
        self.0.lock().await.has(index)
    }
    async fn get(&self, index: u64) -> Result<Option<Vec<u8>>, CoreMethodsError> {
        Ok(self.0.lock().await.get(index).await?)
    }

    async fn append(&self, data: &[u8]) -> Result<AppendOutcome, CoreMethodsError> {
        Ok(self.0.lock().await.append(data).await?)
    }

    async fn append_batch<A: AsRef<[u8]>, B: AsRef<[A]> + Send>(
        &self,
        batch: B,
    ) -> Result<AppendOutcome, CoreMethodsError> {
        Ok(self.0.lock().await.append_batch(batch).await?)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::core::tests::{create_hypercore_with_data, create_hypercore_with_data_and_key_pair};
    #[async_std::test]
    async fn shared_core_methods() -> Result<(), CoreMethodsError> {
        let core = crate::core::tests::create_hypercore_with_data(0).await?;
        let core = SharedCore::from(core);

        // check CoreInfo
        let info = core.info().await;
        assert_eq!(
            info,
            crate::core::Info {
                length: 0,
                byte_length: 0,
                contiguous_length: 0,
                fork: 0,
                writeable: true,
            }
        );

        // key_pair is random, nothing to test here
        let _kp = core.key_pair().await;

        // check CoreMethods
        assert_eq!(core.has(0).await, false);
        assert_eq!(core.get(0).await?, None);
        let res = core.append(b"foo").await?;
        assert_eq!(
            res,
            AppendOutcome {
                length: 1,
                byte_length: 3
            }
        );
        assert_eq!(core.has(0).await, true);
        assert_eq!(core.get(0).await?, Some(b"foo".into()));
        let res = core.append_batch([b"hello", b"world"]).await?;
        assert_eq!(
            res,
            AppendOutcome {
                length: 3,
                byte_length: 13
            }
        );
        assert_eq!(core.has(2).await, true);
        assert_eq!(core.get(2).await?, Some(b"world".into()));
        Ok(())
    }

    #[async_std::test]
    async fn shared_core_replication_methods() -> Result<(), ReplicationMethodsError> {
        let main = create_hypercore_with_data(10).await?;
        let clone = create_hypercore_with_data_and_key_pair(
            0,
            PartialKeypair {
                public: main.key_pair.public,
                secret: None,
            },
        )
        .await?;

        let main = SharedCore::from(main);
        let clone = SharedCore::from(clone);

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
        let main_info = main.info().await;
        let clone_info = clone.info().await;
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
}
