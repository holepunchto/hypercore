use std::fmt::Debug;
#[cfg(feature = "cache")]
use std::time::Duration;
use tracing::instrument;

#[cfg(feature = "cache")]
use crate::common::cache::CacheOptions;
use crate::{Hypercore, HypercoreError, PartialKeypair, Storage, core::HypercoreOptions};

/// Build CacheOptions.
#[cfg(feature = "cache")]
#[derive(Debug)]
pub struct CacheOptionsBuilder(CacheOptions);

#[cfg(feature = "cache")]
impl Default for CacheOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "cache")]
impl CacheOptionsBuilder {
    /// Create a CacheOptions builder with default options
    pub fn new() -> Self {
        Self(CacheOptions::new())
    }

    /// Set cache time to live.
    pub fn time_to_live(mut self, time_to_live: Duration) -> Self {
        self.0.time_to_live = Some(time_to_live);
        self
    }

    /// Set cache time to idle.
    pub fn time_to_idle(mut self, time_to_idle: Duration) -> Self {
        self.0.time_to_idle = Some(time_to_idle);
        self
    }

    /// Set cache max capacity in bytes.
    pub fn max_capacity(mut self, max_capacity: u64) -> Self {
        self.0.max_capacity = Some(max_capacity);
        self
    }

    /// Build new cache options.
    pub(crate) fn build(self) -> CacheOptions {
        self.0
    }
}

/// Build a Hypercore instance with options.
#[derive(Debug)]
pub struct HypercoreBuilder {
    storage: Storage,
    options: HypercoreOptions,
}

impl HypercoreBuilder {
    /// Create a hypercore builder with a given storage
    pub fn new(storage: Storage) -> Self {
        Self {
            storage,
            options: HypercoreOptions::new(),
        }
    }

    /// Set key pair.
    pub fn key_pair(mut self, key_pair: PartialKeypair) -> Self {
        self.options.key_pair = Some(key_pair);
        self
    }

    /// Set open.
    pub fn open(mut self, open: bool) -> Self {
        self.options.open = open;
        self
    }

    /// Set node cache options.
    #[cfg(feature = "cache")]
    pub fn node_cache_options(mut self, builder: CacheOptionsBuilder) -> Self {
        self.options.node_cache_options = Some(builder.build());
        self
    }

    /// Build a new Hypercore.
    #[instrument(err, skip_all)]
    pub async fn build(self) -> Result<Hypercore, HypercoreError> {
        Hypercore::new(self.storage, self.options).await
    }
}
