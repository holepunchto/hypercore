use moka::sync::Cache;
use std::time::Duration;

use hypercore_schema::Node;

// Default to 1 year of cache
const DEFAULT_CACHE_TTL_SEC: u64 = 31556952;
const DEFAULT_CACHE_TTI_SEC: u64 = 31556952;
// Default to 100kb of node cache
const DEFAULT_CACHE_MAX_SIZE: u64 = 100000;
const NODE_WEIGHT: u32 =
    // Byte size of a Node based on the fields.
    3 * 8 + 32 + 4 +
    // Then 8 for key and guesstimate 8 bytes of overhead.
    8 + 8;

#[derive(Debug, Clone)]
pub(crate) struct CacheOptions {
    pub(crate) time_to_live: Option<Duration>,
    pub(crate) time_to_idle: Option<Duration>,
    pub(crate) max_capacity: Option<u64>,
}

impl CacheOptions {
    pub(crate) fn new() -> Self {
        Self {
            time_to_live: None,
            time_to_idle: None,
            max_capacity: None,
        }
    }

    pub(crate) fn to_node_cache(&self, initial_nodes: Vec<Node>) -> Cache<u64, Node> {
        let cache = if self.time_to_live.is_some() || self.time_to_idle.is_some() {
            Cache::builder()
                .time_to_live(
                    self.time_to_live
                        .unwrap_or_else(|| Duration::from_secs(DEFAULT_CACHE_TTL_SEC)),
                )
                .time_to_idle(
                    self.time_to_idle
                        .unwrap_or_else(|| Duration::from_secs(DEFAULT_CACHE_TTI_SEC)),
                )
                .max_capacity(self.max_capacity.unwrap_or(DEFAULT_CACHE_MAX_SIZE))
                .weigher(|_, _| NODE_WEIGHT)
                .build()
        } else {
            Cache::builder()
                .max_capacity(self.max_capacity.unwrap_or(DEFAULT_CACHE_MAX_SIZE))
                .weigher(|_, _| NODE_WEIGHT)
                .build()
        };
        for node in initial_nodes {
            cache.insert(node.index, node);
        }
        cache
    }
}
