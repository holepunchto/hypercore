const HypercoreCache = require('hypercore-cache')

const DEFAULT_TREE_CACHE_SIZE = 65536 * 40

function createCache (opts) {
  const cacheOpts = opts.cache || {}
  if (cacheOpts.tree === undefined || typeof cacheOpts.tree === 'number') {
    let cacheSize = cacheOpts.tree || opts.storageCacheSize
    cacheOpts.tree = new HypercoreCache({
      maxByteSize: cacheSize !== undefined ? cacheSize : DEFAULT_TREE_CACHE_SIZE,
      estimateSize: () => 40
    })
  }
  if (cacheOpts.data === undefined) return cacheOpts
  if (typeof cacheOpts.data === 'number') {
    cacheOpts.data = new HypercoreCache({
      maxByteSize: cacheOpts.data,
      estimateSize: buf => buf.length
    })
  }
  return cacheOpts
}

module.exports = createCache
