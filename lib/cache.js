const HypercoreCache = require('hypercore-cache')

const DEFAULT_TREE_CACHE_SIZE = 65536 * 40

function createCache (opts) {
  if (opts.cache === false) return {}

  const cacheOpts = opts.cache || {}
  if (cacheOpts.tree === undefined || typeof cacheOpts.tree === 'number') {
    const cacheSize = cacheOpts.tree || opts.storageCacheSize
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
