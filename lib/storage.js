var uint64be = require('uint64be')
var flat = require('flat-tree')
var raf = require('random-access-file')
var createCache = require('./cache')

module.exports = Storage

var noarr = []

function Storage (createStorage, opts) {
  if (!(this instanceof Storage)) return new Storage(createStorage, opts)

  if (typeof createStorage === 'string') createStorage = defaultStorage(createStorage)
  if (typeof createStorage !== 'function') throw new Error('Storage should be a function or string')

  const cache = createCache(opts)

  this.treeCache = cache.tree || null
  this.dataCache = cache.data || null
  this._key = null
  this._secretKey = null
  this._tree = null
  this._data = null
  this._bitfield = null
  this._signatures = null
  this._create = createStorage
}

Storage.prototype.putData = function (index, data, nodes, cb) {
  if (!cb) cb = noop
  var self = this
  if (!data.length) return cb(null)
  this._dataOffset(index, nodes, function (err, offset, size) {
    if (err) return cb(err)
    if (size !== data.length) return cb(new Error('Unexpected data size'))
    self._data.write(offset, data, cb)
  })
}

/**
 * @param {number} index
 * @param {Buffer[]} dataBatch
 * @param {object} [opts]
 * @param {number} [opts.byteOffset] If known, byteOffset of index, to avoid recalculation
 * @param {Node[]} [opts.cachedNodes] Cached nodes to speed up calculation of byteOffset of index
 */
Storage.prototype.putDataBatch = function (index, dataBatch, opts, cb) {
  if (typeof cb === 'undefined' && typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  opts = opts || {}
  var self = this
  var cachedNodes = opts.cachedNodes || []

  if (typeof opts.byteOffset === 'number') {
    onOffset(null, opts.byteOffset)
  } else {
    this._dataOffset(index, cachedNodes, onOffset)
  }

  function onOffset (err, offset) {
    if (err) return cb(err)
    if (dataBatch.length === 1) {
      self._data.write(offset, dataBatch[0], cb)
    } else {
      self._data.write(offset, Buffer.concat(dataBatch), cb)
    }
  }
}

Storage.prototype.getData = function (index, cb) {
  var self = this
  var cached = this.dataCache && this.dataCache.get(index)
  if (cached) return process.nextTick(cb, null, cached)
  this._dataOffset(index, noarr, function (err, offset, size) {
    if (err) return cb(err)
    self._data.read(offset, size, (err, data) => {
      if (err) return cb(err)
      if (self.dataCache) self.dataCache.set(index, data)
      return cb(null, data)
    })
  })
}

Storage.prototype.clearData = function (start, end, opts, cb) {
  if (typeof end === 'function') return this.clearData(start, start + 1, null, end)
  if (typeof opts === 'function') return this.clearData(start, end, null, opts)
  if (!opts) opts = {}
  if (!end) end = start + 1
  if (!cb) cb = noop

  var self = this
  var byteOffset = start === 0 ? 0 : (typeof opts.byteOffset === 'number' ? opts.byteOffset : -1)
  var byteLength = typeof opts.byteLength === 'number' ? opts.byteLength : -1

  if (byteOffset > -1) return onstartbytes(null, byteOffset)
  this._dataOffset(start, [], onstartbytes)

  function onstartbytes (err, offset) {
    if (err) return cb(err)
    byteOffset = offset
    if (byteLength > -1) return onendbytes(null, byteLength + byteOffset)
    // TODO: shortcut if (end === self.length)
    self._dataOffset(end, [], onendbytes)
  }

  function onendbytes (err, end) {
    if (err) return cb(err)
    if (!self._data.del) return cb() // Not all data storage impls del
    self._data.del(byteOffset, end - byteOffset, cb)
  }
}

Storage.prototype.nextSignature = function (index, cb) {
  var self = this

  this._getSignature(index, function (err, signature) {
    if (err) return cb(err)
    if (isBlank(signature)) return self.nextSignature(index + 1, cb)
    cb(null, { index: index, signature: signature })
  })
}

Storage.prototype.getSignature = function (index, cb) {
  this._getSignature(index, function (err, signature) {
    if (err) return cb(err)
    if (isBlank(signature)) return cb(new Error('No signature found'))
    cb(null, signature)
  })
}

// Caching not enabled for signatures because they are rarely reused.
Storage.prototype._getSignature = function (index, cb) {
  this._signatures.read(32 + 64 * index, 64, cb)
}

Storage.prototype.putSignature = function (index, signature, cb) {
  this._signatures.write(32 + 64 * index, signature, cb)
}

Storage.prototype.deleteSignatures = function (start, end, cb) {
  this._signatures.del(32 + 64 * start, (end - start) * 64, cb)
}

Storage.prototype._dataOffset = function (index, cachedNodes, cb) {
  var roots = flat.fullRoots(2 * index)
  var self = this
  var offset = 0
  var pending = roots.length
  var error = null
  var blk = 2 * index

  if (!pending) {
    pending = 1
    onnode(null, null)
    return
  }

  for (var i = 0; i < roots.length; i++) {
    var node = findNode(cachedNodes, roots[i])
    if (node) onnode(null, node)
    else this.getNode(roots[i], onnode)
  }

  function onlast (err, node) {
    if (err) return cb(err)
    cb(null, offset, node.size)
  }

  function onnode (err, node) {
    if (err) error = err
    if (node) offset += node.size
    if (--pending) return

    if (error) return cb(error)

    var last = findNode(cachedNodes, blk)
    if (last) onlast(null, last)
    else self.getNode(blk, onlast)
  }
}

// Caching not enabled for batch reads because they'd be complicated to batch and they're rarely used.
Storage.prototype.getDataBatch = function (start, n, cb) {
  var result = new Array(n)
  var sizes = new Array(n)
  var self = this

  this._dataOffset(start, noarr, function (err, offset, size) {
    if (err) return cb(err)

    start++
    n--

    if (n <= 0) return ontree(null, null)
    self._tree.read(32 + 80 * start, 80 * n - 40, ontree)

    function ontree (err, buf) {
      if (err) return cb(err)

      var total = sizes[0] = size

      if (buf) {
        for (var i = 1; i < sizes.length; i++) {
          sizes[i] = uint64be.decode(buf, 32 + (i - 1) * 80)
          total += sizes[i]
        }
      }

      self._data.read(offset, total, ondata)
    }

    function ondata (err, buf) {
      if (err) return cb(err)
      var total = 0
      for (var i = 0; i < result.length; i++) {
        result[i] = buf.slice(total, total += sizes[i])
      }

      cb(null, result)
    }
  })
}

Storage.prototype.getNode = function (index, cb) {
  if (this.treeCache) {
    var cached = this.treeCache.get(index)
    if (cached) return cb(null, cached)
  }

  var self = this

  this._tree.read(32 + 40 * index, 40, function (err, buf) {
    if (err) return cb(err)

    var hash = buf.slice(0, 32)
    var size = uint64be.decode(buf, 32)

    if (!size && isBlank(hash)) return cb(new Error('No node found'))

    var val = new Node(index, hash, size, null)
    if (self.treeCache) self.treeCache.set(index, val)
    cb(null, val)
  })
}

Storage.prototype.putNodeBatch = function (index, nodes, cb) {
  if (!cb) cb = noop

  var buf = Buffer.alloc(nodes.length * 40)

  for (var i = 0; i < nodes.length; i++) {
    var offset = i * 40
    var node = nodes[i]
    if (!node) continue
    node.hash.copy(buf, offset)
    uint64be.encode(node.size, buf, 32 + offset)
  }

  this._tree.write(32 + 40 * index, buf, cb)
}

Storage.prototype.putNode = function (index, node, cb) {
  if (!cb) cb = noop

  // TODO: re-enable put cache. currently this causes a memleak
  // because node.hash is a slice of the big data buffer on replicate
  // if (this.cache) this.cache.set(index, node)

  var buf = Buffer.allocUnsafe(40)

  node.hash.copy(buf, 0)
  uint64be.encode(node.size, buf, 32)
  this._tree.write(32 + 40 * index, buf, cb)
}

Storage.prototype.putBitfield = function (offset, data, cb) {
  this._bitfield.write(32 + offset, data, cb)
}

Storage.prototype.delBitfield = function (cb) {
  this._bitfield.del(32, Infinity, cb)
}

Storage.prototype.close = function (cb) {
  if (!cb) cb = noop
  var missing = 6
  var error = null

  close(this._bitfield, done)
  close(this._tree, done)
  close(this._data, done)
  close(this._key, done)
  close(this._secretKey, done)
  close(this._signatures, done)

  function done (err) {
    if (err) error = err
    if (--missing) return
    cb(error)
  }
}

Storage.prototype.destroy = function (cb) {
  if (!cb) cb = noop
  var missing = 6
  var error = null

  destroy(this._bitfield, done)
  destroy(this._tree, done)
  destroy(this._data, done)
  destroy(this._key, done)
  destroy(this._secretKey, done)
  destroy(this._signatures, done)

  function done (err) {
    if (err) error = err
    if (--missing) return
    cb(error)
  }
}

Storage.prototype.openKey = function (opts, cb) {
  if (typeof opts === 'function') return this.openKey({}, opts)
  if (!this._key) this._key = this._create('key', opts)
  this._key.read(0, 32, cb)
}

Storage.prototype.writeKey = function (key, cb) {
  this._key.write(0, key, cb)
}

Storage.prototype.writeSecretKey = function (key, cb) {
  this._secretKey.write(0, key, cb)
}

Storage.prototype.open = function (opts, cb) {
  if (typeof opts === 'function') return this.open({}, opts)

  var self = this
  var error = null
  var missing = 5

  if (!this._key) this._key = this._create('key', opts)
  if (!this._secretKey) this._secretKey = this._create('secret_key', opts)
  if (!this._tree) this._tree = this._create('tree', opts)
  if (!this._data) this._data = this._create('data', opts)
  if (!this._bitfield) this._bitfield = this._create('bitfield', opts)
  if (!this._signatures) this._signatures = this._create('signatures', opts)

  var result = {
    bitfield: [],
    bitfieldPageSize: 3584, // we upgraded the page size to fix a bug
    secretKey: null,
    key: null
  }

  this._bitfield.read(0, 32, function (err, h) {
    if (err && err.code === 'ELOCKED') return cb(err)
    if (h) result.bitfieldPageSize = h.readUInt16BE(5)
    self._bitfield.write(0, header(0, result.bitfieldPageSize, null), function (err) {
      if (err) return cb(err)
      readAll(self._bitfield, 32, result.bitfieldPageSize, function (err, pages) {
        if (pages) result.bitfield = pages
        done(err)
      })
    })
  })

  this._signatures.write(0, header(1, 64, 'Ed25519'), done)
  this._tree.write(0, header(2, 40, 'BLAKE2b'), done)

  // TODO: Improve the error handling here.
  // I.e. if secretKey length === 64 and it fails, error

  this._secretKey.read(0, 64, function (_, data) {
    if (data) result.secretKey = data
    done(null)
  })

  this._key.read(0, 32, function (_, data) {
    if (data) result.key = data
    done(null)
  })

  function done (err) {
    if (err) error = err
    if (--missing) return
    if (error) cb(error)
    else cb(null, result)
  }
}

Storage.Node = Node

function noop () {}

function header (type, size, name) {
  var buf = Buffer.alloc(32)

  // magic number
  buf[0] = 5
  buf[1] = 2
  buf[2] = 87
  buf[3] = type

  // version
  buf[4] = 0

  // block size
  buf.writeUInt16BE(size, 5)

  if (name) {
    // algo name
    buf[7] = name.length
    buf.write(name, 8)
  }

  return buf
}

function Node (index, hash, size) {
  this.index = index
  this.hash = hash
  this.size = size
}

function findNode (nodes, index) {
  for (var i = 0; i < nodes.length; i++) {
    if (nodes[i].index === index) return nodes[i]
  }
  return null
}

function isBlank (buf) {
  for (var i = 0; i < buf.length; i++) {
    if (buf[i]) return false
  }
  return true
}

function close (st, cb) {
  if (st.close) st.close(cb)
  else cb()
}

function destroy (st, cb) {
  if (st.destroy) st.destroy(cb)
  else cb()
}

function statAndReadAll (st, offset, pageSize, cb) {
  st.stat(function (err, stat) {
    if (err) return cb(null, [])

    var result = []

    loop(null, null)

    function loop (err, batch) {
      if (err) return cb(err)

      if (batch) {
        offset += batch.length
        for (var i = 0; i < batch.length; i += pageSize) {
          result.push(batch.slice(i, i + pageSize))
        }
      }

      var next = Math.min(stat.size - offset, 32 * pageSize)
      if (!next) return cb(null, result)

      st.read(offset, next, loop)
    }
  })
}

function readAll (st, offset, pageSize, cb) {
  if (st.statable === true) return statAndReadAll(st, offset, pageSize, cb)

  var bufs = []

  st.read(offset, pageSize, loop)

  function loop (err, buf) {
    if (err) return cb(null, bufs)
    bufs.push(buf)
    st.read(offset + bufs.length * pageSize, pageSize, loop)
  }
}

function defaultStorage (dir) {
  return function (name) {
    try {
      var lock = name === 'bitfield' ? require('fd-lock') : null
    } catch (err) {}
    return raf(name, { directory: dir, lock: lock })
  }
}
