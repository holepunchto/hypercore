var low = require('last-one-wins')
var inherits = require('inherits')
var flat = require('flat-tree')
var randomBytes = require('randombytes')
var merkle = require('merkle-tree-stream/generator')
var events = require('events')
var thunky = require('thunky')
var signatures = require('sodium-signatures')
var equals = require('buffer-equals')
var bitfield = require('./bitfield')
var hash = require('./hash')
var tree = require('./tree-index')
var messages = require('./messages')
var storage = require('./storage')
var replicate = require('./replicate')
var selection = require('./selection')

module.exports = Feed

function Feed (core, opts) {
  if (!(this instanceof Feed)) return new Feed(core, opts)
  events.EventEmitter.call(this)

  var self = this

  this.options = opts || {}
  this.key = this.options.key || null
  if (this.key && this.key.length !== 32) throw new Error('key should be a 32 byte buffer')

  this.secretKey = this.options.secretKey || null
  this.discoveryKey = this.key ? hash.discoveryKey(this.key) : null
  this.prefix = null
  this.live = !!this.options.live || !!this.secretKey
  this.open = thunky(open)
  this.opened = false
  this.tree = tree()
  this.bitfield = bitfield(8)
  this.blocks = 0
  this.bytes = 0
  this.storage = opts.storage ? storage(this, opts.storage) : null
  if (!this.storage && core._storage) this.storage = storage(this, core._storage(this))
  this.valueEncoding = opts.valueEncoding
  this.verifyReplicationReads = opts.verifyReplicationReads

  this.peers = []
  this.selection = selection(this)

  if (this.live && !this.key) {
    var keyPair = signatures.keyPair()
    this.key = keyPair.publicKey
    this.secretKey = keyPair.secretKey
    this.discoveryKey = hash.discoveryKey(this.key)
  }

  if (!opts.sparse) {
    this.selection.add({priority: 0, start: 0, end: Infinity})
  }

  this._sync = low(sync)
  this._first = true
  this._db = core._db
  this._core = core
  this._prefix = null
  this._merkle = null
  this._missing = 0
  this._appendCallback = null
  this._buffer = null
  this._bufferCallbacks = null
  this._afterAppend = afterAppend
  this._appending = 0
  this._appendingBytes = 0
  this._downloaded = 0

  var callbacks = null

  function afterAppend () {
    // TODO: this needs error handling!
    if (--self._missing) return

    while (self._appending) {
      var blk = self.blocks++
      self.tree.set(2 * blk)
      self.bitfield.set(blk, true)
      self._appending--
      self._drain(blk)
      self.emit('update')
    }

    self.bytes += self._appendingBytes
    self._appendingBytes = 0

    if (self.live) self._sync(null, afterSync)
    else afterSync(null)
  }

  function afterSync (err) {
    self._appendCallback(err)
    self._appendCallback = null
    callbacks = null

    if (self._buffer) {
      callbacks = self._bufferCallbacks
      self._append(self._buffer, bufferCallback)
    }
  }

  function bufferCallback (err) {
    for (var i = 0; i < callbacks.length; i++) callbacks[i](err)
  }

  function open (cb) {
    self._open(cb)
  }

  function sync (_, cb) {
    self._flush(cb)
  }
}

inherits(Feed, events.EventEmitter)

Feed.prototype.createReadStream = function (opts) {
  if (!opts) opts = {}
  opts.feed = this
  return this._core.createReadStream(null, opts)
}

Feed.prototype.createWriteStream = function (opts) {
  if (!opts) opts = {}
  opts.feed = this
  return this._core.createWriteStream(null, opts)
}

Feed.prototype.prioritize = function (opts, cb) {
  var range = this.selection.add(opts, cb)
  this._updatePeers()
  return range
}

Feed.prototype.unprioritize = function (opts) {
  return this.selection.remove(opts)
}

Feed.prototype.replicate = function (opts) {
  if (isStream(opts)) opts = {stream: opts}
  return replicate(this._core, this, opts)
}

Feed.prototype.unreplicate = function (stream) {
  return replicate.unreplicate(this._core, this, stream)
}

Feed.prototype.head = function (block, cb) {
  if (typeof block === 'function') return this.head(this.blocks - 1, block)

  var roots = flat.fullRoots(2 + 2 * block)
  var nodes = []
  var self = this

  loop(null, null)

  function loop (err, node) {
    if (err) return cb(err)
    if (node) nodes.push(node)
    if (!roots.length) return cb(null, hash.tree(nodes), block)
    self._core._nodes.get(self._prefix + roots.shift(), loop)
  }
}

Feed.prototype.has = function (block) {
  return this.bitfield.get(block)
}

Feed.prototype.get = function (block, opts, cb) { // TODO: on static feeds return null if > blocks
  if (typeof opts === 'function') return this.get(block, null, opts)
  if (!opts) opts = {}
  var self = this

  if (!this.opened) return this._openAndGet(block, opts, cb)

  if (!this.bitfield.get(block)) {
    if (opts.wait === false) {
      // just error instead of queueing for the download
      var error = new Error('Block not found')
      error.notFound = true
      cb(error)
    }
    return this._getNext(block, cb)
  }

  if (this.storage) this.storage.get(block, doneStorage)
  else this._core._data.get(this._prefix + block, done)

  function doneStorage (err, value) {
    if (err) return cb(err)
    if (!opts.verify) return done(null, value)
    // there are different storage schemes which may not keep historical data
    // if opts.verify is enabled, we need to double check that the data returned
    // is correct. if it's not, then we must update the local bitfield and
    // return an error.
    self._core._nodes.get(self._prefix + (block * 2), function (err, node) {
      if (err) return cb(err)
      var blockHash = hash.data(value)
      if (!equals(blockHash, node.hash)) {
        self.bitfield.set(block, false) // remove from the local bitfield
        self._sync()

        var error = new Error('Block not found')
        error.notFound = true
        cb(error)
      } else {
        done(null, value)
      }
    })
  }

  function done (err, value) {
    if (err) return cb(err)
    try {
      value = self.valueEncoding.decode(value)
    } catch (err) {
      return cb(err)
    }
    cb(null, value)
  }
}

Feed.prototype.verifyStorage = function (cb) {
  var self = this
  var numMissing = 0
  var index = 0
  var getOpts = { verify: true }

  loop()

  function loop (err) {
    if (err) {
      if (err.notFound) numMissing++
      else return cb(err)
    }

    while (index < self.blocks) {
      var block = index
      index++
      if (self.bitfield.get(block)) {
        return self.get(block, getOpts, loop)
      }
    }

    cb(null, { numMissing: numMissing })
  }
}

Feed.prototype._getNext = function (block, cb) {
  var self = this
  this.selection.add({priority: 2, block: block, bytes: 0}, done)
  this._updatePeers()

  function done (err) {
    if (err) return cb(err)
    self.get(block, cb)
  }
}

Feed.prototype.seek = function (bytes, opts, cb) { // TODO: on static feeds return null if > blocks
  if (!this.opened) return this._openAndSeek(bytes, opts, cb)
  if (typeof opts === 'function') return this.seek(bytes, null, opts)
  if (!opts) opts = {}

  var self = this
  this._seek(bytes, function (err, index, offset) {
    if (!err && isBlock(index)) return cb(null, index / 2, offset)
    self.selection.add({priority: 4, bytes: bytes, start: opts.start, end: opts.end}, cb)
    self._updatePeers()
  })
}

Feed.prototype.put = function (block, data, proof, cb) {
  if (!this.opened) return this._openAndPut(block, data, proof, cb)
  data = this.valueEncoding.encode(data)

  var self = this
  var offset = 0
  var batch = []
  var nodes = proof.nodes || []

  var top = {
    index: 2 * block,
    size: data.length,
    hash: hash.data(data)
  }

  if (!this.storage) {
    batch.push({
      type: 'put',
      key: '!data!' + this._prefix + block,
      value: data
    })
  }

  batch.push({
    type: 'put',
    key: '!nodes!' + this._prefix + top.index,
    value: messages.Node.encode(top)
  })

  loop(null, null)

  function loop (err, node) {
    if (err) return cb(err)

    if (node && node.index === top.index) {
      if (!equals(top.hash, node.hash)) {
        return cb(new Error('Checksum mismatch'))
      }
      self._db.batch(batch, writeStorage)
      return
    }

    var sibling = flat.sibling(top.index)
    var verified = self.tree.get(top.index)
    while (!verified && ((node && node.index === sibling) || (offset < nodes.length && nodes[offset].index === sibling))) {
      var next = node && node.index === sibling ? node : nodes[offset++]
      top.hash = hash.parent(top, next)
      top.size += next.size
      top.index = flat.parent(top.index)
      sibling = flat.sibling(top.index)
      verified = self.tree.get(top.index)

      batch.push({
        type: 'put',
        key: '!nodes!' + self._prefix + next.index,
        value: messages.Node.encode(next)
      })

      batch.push({
        type: 'put',
        key: '!nodes!' + self._prefix + top.index,
        value: messages.Node.encode(top)
      })
    }

    if (verified) return self._core._nodes.get(self._prefix + top.index, loop)
    if (self.tree.get(sibling)) return self._core._nodes.get(self._prefix + sibling, loop)

    self._putRoots(top, batch, proof.nodes, offset, proof.signature, writeStorage)
    offset = nodes.length // put roots returns error if this does hold anyways
  }

  function writeStorage (err) {
    if (err) return cb(err)
    if (!self.storage) return finalize(null)
    self.storage.put(block, data, finalize)
  }

  function finalize (err) {
    if (err) return cb(err)
    self.tree.set(2 * block)
    for (var i = 0; i < offset; i++) self.tree.set(nodes[i].index)
    if (self.bitfield.set(block, true)) self.emit('have', block, data)
    self._first = false
    self._sync(null, cb)
    self._drain(block)
  }
}

Feed.prototype._seek = function (offset, cb) {
  if (offset === 0) return cb(null, 0, 0)

  var self = this
  var nodes = this._core._nodes
  var roots = flat.fullRoots(this.blocks * 2)
  var nearestRoot = 0

  loop(null, null)

  function onroot (top) {
    if (isBlock(top)) return cb(null, top, offset)

    var left = flat.leftChild(top)
    while (!self.tree.get(left)) {
      if (isBlock(left)) return cb(null, nearestRoot, offset)
      left = flat.leftChild(left)
    }

    nodes.get(self._prefix + left, onleftchild)
  }

  function onleftchild (err, node) {
    if (err) return cb(err)

    if (node.size > offset) {
      nearestRoot = node.index
      onroot(node.index)
    } else {
      offset -= node.size
      onroot(flat.sibling(node.index))
    }
  }

  function loop (err, node) {
    if (err) return cb(err)

    if (node) {
      if (node.size > offset) {
        nearestRoot = node.index
        return onroot(node.index)
      }
      offset -= node.size
    }

    if (!roots.length) return cb(new Error('Out of bounds'))
    nodes.get(self._prefix + roots.shift(), loop)
  }
}

Feed.prototype._drain = function (block) {
  // TODO: this can be optimized a lot

  var i = 0
  var prioritized = this.selection.range(4)
  var bytes = []

  for (i = 0; i < prioritized.length; i++) {
    var req = prioritized[i]
    if (req.bytes) bytes.push(req)
  }

  this.selection.update(block)

  for (i = 0; i < bytes.length; i++) {
    this._reseek(bytes[i])
  }

  for (i = 0; i < this.peers.length; i++) {
    this.peers[i].have(block)
  }

  this._updateDownloaded()
}

Feed.prototype._reseek = function (message) {
  var self = this

  // this will only work as long as _seek is *strictly* async
  this._seek(message.bytes, function (err, index) {
    if (err || !isBlock(index)) return

    if (!self.selection.remove(message)) return
    self.seek(message.bytes, message.callback)
  })
}

Feed.prototype._putRoots = function (top, batch, nodes, nodesOffset, sig, cb) {
  var self = this
  var lastNode = nodes.length ? nodes[nodes.length - 1].index : top.index
  var verifiedBy = Math.max(flat.rightSpan(top.index), flat.rightSpan(lastNode)) + 2
  var indexes = flat.fullRoots(verifiedBy)
  var roots = new Array(indexes.length)
  var offset = 0

  loop(null, null)

  function loop (err, node) {
    if (err) return cb(err)
    if (node) roots[offset++] = node

    while (offset < roots.length) {
      var want = indexes[offset]
      if (want === top.index) {
        roots[offset++] = top
        continue
      }
      if (nodesOffset < nodes.length && want === nodes[nodesOffset].index) {
        roots[offset++] = nodes[nodesOffset++]
        continue
      }

      self._core._nodes.get(self._prefix + want, loop)
      return
    }

    var checksum = hash.tree(roots)

    if (sig) {
      if (!signatures.verify(checksum, sig, self.key)) {
        return cb(new Error('Signature does not verify'))
      }
      self.live = true
      batch.push({
        type: 'put',
        key: '!signatures!' + self._prefix + verifiedBy,
        value: sig
      })
    } else if (!equals(checksum, self.key)) {
      return cb(new Error('Checksum does not match key'))
    }

    for (var i = 0; i < roots.length; i++) {
      if (roots[i] === top) continue
      batch.push({
        type: 'put',
        key: '!nodes!' + self._prefix + roots[i].index,
        value: messages.Node.encode(roots[i])
      })
    }

    if (nodesOffset !== nodes.length) return cb(new Error('Cannot verify all nodes'))

    if (self._first) {
      batch.push({
        type: 'put',
        key: '!feeds!' + self.discoveryKey.toString('hex'),
        value: messages.Feed.encode(self)
      })
    }

    if (indexes.length) {
      var newBlocks = flat.rightSpan(indexes[indexes.length - 1]) / 2 + 1
      if (newBlocks > self.blocks) {
        self.blocks = newBlocks
        self.bytes = byteSize(roots)
        self.emit('update')
      }
    }

    self._db.batch(batch, cb)
  }
}

Feed.prototype.digest = function (block) {
  return this.tree.digest(2 * block)
}

Feed.prototype.proof = function (block, opts, cb) {
  if (typeof opts === 'function') return this.proof(block, null, opts)
  if (!this.opened) return this._openAndProof(block, cb)
  if (!opts) opts = {}

  var proof = this.tree.proof(2 * block, opts)
  if (!proof) return cb(new Error('No proof available'))

  var self = this
  var result = {nodes: [], signature: null}
  var i = 0

  loop(null, null)

  function onsign (err, sig) {
    if (err) return cb(err)
    result.signature = sig
    cb(null, result)
  }

  function loop (err, node) {
    if (err) return cb(err)
    if (node) result.nodes.push(node)

    if (i === proof.nodes.length) {
      if (!self.live || !proof.verifiedBy) return cb(null, result)
      return self._core._signatures.get(self._prefix + proof.verifiedBy, onsign)
    }

    self._core._nodes.get(self._prefix + proof.nodes[i++], loop)
  }
}

Feed.prototype.finalize = function (cb) {
  var self = this

  if (!cb) cb = noop
  if (!this.opened) return this.open(onopen)
  if (this.live) return this.flush(cb)

  this.flush(onflush)

  function onflush (err) {
    if (err) return cb(err)
    self.key = self._merkle.roots.length ? hash.tree(self._merkle.roots) : null
    self.discoveryKey = self.key && hash.discoveryKey(self.key)
    if (!self.key) return cb()
    self._sync(null, onsync)
  }

  function onsync (err) {
    if (err) return cb(err)
    self._core._feeds.put(self.discoveryKey.toString('hex'), self, cb)
  }

  function onopen (err) {
    if (err) return cb(err)
    self.finalize(cb)
  }
}

Feed.prototype.append = function (buffers, cb) {
  if (typeof buffers === 'string') buffers = [Buffer(buffers)]
  if (!Array.isArray(buffers)) buffers = [buffers]
  this._append(buffers, cb || noop)
}

Feed.prototype.close = function (cb) {
  if (!cb) cb = noop

  var self = this
  this.flush(onflush)

  function onflush (err) {
    if (err) return cb(err)
    if (self.storage) self.storage.close(onclose)
    else onclose()
  }

  function onclose (err) {
    if (err) return cb(err)
    self.emit('close')
    cb()
  }
}

Feed.prototype.blocksRemaining = function () {
  var remaining = 0
  for (var i = 0; i < this.blocks; i++) {
    if (!this.bitfield.get(i)) remaining++
  }
  return remaining
}

Feed.prototype._updatePeers = function () {
  for (var i = 0; i < this.peers.length; i++) this.peers[i].update()
}

Feed.prototype._updateDownloaded = function () {
  if (!this.has(this._downloaded)) return
  while (this.has(this._downloaded)) this._downloaded++
  if (this._downloaded >= this.blocks) this.emit('download-finished')
}

Feed.prototype._flush = function (cb) { // TODO: split up local bitfields as well
  var batch = [{
    type: 'put',
    key: this._prefix + 'blocks',
    value: this.bitfield.buffer
  }, {
    type: 'put',
    key: this._prefix + 'tree',
    value: this.tree.bitfield.buffer
  }]

  this._core._bitfields.batch(batch, cb)
}

Feed.prototype._open = function (cb) {
  var self = this
  var wroteFeed = false

  if (!this.discoveryKey) {
    this.prefix = this.options.prefix || randomBytes(32)
    this._merkle = merkle(hash)
    this._prefix = '!' + this.prefix.toString('hex') + '!'
    return process.nextTick(openStorage)
  }

  this._core._feeds.get(this.discoveryKey.toString('hex'), function (err, feed) {
    if (err) self._core._feeds.get(self.key.toString('hex'), onfeed)
    else onfeed(null, feed)
  })

  function onfeed (_, feed) {
    if (feed) {
      self.discoveryKey = feed.discoveryKey
      self.key = feed.key
      self.secretKey = feed.secretKey
      self.prefix = feed.prefix || feed.key
      self.live = feed.live
    } else {
      var owner = self.secretKey || !self.key // TODO: expose this
      if (owner) wroteFeed = true
    }

    if (!self.prefix) self.prefix = self.options.prefix || randomBytes(32)
    self._prefix = '!' + self.prefix.toString('hex') + '!'

    var missing = 2

    self._core._bitfields.get(self._prefix + 'tree', function (_, buffer) {
      if (buffer) self.tree = tree(buffer)
      if (!--missing) index()
    })

    self._core._bitfields.get(self._prefix + 'blocks', function (_, buffer) {
      if (buffer) self.bitfield = bitfield(buffer)
      if (!--missing) index()
    })
  }

  function index () {
    // TODO: move this to first append instead of on open for a bit faster open
    var indexes = self.tree.roots()
    var roots = []

    if (indexes.length) self.blocks = flat.rightSpan(indexes[indexes.length - 1]) / 2 + 1

    loop(null, null)

    function loop (err, node) {
      if (err) return done(err)
      if (node) roots.push(node)

      if (!indexes.length) {
        self.bytes = Math.max(self.bytes, byteSize(roots))
        self._merkle = merkle(hash, roots)
        if (wroteFeed) self._core._feeds.put(self.discoveryKey.toString('hex'), self, openStorage)
        else openStorage()
      } else {
        self._core._nodes.get(self._prefix + indexes.shift(), loop)
      }
    }
  }

  function openStorage (err) {
    if (err) return done(err)
    if (!self.storage) return done(null)
    self.storage.open(done)
  }

  function done (err) {
    if (err) return cb(err)
    if (wroteFeed) self._first = false
    self.opened = true
    self.emit('open')
    cb()
  }
}

Feed.prototype._append = function (buffers, cb) {
  if (!this.opened) return this._openAndAppend(buffers, cb)
  if (this.live && !this.secretKey) throw new Error('Only the owner can append to this feed')

  if (this._appendCallback) {
    if (!this._buffer) {
      this._buffer = buffers
      this._bufferCallbacks = []
    } else {
      for (var h = 0; h < buffers.length; h++) {
        this._buffer.push(buffers[h])
      }
    }
    this._bufferCallbacks.push(cb)
    return
  }

  this._appending = buffers.length
  this._missing = this.storage ? 2 : 1
  this._buffer = null
  this._bufferCallbacks = null
  this._appendCallback = cb

  var batch = []

  for (var i = 0; i < buffers.length; i++) {
    var buf = this.valueEncoding.encode(buffers[i])
    var nodes = this._merkle.next(buf)

    this._appendingBytes += buf.length

    for (var j = 0; j < nodes.length; j++) {
      var node = nodes[j]

      if (node.data) {
        if (!this.storage) {
          batch.push({
            type: 'put',
            key: '!data!' + this._prefix + (node.index / 2),
            value: node.data
          })
        }

        if (this.live) {
          var sig = signatures.sign(hash.tree(this._merkle.roots), this.secretKey)
          batch.push({
            type: 'put',
            key: '!signatures!' + this._prefix + (node.index + 2),
            value: sig
          })
        }
      }

      batch.push({
        type: 'put',
        key: '!nodes!' + this._prefix + node.index,
        value: messages.Node.encode(node)
      })
    }
  }

  this._db.batch(batch, this._afterAppend)
  if (this.storage) this.storage.append(this.blocks, buffers, this._afterAppend)
}

Feed.prototype.flush = function (cb) {
  if (!cb) cb = noop
  if (this.live && !this.secretKey) return cb() // is not owner
  this._append([], cb)
}

Feed.prototype._openAndAppend = function (buffers, cb) {
  var self = this
  this.open(function (err) {
    if (err) return cb(err)
    self._append(buffers, cb)
  })
}

Feed.prototype._openAndPut = function (block, data, proof, cb) {
  var self = this
  this.open(function (err) {
    if (err) return cb(err)
    self.put(block, data, proof, cb)
  })
}

Feed.prototype._openAndGet = function (block, opts, cb) {
  var self = this
  this.open(function (err) {
    if (err) return cb(err)
    self.get(block, opts, cb)
  })
}

Feed.prototype._openAndSeek = function (offset, opts, cb) {
  var self = this
  this.open(function (err) {
    if (err) return cb(err)
    self.seek(offset, opts, cb)
  })
}

Feed.prototype._openAndProof = function (block, cb) {
  var self = this
  this.open(function (err) {
    if (err) return cb(err)
    self.proof(block, cb)
  })
}

function noop () {}

function isStream (stream) {
  return !!stream && typeof stream.pipe === 'function'
}

function isBlock (i) {
  return (i & 1) === 0
}

function byteSize (nodes) {
  var sum = 0
  for (var i = 0; i < nodes.length; i++) sum += nodes[i].size
  return sum
}
