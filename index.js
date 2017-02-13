var equals = require('buffer-equals')
var low = require('last-one-wins')
var remove = require('unordered-array-remove')
var merkle = require('merkle-tree-stream/generator')
var flat = require('flat-tree')
var bulk = require('bulk-write-stream')
var signatures = require('sodium-signatures')
var from = require('from2')
var codecs = require('codecs')
var bitfield = require('sparse-bitfield')
var thunky = require('thunky')
var batcher = require('atomic-batcher')
var inherits = require('inherits')
var events = require('events')
var raf = require('random-access-file')
var treeIndex = require('./lib/tree-index')
var storage = require('./lib/storage')
var hash = require('./lib/hash')
var replicate = null

module.exports = Feed

function Feed (createStorage, key, opts) {
  if (!(this instanceof Feed)) return new Feed(createStorage, key, opts)
  events.EventEmitter.call(this)

  if (typeof createStorage === 'string') createStorage = defaultStorage(createStorage)
  if (typeof createStorage !== 'function') throw new Error('Storage should be a function or string')

  if (typeof key === 'string') key = new Buffer(key, 'hex')

  if (!Buffer.isBuffer(key)) {
    opts = key
    key = null
  }

  if (!opts) opts = {}

  var self = this

  this.id = opts.id || hash.randomBytes(32)
  this.live = opts.live !== false
  this.length = 0
  this.byteLength = 0
  this.key = key || null
  this.discoveryKey = this.key && hash.discoveryKey(this.key)
  this.secretKey = null
  this.tree = treeIndex(bitfield({trackUpdates: true, pageSize: 1024}))
  this.bitfield = bitfield({trackUpdates: true, pageSize: 1024})
  this.writable = false
  this.readable = true
  this.opened = false

  this._ready = thunky(open) // TODO: if open fails, do not reopen next time
  this._indexing = !!opts.indexing
  this._createIfMissing = opts.createIfMissing !== false
  this._overwrite = !!opts.overwrite
  this._merkle = null
  this._storage = storage(createStorage)
  this._batch = batcher(work)
  this._waiting = []

  // Switch to ndjson encoding if JSON is used. That way data files parse like ndjson \o/
  this._codec = codecs(opts.valueEncoding === 'json' ? 'ndjson' : opts.valueEncoding)
  this._sync = low(sync)

  // for replication
  this._selection = []
  this._peers = []

  // open it right away. TODO: do not reopen (i.e, set a flag not to retry)
  this._ready(onerror)

  function onerror (err) {
    if (err) self.emit('error')
  }

  function work (values, cb) {
    self._append(values, cb)
  }

  function sync (_, cb) {
    self._syncBitfield(cb)
  }

  function open (cb) {
    self._open(cb)
  }
}

inherits(Feed, events.EventEmitter)

Feed.prototype.replicate = function () {
  // Lazy load replication deps
  if (!replicate) replicate = require('./lib/replicate')
  return replicate(this)
}

Feed.prototype.ready = function (onready) {
  this._ready(function (err) {
    if (!err) onready()
  })
}

Feed.prototype._open = function (cb) {
  var self = this

  this._storage.open(onopen)

  function onopen (err, state) {
    if (err) return cb(err)

    // if no key but we have data do a bitfield reset since we cannot verify the data.
    if (!state.key && (state.treeBitfield.length || state.dataBitfield.length)) {
      self._overwrite = true
    }

    if (self._overwrite) {
      state.dataBitfield.fill(0)
      state.treeBitfield.fill(0)
      state.key = state.secretKey = null
    }

    self.bitfield.setBuffer(0, state.dataBitfield)
    self.tree.bitfield.setBuffer(0, state.treeBitfield)

    var len = bitfieldLength(state.treeBitfield)
    if (len) {
      // last node should be a factor of 2 (leaf node)
      // if not, last write wasn't flushed completely and we need to find the
      // last written leaf
      if ((len & 1) === 0) {
        len--
        while (len > 0 && !self.tree.bitfield.get(len - 1)) len -= 2
      }

      if (len > 0) self.length = (len + 1) / 2
    }

    if (state.key && self.key && !equals(state.key, self.key)) {
      return cb(new Error('Another hypercore is stored here'))
    }

    if (state.key) self.key = state.key
    if (state.secretKey) self.secretKey = state.secretKey

    if (self.length) self._storage.getNode(self.length * 2 - 2, onlastnode)
    else onlastnode(null, null)

    function onlastnode (err, node) {
      if (err) return cb(err)

      if (node) self.live = !!node.signature

      if (!self.key && !self._createIfMissing) {
        return cb(new Error('No hypercore is stored here'))
      }

      if (!self.key && self.live) {
        var keyPair = signatures.keyPair()
        self.secretKey = keyPair.secretKey
        self.key = keyPair.publicKey
      }

      self.writable = !!self.secretKey || self.key === null
      self.discoveryKey = self.key && hash.discoveryKey(self.key)

      var missing = 1 + (self.key ? 1 : 0) + (self.secretKey ? 1 : 0) + (self._overwrite ? 2 : 0)
      var error = null

      if (self.key) self._storage.key.write(0, self.key, done)
      if (self.secretKey) self._storage.secretKey.write(0, self.secretKey, done)

      if (self._overwrite) { // TODO: support storage.resize for this instead
        self._storage.treeBitfield.write(0, state.treeBitfield, done)
        self._storage.dataBitfield.write(0, state.dataBitfield, done)
      }

      done(null)

      function done (err) {
        if (err) error = err
        if (--missing) return
        if (error) return cb(error)
        self._roots(self.length, onroots)
      }

      function onroots (err, roots) {
        if (err) return cb(err)

        self._merkle = merkle(hash, roots)
        self.byteLength = roots.reduce(addSize, 0)
        self.opened = true
        self.emit('ready')

        cb(null)
      }
    }
  }
}

Feed.prototype.download = function (index, cb) {
  if (typeof index === 'function') return this.download(null, index)

  if (!index && typeof index !== 'number') {
    index = []
    while (index.length < this.length) index.push(index.length)
    require('shuffle-array')(index)
  }

  if (!Array.isArray(index)) index = [index]

  this._selection.push({
    blocks: index,
    ptr: 0,
    downloaded: 0,
    callback: cb
  })

  this._updatePeers()
}

Feed.prototype.undownload = function (index, cb) {
  for (var i = 0; i < this._selection.length; i++) {
    if (this._selection[i].index === index) {
      this._selection.splice(i, 1)
      return
    }
  }
}

Feed.prototype.proof = function (index, opts, cb) {
  if (typeof opts === 'function') return this.proof(index, null, opts)
  if (!this.opened) return this._readyAndProof(index, opts, cb)
  if (!opts) opts = {}

  var proof = this.tree.proof(2 * index, opts)
  var needsSig = this.live && !!proof.verifiedBy
  var sigIndex = needsSig ? proof.verifiedBy - 2 : 0
  var pending = proof.nodes.length + (needsSig ? 1 : 0)
  var error = null
  var signature = null
  var nodes = new Array(proof.nodes.length)

  if (!pending) return cb(null, {nodes: nodes, signature: null})

  for (var i = 0; i < proof.nodes.length; i++) {
    this._storage.getNode(proof.nodes[i], onnode)
  }

  if (needsSig) {
    this._storage.getNode(sigIndex, onnode)
  }

  function onnode (err, node) {
    if (err) error = err

    if (node) {
      if (needsSig && !signature && node.index === sigIndex) {
        signature = node.signature
      } else {
        nodes[proof.nodes.indexOf(node.index)] = node
      }
    }

    if (--pending) return
    if (error) return cb(error)

    cb(null, {nodes: nodes, signature: signature})
  }
}

Feed.prototype._readyAndProof = function (index, opts, cb) {
  var self = this
  this._ready(function (err) {
    if (err) return cb(err)
    self.proof(index, opts, cb)
  })
}

Feed.prototype.put = function (index, data, proof, cb) {
  if (!this.opened) return this._readyAndPut(index, data, proof, cb)
  this._putBuffer(index, this._codec.encode(data), proof, null, cb)
}

Feed.prototype.seek = function (bytes, cb) {
  if (!this.opened) return this._readyAndSeek(bytes, cb)

  // var self = this
  this._seek(bytes, function (err, index, offset) {
    if (!err && isBlock(index)) return cb(null, index / 2, offset)
    cb(err || new Error('not seekable: but in tree: ' + index))
  })
}

Feed.prototype._seek = function (offset, cb) {
  if (offset === 0) return cb(null, 0, 0)

  var self = this
  var roots = flat.fullRoots(this.length * 2)
  var nearestRoot = 0

  loop(null, null)

  function onroot (top) {
    if (isBlock(top)) return cb(null, top, offset)

    var left = flat.leftChild(top)
    while (!self.tree.get(left)) {
      if (isBlock(left)) return cb(null, nearestRoot, offset)
      left = flat.leftChild(left)
    }

    self._storage.getNode(left, onleftchild)
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
    self._storage.getNode(roots.shift(), loop)
  }
}

Feed.prototype._readyAndSeek = function (bytes, cb) {
  var self = this
  this._ready(function (err) {
    if (err) return cb(err)
    self.seek(bytes, cb)
  })
}

Feed.prototype._putBuffer = function (index, data, proof, from, cb) {
  var self = this
  var trusted = -1
  var missing = []
  var next = 2 * index
  var i = 0

  for (i = 0; i < proof.nodes.length; i++) {
    if (this.tree.get(next)) {
      trusted = next
      break
    }

    var sib = flat.sibling(next)
    next = flat.parent(next)

    if (proof.nodes[i].index === sib) continue
    if (!this.tree.get(sib)) break

    missing.push(sib)
  }

  if (trusted === -1 && this.tree.get(next)) trusted = next

  var error = null
  var trustedNode = null
  var missingNodes = new Array(missing.length)
  var pending = missing.length + (trusted > -1 ? 1 : 0)

  for (i = 0; i < missing.length; i++) this._storage.getNode(missing[i], onmissing)
  if (trusted > -1) this._storage.getNode(trusted, ontrusted)
  if (!missing.length && trusted === -1) onmissingloaded(null)

  function ontrusted (err, node) {
    if (err) error = err
    if (node) trustedNode = node
    if (!--pending) onmissingloaded(error)
  }

  function onmissing (err, node) {
    if (err) error = err
    if (node) missingNodes[missing.indexOf(node.index)] = node
    if (!--pending) onmissingloaded(error)
  }

  function onmissingloaded (err) {
    if (err) return cb(err)
    var writes = self._verify(index, data, proof, missingNodes, trustedNode)
    if (!writes) return cb(new Error('Could not verify data'))
    self._commit(index, data, writes, from, cb)
  }
}

Feed.prototype._readyAndPut = function (index, data, proof, cb) {
  var self = this
  this._ready(function (err) {
    if (err) return cb(err)
    self.put(index, data, proof, cb)
  })
}

Feed.prototype._commit = function (index, data, nodes, from, cb) {
  var self = this
  var pending = nodes.length + 1
  var error = null

  for (var i = 0; i < nodes.length; i++) this._storage.putNode(nodes[i].index, nodes[i], ondone)
  this._storage.putData(index, data, nodes, ondone)

  function ondone (err) {
    if (err) error = err
    if (--pending) return
    if (error) return cb(error)
    self._commitDone(index, data, nodes, from, cb)
  }
}

Feed.prototype._commitDone = function (index, data, nodes, from, cb) {
  for (var i = 0; i < nodes.length; i++) this.tree.set(nodes[i].index)
  this.tree.set(2 * index)

  if (this.bitfield.set(index, true)) this.emit('download', index, data, nodes)
  if (this._peers.length && this._peers[0] !== from) this._announce({start: index}, from)

  this._sync(null, cb)
}

Feed.prototype._verifyRoots = function (top, proof, batch) {
  var lastNode = proof.nodes.length ? proof.nodes[proof.nodes.length - 1].index : top.index
  var verifiedBy = Math.max(flat.rightSpan(top.index), flat.rightSpan(lastNode)) + 2
  var indexes = flat.fullRoots(verifiedBy)
  var roots = new Array(indexes.length)

  for (var i = 0; i < roots.length; i++) {
    if (indexes[i] === top.index) {
      roots[i] = top
      batch.push(top)
    } else if (proof.nodes.length && indexes[i] === proof.nodes[0].index) {
      roots[i] = proof.nodes.shift()
      batch.push(roots[i])
    } else {
      return null
    }
  }

  var checksum = hash.tree(roots)

  if (proof.signature) {
    // check signature
    if (!signatures.verify(checksum, proof.signature, this.key)) return null
    this.live = true
  } else {
    // check tree root
    if (!equals(checksum, this.key)) return null
    this.live = false
  }

  var length = verifiedBy / 2
  if (length > this.length) {
    this.length = length
    this.byteLength = roots.reduce(addSize, 0)
    this.emit('append')
  }

  return batch
}

Feed.prototype._verify = function (index, data, proof, missing, trusted) {
  var top = new storage.Node(2 * index, hash.data(data), data.length, null)
  var writes = []

  if (verifyNode(trusted, top)) return writes

  while (true) {
    var node = null
    var next = flat.sibling(top.index)

    if (proof.nodes.length && proof.nodes[0].index === next) {
      node = proof.nodes.shift()
      writes.push(node)
    } else if (missing.length && missing[0].index === next) {
      node = missing.shift()
    } else { // all remaining nodes should be roots now
      return this._verifyRoots(top, proof, writes)
    }

    writes.push(top)
    top = new storage.Node(flat.parent(top.index), hash.parent(top, node), top.size + node.size, null)

    if (verifyNode(trusted, top)) return writes
  }
}

Feed.prototype._announce = function (message, from) {
  for (var i = 0; i < this._peers.length; i++) {
    var peer = this._peers[i]
    if (peer !== from) peer.have(message)
  }
}

Feed.prototype.has = function (index) {
  return this.bitfield.get(index)
}

Feed.prototype.get = function (index, opts, cb) {
  if (typeof opts === 'function') return this.get(index, null, opts)
  if (!this.opened) return this._readyAndGet(index, opts, cb)

  if (opts && opts.timeout) cb = timeoutCallback(cb, opts.timeout)

  if (!this.has(index)) {
    if (opts && opts.wait === false) return cb(new Error('Block not downloaded'))

    this._waiting.push({index: index, callback: cb})
    this._updatePeers()
    return
  }

  if (this._codec !== codecs.binary) cb = this._wrapCodec(cb)
  this._storage.getData(index, cb)
}

Feed.prototype._readyAndGet = function (index, opts, cb) {
  var self = this
  this._ready(function (err) {
    if (err) return cb(err)
    self.get(index, opts, cb)
  })
}

Feed.prototype._updatePeers = function () {
  for (var i = 0; i < this._peers.length; i++) this._peers[i].update()
}

Feed.prototype._wrapCodec = function (cb) {
  var self = this
  return function (err, buf) {
    if (err) return cb(err)
    cb(null, self._codec.decode(buf))
  }
}

Feed.prototype.createWriteStream = function () {
  var self = this
  return bulk.obj(write)

  function write (batch, cb) {
    self._batch(batch, cb)
  }
}

Feed.prototype.createReadStream = function (opts) {
  if (!opts) opts = {}

  var self = this
  var start = opts.start || 0
  var end = typeof opts.end === 'number' ? opts.end : -1
  var live = !!opts.live
  var first = true

  return from.obj(read)

  function read (size, cb) {
    if (!self.opened) return open(size, cb)

    if (first) {
      if (end === -1) end = live ? Infinity : self.length
      if (opts.tail) start = self.length
      first = false
    }

    if (start === end) return cb(null, null)
    self.get(start++, opts, cb)
  }

  function open (size, cb) {
    self._ready(function (err) {
      if (err) return cb(err)
      read(size, cb)
    })
  }
}

// TODO: when calling finalize on a live feed write an END_OF_FEED block (length === 0?)
Feed.prototype.finalize = function (cb) {
  if (!this.key) this.key = hash.tree(this._merkle.roots)
  this._storage.key.write(0, this.key, cb)
}

Feed.prototype.append = function (batch, cb) {
  this._batch(Array.isArray(batch) ? batch : [batch], cb || noop)
}

Feed.prototype.flush = function (cb) {
  this._batch([], cb)
}

Feed.prototype.close = function (cb) {
  var self = this

  this._ready(function () {
    self.writable = false
    self.readable = false
    self._storage.close(cb)
  })
}

Feed.prototype._append = function (batch, cb) {
  if (!this.opened) return this._readyAndAppend(batch, cb)
  if (!this.writable) return cb(new Error('This feed is not writable (Did you create it?)'))

  var self = this
  var pending = batch.length
  var offset = 0
  var error = null

  if (!pending) return cb()

  for (var i = 0; i < batch.length; i++) {
    var data = this._codec.encode(batch[i])
    var nodes = this._merkle.next(data)

    pending += nodes.length

    if (this._indexing) done(null)
    else this._storage.data.write(this.byteLength + offset, data, done)

    offset += data.length

    for (var j = 0; j < nodes.length; j++) {
      var node = nodes[j]
      // TODO: this might deopt? pass in constructor to the merklelizer
      if (this.live) node.signature = signatures.sign(hash.tree(this._merkle.roots), this.secretKey)
      this._storage.putNode(node.index, node, done)
    }
  }

  function done (err) {
    if (err) error = err
    if (--pending) return
    if (error) return cb(error)

    var start = self.length

    self.byteLength += offset
    for (var i = 0; i < batch.length; i++) {
      self.bitfield.set(self.length, true)
      self.tree.set(2 * self.length++)
    }
    self.emit('append')

    var message = self.length - start > 1 ? {start: start, end: self.length} : {start: start}
    if (self._peers.length) self._announce(message)

    self._sync(null, cb)
  }
}

Feed.prototype._readyAndAppend = function (batch, cb) {
  var self = this
  this._ready(function (err) {
    if (err) return cb(err)
    self._append(batch, cb)
  })
}

Feed.prototype._pollWaiting = function () {
  for (var i = 0; i < this._waiting.length; i++) {
    var next = this._waiting[i]
    if (!this.has(next.index)) continue
    remove(this._waiting, i--)
    this.get(next.index, next.callback)
  }
}

Feed.prototype._syncBitfield = function (cb) {
  var missing = this.bitfield.updates.length + this.tree.bitfield.updates.length
  var next = null
  var error = null

  // All data / nodes have been written now. We still need to update the bitfields though

  // TODO 1: if the program fails during this write the bitfield might not have been fully written
  // HOWEVER, we can easily recover from this by traversing the tree and checking if the nodes exists
  // on disk. So if a get fails, it should try and recover once.

  // TODO 2: if .writable append bitfield updates into a single buffer for extra perf
  // Added benefit is that if the program exits while flushing the bitfield the feed will only get
  // truncated and not have missing chunks which is what you expect.

  while ((next = this.bitfield.nextUpdate()) !== null) {
    this._storage.dataBitfield.write(next.offset, next.buffer, ondone)
  }

  while ((next = this.tree.bitfield.nextUpdate()) !== null) {
    this._storage.treeBitfield.write(next.offset, next.buffer, ondone)
  }

  this._pollWaiting()

  function ondone (err) {
    if (err) error = err
    if (--missing) return
    cb(error)
  }
}

Feed.prototype._roots = function (index, cb) {
  var roots = flat.fullRoots(2 * index)
  var result = new Array(roots.length)
  var pending = roots.length
  var error = null

  if (!pending) return cb(null, result)

  for (var i = 0; i < roots.length; i++) {
    this._storage.getNode(roots[i], onnode)
  }

  function onnode (err, node) {
    if (err) error = err
    if (node) result[roots.indexOf(node.index)] = node
    if (--pending) return
    if (error) return cb(error)
    cb(null, result)
  }
}

function noop () {}

function verifyNode (trusted, node) {
  return trusted && trusted.index === node.index && equals(trusted.hash, node.hash)
}

function addSize (size, node) {
  return size + node.size
}

function bitfieldLength (buf) {
  if (!buf.length) return 0

  var max = buf.length - 1
  while (max && !buf[max]) max--

  var b = buf[max]
  if (!b) return max * 8

  var length = (max + 1) * 8

  while (true) {
    if (b & 1) return length
    b >>= 1
    length--
  }
}

function isBlock (index) {
  return (index & 1) === 0
}

function defaultStorage (dir) {
  return function (name) {
    return raf(name, {directory: dir})
  }
}

function timeoutCallback (cb, timeout) {
  var failed = false
  var id = setTimeout(ontimeout, timeout)
  return done

  function ontimeout () {
    failed = true
    // TODO: make libs/errors for all this stuff
    var err = new Error('ETIMEDOUT')
    err.code = 'ETIMEDOUT'
    cb(err)
  }

  function done (err, val) {
    if (failed) return
    clearTimeout(id)
    cb(err, val)
  }
}
