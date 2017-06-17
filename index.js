var equals = require('buffer-equals')
var low = require('last-one-wins')
var remove = require('unordered-array-remove')
var set = require('unordered-set')
var merkle = require('merkle-tree-stream/generator')
var flat = require('flat-tree')
var bulk = require('bulk-write-stream')
var signatures = require('sodium-signatures')
var from = require('from2')
var codecs = require('codecs')
var thunky = require('thunky')
var batcher = require('atomic-batcher')
var inherits = require('inherits')
var events = require('events')
var raf = require('random-access-file')
var bitfield = require('./lib/bitfield')
var sparseBitfield = require('sparse-bitfield')
var treeIndex = require('./lib/tree-index')
var storage = require('./lib/storage')
var hash = require('./lib/hash')
var nextTick = require('process-nextick-args')
var replicate = null

module.exports = Feed

function Feed (createStorage, key, opts) {
  if (!(this instanceof Feed)) return new Feed(createStorage, key, opts)
  events.EventEmitter.call(this)

  if (typeof createStorage === 'string') createStorage = defaultStorage(createStorage)
  if (typeof createStorage !== 'function') throw new Error('Storage should be a function or string')

  if (typeof key === 'string') key = new Buffer(key, 'hex')

  if (!Buffer.isBuffer(key) && !opts) {
    opts = key
    key = null
  }

  if (!opts) opts = {}

  var self = this

  var secretKey = opts.secretKey || null
  if (typeof secretKey === 'string') secretKey = new Buffer(secretKey, 'hex')

  this.id = opts.id || hash.randomBytes(32)
  this.live = opts.live !== false
  this.sparse = !!opts.sparse
  this.length = 0
  this.byteLength = 0
  this.maxRequests = opts.maxRequests || 16
  this.key = key || null
  this.discoveryKey = this.key && hash.discoveryKey(this.key)
  this.secretKey = secretKey
  this.bitfield = null
  this.tree = null
  this.writable = !!opts.writable
  this.readable = true
  this.opened = false
  this.allowPush = !!opts.allowPush
  this.peers = []

  this._ready = thunky(open) // TODO: if open fails, do not reopen next time
  this._indexing = !!opts.indexing
  this._createIfMissing = opts.createIfMissing !== false
  this._overwrite = !!opts.overwrite
  this._storeSecretKey = opts.storeSecretKey !== false
  this._merkle = null
  this._storage = storage(createStorage)
  this._batch = batcher(work)

  this._waiting = []
  this._selections = []
  this._reserved = sparseBitfield()
  this._synced = null

  this._codec = toCodec(opts.valueEncoding)
  this._sync = low(sync)
  if (!this.sparse) this.download({start: 0, end: -1})

  // open it right away. TODO: do not reopen (i.e, set a flag not to retry)
  this._ready(onerror)

  function onerror (err) {
    if (err) self.emit('error', err)
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

Feed.discoveryKey = hash.discoveryKey

Feed.prototype.replicate = function (opts) {
  // Lazy load replication deps
  if (!replicate) replicate = require('./lib/replicate')

  if ((!this._selections.length || this._selections[0].end !== -1) && !this.sparse && !(opts && opts.live)) {
    // hack!! proper fix is to refactor ./replicate to *not* clear our non-sparse selection
    this.download({start: 0, end: -1})
  }

  return replicate(this, opts || {})
}

Feed.prototype.ready = function (onready) {
  this._ready(function (err) {
    if (!err) onready()
  })
}

Feed.prototype.update = function (len, cb) {
  if (typeof len === 'function') return this.update(-1, len)
  if (typeof len !== 'number') len = -1
  if (!cb) cb = noop

  var self = this

  this.ready(function (err) {
    if (err) return cb(err)
    if (len === -1) len = self.length + 1
    if (self.length >= len) return cb(null)

    self._waiting.push({
      hash: true,
      bytes: 0,
      index: len - 1,
      update: true,
      callback: cb
    })
  })
}

Feed.prototype._open = function (cb) {
  var self = this
  var generatedKey = false

  // TODO: clean up the duplicate code below ...

  this._storage.openKey(function (_, key) {
    if (key && !self._overwrite && !self.key) self.key = key

    if (!self.key && self.live) {
      var keyPair = signatures.keyPair()
      self.secretKey = keyPair.secretKey
      self.key = keyPair.publicKey
      generatedKey = true
    }

    self.discoveryKey = self.key && hash.discoveryKey(self.key)
    self._storage.open({key: self.key, discoveryKey: self.discoveryKey}, onopen)
  })

  function onopen (err, state) {
    if (err) return cb(err)

    // if no key but we have data do a bitfield reset since we cannot verify the data.
    if (!state.key && state.bitfield.length) {
      self._overwrite = true
    }

    if (self._overwrite) {
      state.bitfield.fill(0)
      state.key = state.secretKey = null
    }

    self.bitfield = bitfield(state.bitfield)
    self.tree = treeIndex(self.bitfield.tree)
    self.length = self.tree.blocks()
    this._synced = null

    if (state.key && self.key && !equals(state.key, self.key)) {
      return cb(new Error('Another hypercore is stored here'))
    }

    if (state.key) self.key = state.key
    if (state.secretKey) self.secretKey = state.secretKey

    if (!self.length) return onsignature(null, null)
    self._storage.getSignature(self.length - 1, onsignature)

    function onsignature (_, sig) {
      if (self.length) self.live = !!sig

      if ((generatedKey || !self.key) && !self._createIfMissing) {
        return cb(new Error('No hypercore is stored here'))
      }

      if (!self.key && self.live) {
        var keyPair = signatures.keyPair()
        self.secretKey = keyPair.secretKey
        self.key = keyPair.publicKey
      }

      var writable = !!self.secretKey || self.key === null

      if (!writable && self.writable) return cb(new Error('Feed is not writable'))
      self.writable = writable
      self.discoveryKey = self.key && hash.discoveryKey(self.key)

      if (self._storeSecretKey && !self.secretKey) {
        self._storeSecretKey = false
      }

      var missing = 1 + (self.key ? 1 : 0) + (self._storeSecretKey ? 1 : 0) + (self._overwrite ? 1 : 0)
      var error = null

      if (self.key) self._storage.key.write(0, self.key, done)
      if (self._storeSecretKey) self._storage.secretKey.write(0, self.secretKey, done)

      if (self._overwrite) { // TODO: support storage.resize for this instead
        self._storage.putBitfield(0, state.bitfield, done)
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

Feed.prototype.download = function (range, cb) {
  if (typeof range === 'function') return this.download(null, range)
  if (typeof range === 'number') range = {start: range, end: range + 1}
  if (!range) range = {}

  var sel = {
    _index: this._selections.length,
    hash: !!range.hash,
    iterator: null,
    start: range.start || 0,
    end: range.end || -1,
    linear: !!range.linear,
    callback: cb || noop
  }

  this._selections.push(sel)
  this._updatePeers()

  return sel
}

Feed.prototype.undownload = function (range) {
  if (typeof range === 'number') range = {start: range, end: range + 1}
  if (!range) range = {}

  if (range.callback && range._index > -1) {
    set.remove(this._selections, range)
    nextTick(range.callback, new Error('Download was cancelled'))
    return
  }

  var start = range.start || 0
  var end = range.end || -1
  var hash = !!range.hash
  var linear = !!range.linear

  for (var i = 0; i < this._selections.length; i++) {
    var s = this._selections[i]

    if (s.start === start && s.end === end && s.hash === hash && s.linear === linear) {
      set.remove(this._selections, s)
      return
    }
  }
}

Feed.prototype.digest = function (index) {
  return this.tree.digest(2 * index)
}

Feed.prototype.proof = function (index, opts, cb) {
  if (typeof opts === 'function') return this.proof(index, null, opts)
  if (!this.opened) return this._readyAndProof(index, opts, cb)
  if (!opts) opts = {}

  var proof = this.tree.proof(2 * index, opts)
  if (!proof) return cb(new Error('No proof available for this index'))

  var needsSig = this.live && !!proof.verifiedBy
  var pending = proof.nodes.length + (needsSig ? 1 : 0)
  var error = null
  var signature = null
  var nodes = new Array(proof.nodes.length)

  if (!pending) return cb(null, {nodes: nodes, signature: null})

  for (var i = 0; i < proof.nodes.length; i++) {
    this._storage.getNode(proof.nodes[i], onnode)
  }
  if (needsSig) {
    this._storage.getSignature(proof.verifiedBy / 2 - 1, onsignature)
  }

  function onsignature (err, sig) {
    if (sig) signature = sig
    onnode(err, null)
  }

  function onnode (err, node) {
    if (err) error = err

    if (node) {
      nodes[proof.nodes.indexOf(node.index)] = node
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

Feed.prototype.cancel = function (start, end) {  // TODO: use same argument scheme as download
  if (!end) end = start + 1

  // cancel these right away as .download does not wait for ready
  for (var i = this._selections.length - 1; i >= 0; i--) {
    var sel = this._selections[i]
    if (start <= sel.start && sel.end <= end) {
      this.undownload(sel)
    }
  }

  // defer the last part until after ready as .get does that as well
  if (this.opened) this._cancel(start, end)
  else this._readyAndCancel(start, end)
}

Feed.prototype._cancel = function (start, end) {
  var i = 0

  for (i = start; i < end; i++) {
    this._reserved.set(i, false) // TODO: send cancel message if set returns true
  }

  for (i = this._waiting.length - 1; i >= 0; i--) {
    var w = this._waiting[i]
    if ((start <= w.start && w.end <= end) || (start <= w.index && w.index < end)) {
      remove(this._waiting, i)
      if (w.callback) nextTick(w.callback, new Error('Request cancelled'))
    }
  }
}

Feed.prototype.clear = function (start, end, opts, cb) { // TODO: use same argument scheme as download
  if (typeof end === 'function') return this.clear(start, start + 1, null, end)
  if (typeof opts === 'function') return this.clear(start, end, null, opts)
  if (!opts) opts = {}
  if (!end) end = start + 1
  if (!cb) cb = noop

  // TODO: this needs some work. fx we can only calc byte offset for blocks we know about
  // so internally we should make sure to only do that. We should use the merkle tree for this

  var self = this
  var byteOffset = start === 0 ? 0 : (typeof opts.byteOffset === 'number' ? opts.byteOffset : -1)
  var byteLength = typeof opts.byteLength === 'number' ? opts.byteLength : -1

  this._ready(function (err) {
    if (err) return cb(err)

    var modified = false

    // TODO: use a buffer.fill thing here to speed this up!

    for (var i = start; i < end; i++) {
      if (self.bitfield.set(i, false)) modified = true
    }

    if (!modified) return nextTick(cb)

    // TODO: write to a tmp/update file that we want to del this incase it crashes will del'ing

    self._unannounce({start: start, length: end - start})
    if (opts.delete === false || self._indexing) return sync()
    if (byteOffset > -1) return onstartbytes(null, byteOffset)
    self._storage.dataOffset(start, [], onstartbytes)

    function sync () {
      self.emit('clear', start, end)
      self._sync(null, cb)
    }

    function onstartbytes (err, offset) {
      if (err) return cb(err)
      byteOffset = offset
      if (byteLength > -1) return onendbytes(null, byteLength + byteOffset)
      if (end === self.length) return onendbytes(null, self.byteLength)
      self._storage.dataOffset(end, [], onendbytes)
    }

    function onendbytes (err, end) {
      if (err) return cb(err)
      if (!self._storage.data.del) return sync() // Not all data storage impls del
      self._storage.data.del(byteOffset, end - byteOffset, sync)
    }
  })
}

Feed.prototype.seek = function (bytes, opts, cb) {
  if (typeof opts === 'function') return this.seek(bytes, null, opts)
  if (!opts) opts = {}
  if (!this.opened) return this._readyAndSeek(bytes, opts, cb)

  var self = this

  this._seek(bytes, function (err, index, offset) {
    if (!err && isBlock(index)) return done(index / 2, offset)
    if (opts.wait === false) return cb(err || new Error('Unable to seek to this offset'))

    var start = opts.start || 0
    var end = opts.end || -1

    if (!err) {
      var left = flat.leftSpan(index) / 2
      var right = flat.rightSpan(index) / 2 + 1

      if (left > start) start = left
      if (right < end || end === -1) end = right
    }

    if (end > -1 && end <= start) return cb(new Error('Unable to seek to this offset'))

    self._waiting.push({
      hash: opts.hash !== false,
      bytes: bytes,
      index: -1,
      start: start,
      end: end,
      callback: cb || noop
    })

    self._updatePeers()
  })

  function done (index, offset) {
    for (var i = 0; i < self.peers.length; i++) {
      self.peers[i].haveBytes(bytes)
    }
    cb(null, index, offset)
  }
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

Feed.prototype._readyAndSeek = function (bytes, opts, cb) {
  var self = this
  this._ready(function (err) {
    if (err) return cb(err)
    self.seek(bytes, opts, cb)
  })
}

Feed.prototype._getBuffer = function (index, cb) {
  this._storage.getData(index, cb)
}

Feed.prototype._putBuffer = function (index, data, proof, from, cb) {
  // TODO: this nodes in proof are not instances of our Node prototype
  // but just similar. Check if this has any v8 perf implications.

  // TODO: if the proof contains a valid signature BUT fails, emit a critical error
  // --> feed should be considered dead

  var self = this
  var trusted = -1
  var missing = []
  var next = 2 * index
  var i = data ? 0 : 1

  while (true) {
    if (this.tree.get(next)) {
      trusted = next
      break
    }

    var sib = flat.sibling(next)
    next = flat.parent(next)

    if (i < proof.nodes.length && proof.nodes[i].index === sib) {
      i++
      continue
    }

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
    self._verifyAndWrite(index, data, proof, missingNodes, trustedNode, from, cb)
  }
}

Feed.prototype._readyAndPut = function (index, data, proof, cb) {
  var self = this
  this._ready(function (err) {
    if (err) return cb(err)
    self.put(index, data, proof, cb)
  })
}

Feed.prototype._write = function (index, data, nodes, sig, from, cb) {
  var self = this
  var pending = nodes.length + 1 + (sig ? 1 : 0)
  var error = null

  for (var i = 0; i < nodes.length; i++) this._storage.putNode(nodes[i].index, nodes[i], ondone)
  if (data) this._storage.putData(index, data, nodes, ondone)
  else ondone()
  if (sig) this._storage.putSignature(sig.index, sig.signature, ondone)

  function ondone (err) {
    if (err) error = err
    if (--pending) return
    if (error) return cb(error)
    self._writeDone(index, data, nodes, from, cb)
  }
}

Feed.prototype._writeDone = function (index, data, nodes, from, cb) {
  for (var i = 0; i < nodes.length; i++) this.tree.set(nodes[i].index)
  this.tree.set(2 * index)

  if (data) {
    if (this.bitfield.set(index, true)) this.emit('download', index, data, from)
    if (this.peers.length) this._announce({start: index}, from)

    if (!this.writable) {
      if (!this._synced) this._synced = this.bitfield.iterator(0, this.length)
      if (this._synced.next() === -1) {
        this._synced.range(0, this.length)
        this._synced.seek(0)
        if (this._synced.next() === -1) {
          this.emit('sync')
        }
      }
    }
  }

  this._sync(null, cb)
}

Feed.prototype._verifyAndWrite = function (index, data, proof, localNodes, trustedNode, from, cb) {
  var visited = []
  var remoteNodes = proof.nodes
  var top = data ? new storage.Node(2 * index, hash.data(data), data.length) : remoteNodes.shift()

  // check if we already have the hash for this node
  if (verifyNode(trustedNode, top)) {
    this._write(index, data, visited, null, from, cb)
    return
  }

  // keep hashing with siblings until we reach or trusted node
  while (true) {
    var node = null
    var next = flat.sibling(top.index)

    if (remoteNodes.length && remoteNodes[0].index === next) {
      node = remoteNodes.shift()
      visited.push(node)
    } else if (localNodes.length && localNodes[0].index === next) {
      node = localNodes.shift()
    } else {
      // we cannot create another parent, i.e. these nodes must be roots in the tree
      this._verifyRootsAndWrite(index, data, top, proof, visited, from, cb)
      return
    }

    visited.push(top)
    top = new storage.Node(flat.parent(top.index), hash.parent(top, node), top.size + node.size)

    // the tree checks out, write the data and the visited nodes
    if (verifyNode(trustedNode, top)) {
      this._write(index, data, visited, null, from, cb)
      return
    }
  }
}

Feed.prototype._verifyRootsAndWrite = function (index, data, top, proof, nodes, from, cb) {
  var remoteNodes = proof.nodes
  var lastNode = remoteNodes.length ? remoteNodes[remoteNodes.length - 1].index : top.index
  var verifiedBy = Math.max(flat.rightSpan(top.index), flat.rightSpan(lastNode)) + 2
  var indexes = flat.fullRoots(verifiedBy)
  var roots = new Array(indexes.length)
  var error = null
  var pending = roots.length
  var self = this

  for (var i = 0; i < indexes.length; i++) {
    if (indexes[i] === top.index) {
      nodes.push(top)
      onnode(null, top)
    } else if (remoteNodes.length && indexes[i] === remoteNodes[0].index) {
      nodes.push(remoteNodes[0])
      onnode(null, remoteNodes.shift())
    } else if (this.tree.get(indexes[i])) {
      this._storage.getNode(indexes[i], onnode)
    } else {
      onnode(new Error('Remote did not send tree roots'))
    }
  }

  function onnode (err, node) {
    if (err) error = err
    if (node) roots[indexes.indexOf(node.index)] = node
    if (!--pending) verify(error)
  }

  function verify (err) {
    if (err) return cb(err)

    var checksum = hash.tree(roots)
    var signature = null

    if (self.length && self.live && !proof.signature) {
      return cb(new Error('Remote did not include a signature'))
    }

    if (proof.signature) { // check signaturex
      if (!signatures.verify(checksum, proof.signature, self.key)) {
        return cb(new Error('Remote signature could not be verified'))
      }

      signature = {index: verifiedBy / 2 - 1, signature: proof.signature}
    } else { // check tree root
      if (!equals(checksum, self.key)) {
        return cb(new Error('Remote checksum failed'))
      }
    }

    self.live = !!signature

    var length = verifiedBy / 2
    if (length > self.length) {
      // TODO: only emit this after the info has been flushed to storage
      self.length = length
      self.byteLength = roots.reduce(addSize, 0)
      if (self._synced) self._synced.seek(0, self.length)
      self.emit('append')
    }

    self._write(index, data, nodes, signature, from, cb)
  }
}

Feed.prototype._announce = function (message, from) {
  for (var i = 0; i < this.peers.length; i++) {
    var peer = this.peers[i]
    if (peer !== from) peer.have(message)
  }
}

Feed.prototype._unannounce = function (message) {
  for (var i = 0; i < this.peers.length; i++) this.peers[i].unhave(message)
}

Feed.prototype.downloaded = function (start, end) {
  return this.bitfield.total(start, end)
}

Feed.prototype.has = function (start, end) {
  if (end === undefined) return this.bitfield.get(start)
  var total = end - start
  return total === this.bitfield.total(start, end)
}

Feed.prototype.get = function (index, opts, cb) {
  if (typeof opts === 'function') return this.get(index, null, opts)
  if (!this.opened) return this._readyAndGet(index, opts, cb)

  if (opts && opts.timeout) cb = timeoutCallback(cb, opts.timeout)

  if (!this.bitfield.get(index)) {
    if (opts && opts.wait === false) return cb(new Error('Block not downloaded'))

    this._waiting.push({bytes: 0, hash: false, index: index, options: opts, callback: cb})
    this._updatePeers()
    return
  }

  if (opts && opts.valueEncoding) cb = wrapCodec(toCodec(opts.valueEncoding), cb)
  else if (this._codec !== codecs.binary) cb = wrapCodec(this._codec, cb)

  this._getBuffer(index, cb)
}

Feed.prototype._readyAndGet = function (index, opts, cb) {
  var self = this
  this._ready(function (err) {
    if (err) return cb(err)
    self.get(index, opts, cb)
  })
}

Feed.prototype._updatePeers = function () {
  for (var i = 0; i < this.peers.length; i++) this.peers[i].update()
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
  var snapshot = opts.snapshot !== false
  var first = true
  var range = this.download({start: start, end: end, linear: true})

  return from.obj(read).on('end', cleanup).on('close', cleanup)

  function read (size, cb) {
    if (!self.opened) return open(size, cb)

    if (first) {
      if (end === -1) {
        if (live) end = Infinity
        else if (snapshot) end = self.length
      }
      if (opts.tail) start = self.length
      first = false
    }

    if (start === end || (end === -1 && start === self.length)) return cb(null, null)
    self.get(start++, opts, cb)
  }

  function cleanup () {
    if (!range) return
    self.undownload(range)
    range = null
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
  if (!this.key) {
    this.key = hash.tree(this._merkle.roots)
    this.discoveryKey = hash.discoveryKey(this.key)
  }
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
  if (!this.writable) return cb(new Error('This feed is not writable. Did you create it?'))

  var self = this
  var pending = this.live ? 2 * batch.length : batch.length
  var offset = 0
  var error = null

  if (!pending) return cb()

  for (var i = 0; i < batch.length; i++) {
    var data = this._codec.encode(batch[i])
    var nodes = this._merkle.next(data)

    if (this._indexing) done(null)
    else this._storage.data.write(this.byteLength + offset, data, done)

    if (this.live) { // TODO: only sign the LAST set of roots in the batch
      var sig = signatures.sign(hash.tree(this._merkle.roots), this.secretKey)
      this._storage.putSignature(this.length + i, sig, done)
    }

    pending += nodes.length
    offset += data.length

    for (var j = 0; j < nodes.length; j++) {
      var node = nodes[j]
      this._storage.putNode(node.index, node, done)
    }
  }

  function done (err) {
    if (err) error = err
    if (--pending) return
    if (error) return cb(error)

    var start = self.length

    // TODO: only emit append and update length / byteLength after the info has been flushed to storage
    self.byteLength += offset
    for (var i = 0; i < batch.length; i++) {
      self.bitfield.set(self.length, true)
      self.tree.set(2 * self.length++)
    }
    self.emit('append')

    var message = self.length - start > 1 ? {start: start, length: self.length - start} : {start: start}
    if (self.peers.length) self._announce(message)

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

Feed.prototype._readyAndCancel = function (start, end) {
  var self = this
  this.ready(function () {
    self._cancel(start, end)
  })
}

Feed.prototype._pollWaiting = function () {
  var len = this._waiting.length
  for (var i = 0; i < len; i++) {
    var next = this._waiting[i]
    if (!next.bytes && !this.bitfield.get(next.index)) continue

    remove(this._waiting, i--)
    len--

    if (next.bytes) this.seek(next.bytes, next, next.callback)
    else if (next.update) this.update(next.index + 1, next.callback)
    else this.get(next.index, next.options, next.callback)
  }
}

Feed.prototype._syncBitfield = function (cb) {
  var missing = this.bitfield.pages.updates.length
  var next = null
  var error = null

  // All data / nodes have been written now. We still need to update the bitfields though

  // TODO 1: if the program fails during this write the bitfield might not have been fully written
  // HOWEVER, we can easily recover from this by traversing the tree and checking if the nodes exists
  // on disk. So if a get fails, it should try and recover once.

  // TODO 2: if .writable append bitfield updates into a single buffer for extra perf
  // Added benefit is that if the program exits while flushing the bitfield the feed will only get
  // truncated and not have missing chunks which is what you expect.

  if (!missing) {
    this._pollWaiting()
    return cb(null)
  }

  while ((next = this.bitfield.pages.lastUpdate()) !== null) {
    this._storage.putBitfield(next.offset, next.buffer, ondone)
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

function isBlock (index) {
  return (index & 1) === 0
}

function defaultStorage (dir) {
  return function (name) {
    return raf(name, {directory: dir})
  }
}

function toCodec (enc) {
  // Switch to ndjson encoding if JSON is used. That way data files parse like ndjson \o/
  return codecs(enc === 'json' ? 'ndjson' : enc)
}

function wrapCodec (enc, cb) {
  return function (err, buf) {
    if (err) return cb(err)
    cb(null, enc.decode(buf))
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
