var low = require('last-one-wins')
var remove = require('unordered-array-remove')
var set = require('unordered-set')
var merkle = require('merkle-tree-stream/generator')
var flat = require('flat-tree')
var bulk = require('bulk-write-stream')
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
var crypto = require('hypercore-crypto')
var inspect = require('inspect-custom-symbol')
var pretty = require('pretty-hash')
var Nanoguard = require('nanoguard')
var safeBufferEquals = require('./lib/safe-buffer-equals')
var replicate = null

var defaultCrypto = {
  sign (data, sk, cb) {
    return cb(null, crypto.sign(data, sk))
  },

  verify (sig, data, pk, cb) {
    return cb(null, crypto.verify(sig, data, pk))
  }
}

module.exports = Feed

function Feed (createStorage, key, opts) {
  if (!(this instanceof Feed)) return new Feed(createStorage, key, opts)
  events.EventEmitter.call(this)

  if (typeof createStorage === 'string') createStorage = defaultStorage(createStorage)
  if (typeof createStorage !== 'function') throw new Error('Storage should be a function or string')

  if (typeof key === 'string') key = Buffer.from(key, 'hex')

  if (!Buffer.isBuffer(key) && !opts) {
    opts = key
    key = null
  }

  if (!opts) opts = {}

  var self = this

  var secretKey = opts.secretKey || null
  if (typeof secretKey === 'string') secretKey = Buffer.from(secretKey, 'hex')

  this.id = opts.id || crypto.randomBytes(32)
  this.live = opts.live !== false
  this.sparse = !!opts.sparse
  this.length = 0
  this.byteLength = 0
  this.maxRequests = opts.maxRequests || 16
  this.key = key || opts.key || null
  this.discoveryKey = this.key && crypto.discoveryKey(this.key)
  this.secretKey = secretKey
  this.bitfield = null
  this.tree = null
  this.writable = !!opts.writable
  this.readable = true
  this.opened = false
  this.closed = false
  this.allowPush = !!opts.allowPush
  this.peers = []
  this.ifAvailable = new Nanoguard()

  // Extensions must be sorted for handshaking to work
  this.extensions = (opts.extensions || []).sort()

  this.crypto = opts.crypto || defaultCrypto

  // hooks
  this._onwrite = opts.onwrite || null

  this._ready = thunky(open) // TODO: if open fails, do not reopen next time
  this._indexing = !!opts.indexing
  this._createIfMissing = opts.createIfMissing !== false
  this._overwrite = !!opts.overwrite
  this._storeSecretKey = opts.storeSecretKey !== false
  this._merkle = null
  this._storage = storage(createStorage, opts.storageCacheSize)
  this._batch = batcher(this._onwrite ? workHook : work)

  this._seq = 0
  this._waiting = []
  this._selections = []
  this._reserved = sparseBitfield()
  this._synced = null

  this._stats = (typeof opts.stats !== 'undefined' && !opts.stats) ? null : {
    downloadedBlocks: 0,
    downloadedBytes: 0,
    uploadedBlocks: 0,
    uploadedBytes: 0
  }

  this._codec = toCodec(opts.valueEncoding)
  this._sync = low(sync)
  if (!this.sparse) this.download({start: 0, end: -1})

  if (this.sparse && opts.eagerUpdate) {
    this.update(function loop (err) {
      if (err) this.emit('update-error', err)
      self.update(loop)
    })
  }

  // open it right away. TODO: do not reopen (i.e, set a flag not to retry)
  this._ready(onerror)

  function onerror (err) {
    if (err) self.emit('error', err)
  }

  function workHook (values, cb) {
    if (!self._merkle) return self._reloadMerkleStateBeforeAppend(workHook, values, cb)
    self._appendHook(values, cb)
  }

  function work (values, cb) {
    if (!self._merkle) return self._reloadMerkleStateBeforeAppend(work, values, cb)
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

Feed.discoveryKey = crypto.discoveryKey

Feed.prototype[inspect] = function (depth, opts) {
  var indent = ''
  if (typeof opts.indentationLvl === 'number') {
    while (indent.length < opts.indentationLvl) indent += ' '
  }
  return 'Hypercore(\n' +
    indent + '  key: ' + opts.stylize((this.key && pretty(this.key)), 'string') + '\n' +
    indent + '  discoveryKey: ' + opts.stylize((this.discoveryKey && pretty(this.discoveryKey)), 'string') + '\n' +
    indent + '  opened: ' + opts.stylize(this.opened, 'boolean') + '\n' +
    indent + '  sparse: ' + opts.stylize(this.sparse, 'boolean') + '\n' +
    indent + '  writable: ' + opts.stylize(this.writable, 'boolean') + '\n' +
    indent + '  length: ' + opts.stylize(this.length, 'number') + '\n' +
    indent + '  byteLength: ' + opts.stylize(this.byteLength, 'number') + '\n' +
    indent + '  peers: ' + opts.stylize(this.peers.length, 'number') + '\n' +
    indent + ')'
}

// TODO: instead of using a getter, update on remote-update/add/remove
Object.defineProperty(Feed.prototype, 'remoteLength', {
  enumerable: true,
  get: function () {
    var len = 0
    for (var i = 0; i < this.peers.length; i++) {
      var remoteLength = this.peers[i].remoteLength
      if (remoteLength > len) len = remoteLength
    }
    return len
  }
})

Object.defineProperty(Feed.prototype, 'stats', {
  enumerable: true,
  get: function () {
    if (!this._stats) return null
    var peerStats = []
    for (var i = 0; i < this.peers.length; i++) {
      var peer = this.peers[i]
      peerStats[i] = peer._stats
    }
    return {
      peers: peerStats,
      totals: this._stats
    }
  }
})

Feed.prototype.replicate = function (opts) {
  // Lazy load replication deps
  if (!replicate) replicate = require('./lib/replicate')

  if ((!this._selections.length || this._selections[0].end !== -1) && !this.sparse && !(opts && opts.live)) {
    // hack!! proper fix is to refactor ./replicate to *not* clear our non-sparse selection
    this.download({start: 0, end: -1})
  }

  opts = opts || {}
  opts.stats = !!this._stats

  if (!opts.extensions) opts.extensions = this.extensions

  var stream = replicate(this, opts)
  this.emit('replicating', stream)
  return stream
}

Feed.prototype.ready = function (onready) {
  this._ready(function (err) {
    if (!err) onready()
  })
}

Feed.prototype.update = function (opts, cb) {
  if (typeof opts === 'function') return this.update(-1, opts)
  if (typeof opts === 'number') opts = { minLength: opts }
  if (!opts) opts = {}
  if (!cb) cb = noop

  var self = this
  var len = typeof opts.minLength === 'number' ? opts.minLength : -1

  this.ready(function (err) {
    if (err) return cb(err)
    if (len === -1) len = self.length + 1
    if (self.length >= len) return cb(null)

    if (self.writable) cb = self._writeStateReloader(cb)

    var w = {
      hash: opts.hash !== false,
      bytes: 0,
      index: len - 1,
      update: true,
      options: opts,
      callback: cb
    }

    self._waiting.push(w)
    if (opts.ifAvailable) self._ifAvailable(w, len)
    self._updatePeers()
  })
}

Feed.prototype._ifAvailable = function (w, minLength) {
  var cb = w.callback
  var called = false
  var self = this

  w.callback = done

  this.ifAvailable.ready(function () {
    if (self.closed) return done(new Error('Closed'))
    if (self.length >= minLength || self.remoteLength >= minLength) return
    done(new Error('No update available from peers'))
  })

  function done (err) {
    if (called) return
    called = true

    var i = self._waiting.indexOf(w)
    if (i > -1) self._waiting.splice(i, 1)
    cb(err)
  }
}

Feed.prototype._ifAvailableGet = function (w) {
  var cb = w.callback
  var called = false
  var self = this

  w.callback = done

  this.ifAvailable.ready(function () {
    if (self.closed) return done(new Error('Closed'))
    for (var i = 0; i < self.peers.length; i++) {
      var peer = self.peers[i]
      if (peer.remoteBitfield.get(w.index)) return
    }
    done(new Error('Block not available from peers'))
  })

  function done (err, data) {
    if (called) return
    called = true

    var i = self._waiting.indexOf(w)
    if (i > -1) self._waiting.splice(i, 1)
    cb(err, data)
  }
}

// will reload the writable state. used by .update on a writable peer
Feed.prototype._writeStateReloader = function (cb) {
  var self = this
  return function (err) {
    if (err) return cb(err)
    self._reloadMerkleState(cb)
  }
}

Feed.prototype._reloadMerkleState = function (cb) {
  var self = this

  this._roots(self.length, function (err, roots) {
    if (err) return cb(err)
    self._merkle = merkle(crypto, roots)
    cb(null)
  })
}

Feed.prototype._reloadMerkleStateBeforeAppend = function (work, values, cb) {
  this._reloadMerkleState(function (err) {
    if (err) return cb(err)
    work(values, cb)
  })
}

Feed.prototype._open = function (cb) {
  var self = this
  var generatedKey = false
  var retryOpen = true

  // TODO: clean up the duplicate code below ...

  this._storage.openKey(function (_, key) {
    if (key && !self._overwrite && !self.key) self.key = key

    if (!self.key && self.live) {
      var keyPair = crypto.keyPair()
      self.secretKey = keyPair.secretKey
      self.key = keyPair.publicKey
      generatedKey = true
    }

    self.discoveryKey = self.key && crypto.discoveryKey(self.key)
    self._storage.open({key: self.key, discoveryKey: self.discoveryKey}, onopen)
  })

  function onopen (err, state) {
    if (err) return cb(err)

    // if no key but we have data do a bitfield reset since we cannot verify the data.
    if (!state.key && state.bitfield.length) {
      self._overwrite = true
    }

    if (self._overwrite) {
      state.bitfield = []
      state.key = state.secretKey = null
    }

    self.bitfield = bitfield(state.bitfieldPageSize, state.bitfield)
    self.tree = treeIndex(self.bitfield.tree)
    self.length = self.tree.blocks()
    self._seq = self.length

    if (state.key && self.key && Buffer.compare(state.key, self.key) !== 0) {
      return self._forceCloseAndError(cb, new Error('Another hypercore is stored here'))
    }

    if (state.key) self.key = state.key
    if (state.secretKey) self.secretKey = state.secretKey

    if (!self.length) return onsignature(null, null)
    self._storage.getSignature(self.length - 1, onsignature)

    function onsignature (_, sig) {
      if (self.length) self.live = !!sig

      if ((generatedKey || !self.key) && !self._createIfMissing) {
        return self._forceCloseAndError(cb, new Error('No hypercore is stored here'))
      }

      if (!self.key && self.live) {
        var keyPair = crypto.keyPair()
        self.secretKey = keyPair.secretKey
        self.key = keyPair.publicKey
      }

      var writable = !!self.secretKey || self.key === null

      if (!writable && self.writable) return self._forceCloseAndError(cb, new Error('Feed is not writable'))
      self.writable = writable
      self.discoveryKey = self.key && crypto.discoveryKey(self.key)

      if (self._storeSecretKey && !self.secretKey) {
        self._storeSecretKey = false
      }

      var shouldWriteKey = generatedKey || !safeBufferEquals(self.key, state.key)
      var shouldWriteSecretKey = self._storeSecretKey && (generatedKey || !safeBufferEquals(self.secretKey, state.secretKey))

      var missing = 1 +
        (shouldWriteKey ? 1 : 0) +
        (shouldWriteSecretKey ? 1 : 0) +
        (self._overwrite ? 1 : 0)
      var error = null

      if (shouldWriteKey) self._storage.key.write(0, self.key, done)
      if (shouldWriteSecretKey) self._storage.secretKey.write(0, self.secretKey, done)

      if (self._overwrite) {
        self._storage.bitfield.del(32, Infinity, done)
      }

      done(null)

      function done (err) {
        if (err) error = err
        if (--missing) return
        if (error) return self._forceCloseAndError(cb, error)
        self._roots(self.length, onroots)
      }

      function onroots (err, roots) {
        if (err && retryOpen) {
          retryOpen = false
          self.length--
          self._storage.getSignature(self.length - 1, onsignature)
          return
        }

        if (err) return self._forceCloseAndError(cb, err)

        self._merkle = merkle(crypto, roots)
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
  if (!cb) cb = noop
  if (!this.readable) return cb(new Error('Feed is closed'))

  // TODO: if no peers, check if range is already satisfied and nextTick(cb) if so
  // this._updatePeers does this for us when there is a peer though, so not critical

  var sel = {
    _index: this._selections.length,
    hash: !!range.hash,
    iterator: null,
    start: range.start || 0,
    end: range.end || -1,
    want: 0,
    linear: !!range.linear,
    callback: cb
  }

  sel.want = toWantRange(sel.start)

  this._selections.push(sel)
  this._updatePeers()

  return sel
}

Feed.prototype.undownload = function (range) {
  if (typeof range === 'number') range = {start: range, end: range + 1}
  if (!range) range = {}

  if (range.callback && range._index > -1) {
    set.remove(this._selections, range)
    process.nextTick(range.callback, createError('ECANCELED', -11, 'Download was cancelled'))
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
      process.nextTick(range.callback, createError('ECANCELED', -11, 'Download was cancelled'))
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

Feed.prototype.cancel = function (start, end) { // TODO: use same argument scheme as download
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
      if (w.callback) process.nextTick(w.callback, new Error('Request cancelled'))
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

    if (!modified) return process.nextTick(cb)

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

Feed.prototype.signature = function (index, cb) {
  if (typeof index === 'function') return this.signature(this.length - 1, index)

  if (index < 0 || index >= this.length) return cb(new Error('No signature available for this index'))

  this._storage.nextSignature(index, cb)
}

Feed.prototype.verify = function (index, signature, cb) {
  var self = this

  this.rootHashes(index, function (err, roots) {
    if (err) return cb(err)

    var checksum = crypto.tree(roots)

    self.crypto.verify(checksum, signature, self.key, function (err, valid) {
      if (err) return cb(err)

      if (!valid) return cb(new Error('Signature verification failed'))

      return cb(null, true)
    })
  })
}

Feed.prototype.rootHashes = function (index, cb) {
  this._getRootsToVerify(index * 2 + 2, {}, [], cb)
}

Feed.prototype.seek = function (bytes, opts, cb) {
  if (typeof opts === 'function') return this.seek(bytes, null, opts)
  if (!opts) opts = {}
  if (!this.opened) return this._readyAndSeek(bytes, opts, cb)

  var self = this

  if (bytes === this.byteLength) return process.nextTick(cb, null, this.length, 0)

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
      want: toWantRange(start),
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
    if (isBlock(top)) return cb(null, nearestRoot, offset)

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
      if (flat.parent(node.index) === nearestRoot) {
        nearestRoot = flat.sibling(node.index)
        onroot(nearestRoot)
      } else {
        onroot(flat.sibling(node.index))
      }
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
  if (!this._onwrite) return this._writeAfterHook(index, data, nodes, sig, from, cb)
  this._onwrite(index, data, from, writeHookDone(this, index, data, nodes, sig, from, cb))
}

function writeHookDone (self, index, data, nodes, sig, from, cb) {
  return function (err) {
    if (err) return cb(err)
    self._writeAfterHook(index, data, nodes, sig, from, cb)
  }
}

Feed.prototype._writeAfterHook = function (index, data, nodes, sig, from, cb) {
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
    if (this.bitfield.set(index, true)) {
      if (this._stats) {
        this._stats.downloadedBlocks += 1
        this._stats.downloadedBytes += data.length
      }
      this.emit('download', index, data, from)
    }
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
  var top = data ? new storage.Node(2 * index, crypto.data(data), data.length) : remoteNodes.shift()

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
    top = new storage.Node(flat.parent(top.index), crypto.parent(top, node), top.size + node.size)

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
  var self = this

  this._getRootsToVerify(verifiedBy, top, remoteNodes, function (err, roots, extraNodes) {
    if (err) return cb(err)

    var checksum = crypto.tree(roots)
    var signature = null

    if (self.length && self.live && !proof.signature) {
      return cb(new Error('Remote did not include a signature'))
    }

    if (proof.signature) { // check signatures
      self.crypto.verify(checksum, proof.signature, self.key, function (err, valid) {
        if (err) return cb(err)
        if (!valid) return cb(new Error('Remote signature could not be verified'))

        signature = {index: verifiedBy / 2 - 1, signature: proof.signature}
        write()
      })
    } else { // check tree root
      if (Buffer.compare(checksum, self.key) !== 0) {
        return cb(new Error('Remote checksum failed'))
      }

      write()
    }

    function write () {
      self.live = !!signature

      var length = verifiedBy / 2
      if (length > self.length) {
        // TODO: only emit this after the info has been flushed to storage
        if (self.writable) self._merkle = null // We need to reload merkle state now
        self.length = length
        self._seq = length
        self.byteLength = roots.reduce(addSize, 0)
        if (self._synced) self._synced.seek(0, self.length)
        self.emit('append')
      }

      self._write(index, data, nodes.concat(extraNodes), signature, from, cb)
    }
  })
}

Feed.prototype._getRootsToVerify = function (verifiedBy, top, remoteNodes, cb) {
  var indexes = flat.fullRoots(verifiedBy)
  var roots = new Array(indexes.length)
  var nodes = []
  var error = null
  var pending = roots.length

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
      onnode(new Error('Missing tree roots needed for verify'))
    }
  }

  function onnode (err, node) {
    if (err) error = err
    if (node) roots[indexes.indexOf(node.index)] = node
    if (!--pending) done(error)
  }

  function done (err) {
    if (err) return cb(err)

    cb(null, roots, nodes)
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

Feed.prototype.head = function (opts, cb) {
  if (typeof opts === 'function') return this.head({}, opts)
  var self = this
  this._ready(function (err) {
    if (err) return cb(err)
    if (self.length === 0) cb(new Error('feed is empty'))
    else self.get(self.length - 1, opts, cb)
  })
}

Feed.prototype.get = function (index, opts, cb) {
  if (typeof opts === 'function') return this.get(index, null, opts)
  if (!this.opened) return this._readyAndGet(index, opts, cb)
  if (!this.readable) return cb(new Error('Feed is closed'))

  if (opts && opts.timeout) cb = timeoutCallback(cb, opts.timeout)

  if (!this.bitfield.get(index)) {
    if (opts && opts.wait === false) return cb(new Error('Block not downloaded'))

    var w = { bytes: 0, hash: false, index: index, options: opts, callback: cb }
    this._waiting.push(w)

    if (opts && opts.ifAvailable) this._ifAvailableGet(w)

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

Feed.prototype.getBatch = function (start, end, opts, cb) {
  if (typeof opts === 'function') return this.getBatch(start, end, null, opts)
  if (!this.opened) return this._readyAndGetBatch(start, end, opts, cb)

  var self = this
  var wait = !opts || opts.wait !== false

  if (this.has(start, end)) return this._getBatch(start, end, opts, cb)
  if (!wait) return cb(new Error('Block not downloaded'))

  if (opts && opts.timeout) cb = timeoutCallback(cb, opts.timeout)

  this.download({start: start, end: end}, function (err) {
    if (err) return cb(err)
    self._getBatch(start, end, opts, cb)
  })
}

Feed.prototype._getBatch = function (start, end, opts, cb) {
  var enc = opts && opts.valueEncoding
  var codec = enc ? toCodec(enc) : this._codec

  this._storage.getDataBatch(start, end - start, onbatch)

  function onbatch (err, buffers) {
    if (err) return cb(err)

    var batch = new Array(buffers.length)

    for (var i = 0; i < buffers.length; i++) {
      try {
        batch[i] = codec ? codec.decode(buffers[i]) : buffers[i]
      } catch (err) {
        return cb(err)
      }
    }

    cb(null, batch)
  }
}

Feed.prototype._readyAndGetBatch = function (start, end, opts, cb) {
  var self = this
  this._ready(function (err) {
    if (err) return cb(err)
    self.getBatch(start, end, opts, cb)
  })
}

Feed.prototype._updatePeers = function () {
  for (var i = 0; i < this.peers.length; i++) this.peers[i].update()
}

Feed.prototype.createWriteStream = function () {
  var self = this
  return bulk.obj(write)

  function write (batch, cb) {
    self.append(batch, cb)
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
    if (!self.readable) return cb(new Error('Feed is closed'))

    if (first) {
      if (end === -1) {
        if (live) end = Infinity
        else if (snapshot) end = self.length
        if (start > end) return cb(null, null)
      }
      if (opts.tail) start = self.length
      first = false
    }

    if (start === end || (end === -1 && start === self.length)) return cb(null, null)

    range.start++
    if (range.iterator) range.iterator.start++

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
    this.key = crypto.tree(this._merkle.roots)
    this.discoveryKey = crypto.discoveryKey(this.key)
  }
  this._storage.key.write(0, this.key, cb)
}

Feed.prototype.append = function (batch, cb) {
  if (!cb) cb = noop

  var self = this
  var list = Array.isArray(batch) ? batch : [batch]
  this._batch(list, onappend)

  function onappend (err) {
    if (err) return cb(err)
    var seq = self._seq
    self._seq += list.length
    cb(null, seq)
  }
}

Feed.prototype.flush = function (cb) {
  this.append([], cb)
}

Feed.prototype.close = function (cb) {
  var self = this

  this._ready(function () {
    self._forceCloseAndError(cb, null)
  })
}

Feed.prototype._forceCloseAndError = function (cb, error) {
  var self = this

  this.writable = false
  this.readable = false
  this._storage.close(function (err) {
    if (!err) err = error
    if (!self.closed && !err) self._onclose()
    if (cb) cb(err)
  })
}

Feed.prototype._onclose = function () {
  this.ifAvailable.destroy()
  this.closed = true

  while (this._waiting.length) {
    this._waiting.pop().callback(new Error('Feed is closed'))
  }
  while (this._selections.length) {
    this._selections.pop().callback(new Error('Feed is closed'))
  }

  this.emit('close')
}

Feed.prototype._appendHook = function (batch, cb) {
  var self = this
  var missing = batch.length
  var error = null

  if (!missing) return this._append(batch, cb)
  for (var i = 0; i < batch.length; i++) {
    this._onwrite(i + this.length, batch[i], null, done)
  }

  function done (err) {
    if (err) error = err
    if (--missing) return
    if (error) return cb(error)
    self._append(batch, cb)
  }
}

Feed.prototype._append = function (batch, cb) {
  if (!this.opened) return this._readyAndAppend(batch, cb)
  if (!this.writable) return cb(new Error('This feed is not writable. Did you create it?'))

  var self = this
  var pending = 1
  var offset = 0
  var error = null
  var nodeBatch = new Array(batch.length ? batch.length * 2 - 1 : 0)
  var nodeOffset = this.length * 2
  var dataBatch = new Array(batch.length)

  if (!pending) return cb()

  for (var i = 0; i < batch.length; i++) {
    var data = this._codec.encode(batch[i])
    var nodes = this._merkle.next(data)

    offset += data.length
    dataBatch[i] = data

    for (var j = 0; j < nodes.length; j++) {
      var node = nodes[j]
      if (node.index >= nodeOffset && node.index - nodeOffset < nodeBatch.length) {
        nodeBatch[node.index - nodeOffset] = node
      } else {
        pending++
        this._storage.putNode(node.index, node, done)
      }
    }
  }

  if (this.live && batch.length) {
    pending++
    this.crypto.sign(crypto.tree(this._merkle.roots), this.secretKey, function (err, sig) {
      if (err) return done(err)
      self._storage.putSignature(self.length + batch.length - 1, sig, done)
    })
  }

  if (!this._indexing) {
    pending++
    if (dataBatch.length === 1) this._storage.data.write(this.byteLength, dataBatch[0], done)
    else this._storage.data.write(this.byteLength, Buffer.concat(dataBatch), done)
  }

  this._storage.putNodeBatch(nodeOffset, nodeBatch, done)

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

Feed.prototype.audit = function (cb) {
  if (!cb) cb = noop

  var self = this
  var report = {
    valid: 0,
    invalid: 0
  }

  this.ready(function (err) {
    if (err) return cb(err)

    var block = 0
    var max = self.length

    next()

    function onnode (err, node) {
      if (err) return ondata(null, null)
      self._storage.getData(block, ondata)

      function ondata (_, data) {
        var verified = data && crypto.data(data).equals(node.hash)
        if (verified) report.valid++
        else report.invalid++
        self.bitfield.set(block, verified)
        block++
        next()
      }
    }

    function next () {
      while (block < max && !self.bitfield.get(block)) block++
      if (block >= max) return done()
      self._storage.getNode(2 * block, onnode)
    }

    function done () {
      self._sync(null, function (err) {
        if (err) return cb(err)
        cb(null, report)
      })
    }
  })
}

Feed.prototype.extension = function (name, message) {
  var peers = this.peers

  for (var i = 0; i < peers.length; i++) {
    peers[i].extension(name, message)
  }
}

function noop () {}

function verifyNode (trusted, node) {
  return trusted && trusted.index === node.index && Buffer.compare(trusted.hash, node.hash) === 0
}

function addSize (size, node) {
  return size + node.size
}

function isBlock (index) {
  return (index & 1) === 0
}

function defaultStorage (dir) {
  return function (name) {
    try {
      var lock = name === 'bitfield' ? require('fd-lock') : null
    } catch (err) {}
    return raf(name, {directory: dir, lock: lock})
  }
}

function toCodec (enc) {
  // Switch to ndjson encoding if JSON is used. That way data files parse like ndjson \o/
  return codecs(enc === 'json' ? 'ndjson' : enc)
}

function wrapCodec (enc, cb) {
  return function (err, buf) {
    if (err) return cb(err)
    try {
      buf = enc.decode(buf)
    } catch (err) {
      return cb(err)
    }
    cb(null, buf)
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

function toWantRange (i) {
  return Math.floor(i / 1024 / 1024) * 1024 * 1024
}

function createError (code, errno, msg) {
  var err = new Error(msg)
  err.code = code
  err.errno = errno
  return err
}
