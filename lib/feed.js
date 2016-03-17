var low = require('last-one-wins')
var inherits = require('inherits')
var flat = require('flat-tree')
var crypto = require('crypto')
var merkle = require('merkle-tree-stream/generator')
var events = require('events')
var thunky = require('thunky')
var signatures = require('sodium-signatures')
var equals = require('buffer-equals')
var bitfield = require('./bitfield')
var hash = require('./hash')
var tree = require('./tree-index')
var messages = require('./messages')

module.exports = Feed

function Feed (core, opts) {
  if (!(this instanceof Feed)) return new Feed(core, opts)
  events.EventEmitter.call(this)

  var self = this

  this.options = opts || {}
  this.key = this.options.key || null
  this.secretKey = this.options.secretKey || null
  this.publicId = this.key ? hash.publicId(this.key) : null
  this.prefix = null
  this.live = !!this.options.live || !!this.secretKey
  this.open = thunky(open)
  this.opened = false
  this.tree = tree()
  this.bitfield = bitfield(32)

  if (this.live && !this.key) {
    var keyPair = signatures.keyPair()
    this.key = keyPair.publicKey
    this.secretKey = keyPair.secretKey
    this.publicId = hash.publicId(this.key)
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

  var callbacks = null
  var error = null

  function afterAppend (err) {
    if (err) error = err
    if (--self._missing) return

    self._appendCallback(error)
    self._appendCallback = null
    error = callbacks = null

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

Feed.prototype.has = function (block) {
  return this.bitfield.get(block)
}

Feed.prototype.get = function (block, cb) {
  if (!this.opened) return this._openAndGet(block, cb)
  if (!this.bitfield.get(block)) throw new Error('TODO: wait for block ...')
  this._core._data.get(this._prefix + block, cb)
}

Feed.prototype.put = function (block, data, proof, cb) {
  if (!this.opened) return this._openAndPut(block, data, proof, cb)

  var self = this
  var offset = 0
  var batch = []
  var nodes = proof.nodes || []

  var top = {
    index: 2 * block,
    size: data.length,
    hash: hash.data(data)
  }

  batch.push({
    type: 'put',
    key: '!data!' + this._prefix + block,
    value: data
  })

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
      self._db.batch(batch, finalize)
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

    self._putRoots(top, batch, proof.nodes, offset, proof.signature, finalize)
    offset = nodes.length // put roots returns error if this does hold anyways
  }

  function finalize (err) {
    if (err) return cb(err)

    self.tree.set(2 * block)
    for (var i = 0; i < offset; i++) self.tree.set(nodes[i].index)
    self.bitfield.set(block, true)
    self._first = false
    self._sync(null, cb)
  }
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
        key: '!feeds!' + self.publicId.toString('hex'),
        value: messages.Feed.encode(self)
      })
    }

    self._db.batch(batch, cb)
  }
}

Feed.prototype.stats = function (cb) {
  cb(new Error('Not yet implemented'))
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
  if (this.live) return process.nextTick(cb)

  this.flush(onflush)

  function onflush (err) {
    if (err) return cb(err)
    self.key = self._merkle.roots.length ? hash.tree(self._merkle.roots) : null
    self.publicId = self.key && hash.publicId(self.key)
    if (!self.key) return cb()
    self._sync(null, onsync)
  }

  function onsync (err) {
    if (err) return cb(err)
    self._core._feeds.put(self.publicId.toString('hex'), self, cb)
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

  this.flush(function (err) {
    if (err) return cb(err)
    self.emit('close')
    cb()
  })
}

Feed.prototype._flush = function (cb) {
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

  if (!this.publicId) {
    this.prefix = this.options.prefix || crypto.randomBytes(32)
    this._merkle = merkle(hash)
    this._prefix = '!' + this.prefix.toString('hex') + '!'
    return process.nextTick(done)
  }

  this._core._feeds.get(this.publicId.toString('hex'), function (_, feed) {
    if (feed) {
      self.key = feed.key
      self.secretKey = feed.secretKey
      self.prefix = feed.prefix || feed.key
      self.live = feed.live
    } else {
      wroteFeed = true
    }

    if (!self.prefix) self.prefix = self.options.prefix || crypto.randomBytes(32)
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
  })

  function index () {
    // TODO: move this to first append instead of on open for a bit faster open
    var indexes = self.tree.roots()
    var roots = []

    loop(null, null)

    function loop (err, node) {
      if (err) return done(err)
      if (node) roots.push(node)

      if (!indexes.length) {
        self._merkle = merkle(hash, roots)
        if (wroteFeed) self._core._feeds.put(self.publicId.toString('hex'), self, done)
        else done()
      } else {
        self._core._nodes.get(self._prefix + indexes.shift(), loop)
      }
    }
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

  this._missing = this.live ? 2 : 1
  this._buffer = null
  this._bufferCallbacks = null
  this._appendCallback = cb

  var batch = []

  for (var i = 0; i < buffers.length; i++) {
    var buf = typeof buffers[i] === 'string' ? Buffer(buffers[i]) : buffers[i]
    var nodes = this._merkle.next(buf)

    for (var j = 0; j < nodes.length; j++) {
      var node = nodes[j]

      if (node.data) {
        this.tree.set(node.index)
        this.bitfield.set(node.index / 2, true)
        batch.push({
          type: 'put',
          key: '!data!' + this._prefix + (node.index / 2),
          value: node.data
        })

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
  if (this.live) this._sync(null, this._afterAppend)
}

Feed.prototype.flush = function (cb) {
  this._append([], cb || noop)
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

Feed.prototype._openAndGet = function (block, cb) {
  var self = this
  this.open(function (err) {
    if (err) return cb(err)
    self.get(block, cb)
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
