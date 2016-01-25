var flat = require('flat-tree')
var low = require('last-one-wins')
var crypto = require('crypto')
var bitfield = require('bitfield')
var prefix = require('sublevel-prefixer')()
var generator = require('merkle-tree-stream/generator')
var equals = require('buffer-equals')
var thunky = require('thunky')
var util = require('util')
var events = require('events')
var hash = require('./hash')
var messages = require('./messages')

var BITFIELD_OPTIONS = {grow: 5 * 1024 * 1024}
var EMPTY_BITFIELD = bitfield(0)
var CHECKSUM_MISMATCH = new Error('Checksum mismatch')
CHECKSUM_MISMATCH.notFound = true

module.exports = Feed

function Feed (core, id, opts) {
  if (!(this instanceof Feed)) return new Feed(core, id, opts)
  if (!opts) opts = {}

  events.EventEmitter.call(this)

  var self = this
  var callbacks = null

  this.id = id || null
  this.blocks = opts.blocks || 0
  this.bitfield = bitfield(1, BITFIELD_OPTIONS)
  this.pointer = null
  this.want = []
  this.cache = null
  this.options = opts

  this._core = core
  this._generator = generator(hash)
  this._prefix = prefix((id || opts.pointer || crypto.randomBytes(32)).toString('hex'), '')

  this._storage = opts.storage || null
  this._missing = 0
  this._callbacks = null
  this._next = null
  this._pending = null
  this._appending = 0
  this._afterAppend = afterAppend
  this._sync = low(sync)
  this._extensions = core._extensions

  this.setMaxListeners(0)

  this.opened = false
  this.open = thunky(open)
  this.open()

  function open (cb) {
    if (!self.id) {
      self.opened = true
      self.emit('open')
      self.pointer = opts.pointer || crypto.randomBytes(32)
      self.cache = new Cache(self.pointer, self._core._cache)
      self._prefix = prefix(self.pointer.toString('hex'), '')
      return cb()
    }

    self._core._feeds.get(self.id.toString('hex'), function (_, feed) {
      if (feed) {
        self.blocks = feed.blocks
        self.pointer = feed.pointer || feed.id
      } else {
        self.pointer = self.id
      }
      self.cache = new Cache(self.pointer, self._core._cache)
      self._prefix = prefix(self.pointer.toString('hex'), '')
      self._core._bitfields.get(self.id.toString('hex'), function (_, buf) {
        self.bitfield = bitfield(buf || self.blocks || 1, BITFIELD_OPTIONS)
        self._core.swarm.join(self.id)
        self.opened = true
        self.emit('open')
        cb()
      })
    })
  }

  function afterAppend (err) {
    if (--self._missing) return // TODO: support error handling
    if (!err) self.blocks += self._appending

    var cb = self._next
    self._next = null
    self._appending = 0

    if (self._pending) {
      var values = self._pending
      callbacks = self._callbacks
      self._callbacks = self._pending = null
      self._append(values, call)
    }

    cb(err)
  }

  function call (err) {
    for (var i = 0; i < callbacks.length; i++) callbacks[i](err)
    callbacks = null
  }

  function sync (writeFeed, cb) {
    self.emit('update')

    if (writeFeed) {
      self._core.db.batch([{
        type: 'put',
        key: prefix('feeds', self.id.toString('hex')),
        value: messages.Feed.encode({
          pointer: self.pointer,
          id: self.id,
          blocks: self.blocks
        })
      }, {
        type: 'put',
        key: prefix('bitfields', self.id.toString('hex')),
        value: self.bitfield.buffer
      }], cb)
    } else {
      self._core._bitfields.put(self.id.toString('hex'), self.bitfield.buffer, cb)
    }
  }
}

util.inherits(Feed, events.EventEmitter)

Feed.prototype.ready = function (cb) {
  var self = this
  this.open(function kick (err) {
    if (err) return cb(err)
    if (self.blocks) cb()
    else self.once('update', kick)
  })
}

Feed.prototype.append = function (value, cb) {
  if (this.id) throw new Error('Cannot append to a finalized feed')
  if (!cb) cb = noop
  if (!this.opened) return this._defer(this.append.bind(this, value), cb)
  this._append(Array.isArray(value) ? value : [value], cb)
}

Feed.prototype.finalize = function (cb) {
  if (!cb) cb = noop
  var self = this

  if (this._storage && this._storage.finalize) this._storage.finalize(done)
  else done()

  function done (err) {
    if (err) return cb(err)

    self.flush(function (err) {
      if (err) return cb(err)
      if (!self.blocks || self.id) return cb(null)
      self.id = hash.root(self._generator.roots)
      self.bitfield = fullBitfield(self.blocks)
      self._sync(true, function (err) {
        if (err) return cb(err)
        self._core.swarm.join(self.id)
        self.emit('finalize')
        cb()
      })
    })
  }
}

Feed.prototype.close = function (cb) {
  if (!cb) cb = noop
  var self = this

  if (!this._storage || !this._storage.close) onstorageclose()
  else this._storage.close(onstorageclose)

  function onstorageclose (err) {
    if (err) return cb(err)
    if (self.id) self._core._close(self.id)
    self.emit('close')
    cb()
  }
}

Feed.prototype._defer = function (fn, cb) {
  this.open(function (err) {
    if (err) return cb(err)
    fn(cb)
  })
}

Feed.prototype._append = function (values, cb) {
  if (!cb) cb = noop

  if (this._appending) {
    if (!this._pending) this._pending = []
    if (!this._callbacks) this._callbacks = []
    for (var i = 0; i < values.length; i++) this._pending.push(values[i])
    this._callbacks.push(cb)
    return
  }

  this._appending = values.length
  this._next = cb

  if (!this._appending) {
    this._missing = 1
    this._afterAppend(null)
    return
  }

  this._openStorage()

  var batch = []
  var storageBatch = this._storage && []

  for (var h = 0; h < values.length; h++) {
    var nodes = this._generator.next(values[h])
    for (var j = 0; j < nodes.length; j++) {
      var node = nodes[j]
      if (node.data) {
        if (storageBatch) {
          storageBatch.push(node.data)
        } else {
          batch.push({
            type: 'put',
            key: prefix('blocks', node.hash.toString('hex')),
            value: node.data
          })
        }
      }
      batch.push({
        type: 'put',
        key: prefix('hashes', this._prefix) + node.index,
        value: node.hash
      })
    }
  }

  if (this._storage && storageBatch.length) {
    this._missing = 2
    this._core.db.batch(batch, this._afterAppend)
    this._storage.append(storageBatch, this._afterAppend)
  } else {
    this._missing = 1
    this._core.db.batch(batch, this._afterAppend)
  }
}

Feed.prototype.flush = function (cb) {
  this.append([], cb)
}

Feed.prototype.has = function (block) {
  if (!this.opened) throw new Error('Wait for feed to open before calling this method')
  if (block < this.blocks && !this.id) return true
  return this.bitfield.get(block)
}

Feed.prototype.get = function (block, cb) {
  if (!this.opened) return this._defer(this.get.bind(this, block), cb)
  if (this.blocks && block >= this.blocks) return cb(null, null)

  if (!this.has(block)) {
    this.want.push({block: block, callback: cb})
    this.emit('want', block)
    return
  }

  var self = this

  this._openStorage()
  this._core._hashes.get(this._prefix + (2 * block), function (err, hash) {
    if (err) return cb(err)
    if (self._storage) self._getAndVerify(block, hash, cb)
    else self._core._blocks.get(hash.toString('hex'), cb)
  })
}

Feed.prototype._getAndVerify = function (block, digest, cb) {
  var self = this
  this._storage.get(block, function (err, data) {
    if (err) return cb(err)

    if (!equals(hash.data(data), digest)) {
      // TODO: unhave event that bubbles up to an unhave message here?
      self.bitfield.set(block, false)
      self._sync(false) // TODO: wait for cb? not really needed...
      return cb(CHECKSUM_MISMATCH) // TODO: would it be better to recursively call .get again?
    }

    cb(null, data)
  })
}

Feed.prototype.proof = function (block, have, cb) {
  if (!this.opened) return this._defer(this.proof.bind(this, block), cb)
  if (typeof have === 'function') return this.proof(block, null, have)
  if (block >= this.blocks) return cb(null, [])
  if (!have) have = EMPTY_BITFIELD

  var proof = []
  var limit = 2 * this.blocks

  if (!have.get(2 * block)) {
    var want = flat.sibling(2 * block)
    var needsRoots = true

    while (flat.rightSpan(want) < limit) {
      if (!have.get(want)) {
        proof.push({
          index: want,
          hash: null
        })
      }

      var parent = flat.parent(want)
      if (have.get(parent)) {
        needsRoots = false
        break
      }

      want = flat.sibling(parent)
    }

    if (needsRoots) {
      var roots = flat.fullRoots(limit)
      for (var i = 0; i < roots.length; i++) {
        proof.push({
          index: roots[i],
          hash: null
        })
      }
    }
  }

  getHashes(this._core._hashes, this._prefix, proof, cb)
}

Feed.prototype.put = function (block, data, proof, cb) {
  if (!this.opened) return this._defer(this.proof.bind(this, block, data, proof), cb)
  if (!this.id) throw new Error('Can only .put on finalized feeds')
  if (!cb) cb = noop
  if (!this.blocks) return this._putRoots(block, data, proof, cb)

  var self = this
  var top = hash.data(data)
  var batch = []
  var want = 2 * block
  var offset = 0
  var swap = false
  var hashes = this._core._hashes

  var digest = {
    index: want,
    hash: top
  }

  batch.push({type: 'put', key: self._prefix + digest.index, value: digest.hash})
  hashes.get(this._prefix + want, loop)

  function finalize (err) {
    if (err) return cb(err)
    self.bitfield.set(block, true)

    var remove = []
    for (var i = 0; i < self.want.length; i++) {
      if (self.want[i].block === block) remove.push(i)
    }
    for (var j = remove.length - 1; j >= 0; j--) {
      var want = self.want[remove[j]]
      self.want.splice(remove[j], 1)
      self.emit('unwant', want.block)
      want.callback(null, data)
    }

    self.emit('put', block, data, proof)
    self._sync(false, cb)
  }

  function write (err) {
    if (err) return cb(err)
    self._openStorage()
    if (self._storage) self._storage.put(block, data, finalize)
    else self._core._blocks.put(digest.hash.toString('hex'), data, finalize)
  }

  function validated () {
    if (want === digest.index) return write(null)
    hashes.batch(batch, write)
  }

  function loop (_, trusted) {
    if (trusted && equals(trusted, top)) return validated()

    var sibling = flat.sibling(want)
    swap = sibling < want
    want = flat.parent(sibling)

    if (offset < proof.length && proof[offset].index === sibling) {
      batch.push({type: 'put', key: self._prefix + proof[offset].index, value: proof[offset].hash})
      next(null, proof[offset++].hash)
    } else {
      hashes.get(self._prefix + sibling, next)
    }
  }

  function next (err, sibling) {
    if (err) return cb(err)
    if (swap) top = hash.tree(sibling, top)
    else top = hash.tree(top, sibling)
    batch.push({type: 'put', key: self._prefix + want, value: top})
    hashes.get(self._prefix + want, loop)
  }
}

Feed.prototype._openStorage = function () {
  if (!this._storage && this._core._storage) this._storage = this._core._storage(this)
}

Feed.prototype._putRoots = function (block, data, proof, cb) {
  if (!cb) cb = noop

  var self = this
  var roots = this._verifyRoots(proof)
  if (!roots) return cb(new Error('Validation failed'))

  var proofRoots = proof.slice(-roots.length)
  var batch = new Array(proofRoots.length)

  for (var i = 0; i < proofRoots.length; i++) {
    var next = proofRoots[i]
    batch[i] = {type: 'put', key: this._prefix + next.index, value: next.hash}
  }

  this._core._hashes.batch(batch, function (err) {
    if (err) return cb(err)
    self._sync(true, function (err) {
      if (err) return cb(err)

      var remove = []
      for (var i = 0; i < self.want.length; i++) {
        if (self.want[i].block >= self.blocks) {
          remove.push(i)
        }
      }
      for (var j = remove.length - 1; j >= 0; j--) {
        var want = self.want[remove[j]]
        self.want.splice(remove[j], 1)
        self.emit('unwant', want.block)
        want.callback(null, null)
      }

      self.put(block, data, proof, cb)
    })
  })
}

Feed.prototype._verifyRoots = function (proof) {
  if (!proof.length) return null

  var blocks = (flat.rightSpan(proof[proof.length - 1].index) + 2) / 2
  var roots = flat.fullRoots(2 * blocks)

  if (proof.length < roots.length) return null

  var proofRoots = proof.slice(-roots.length)
  for (var i = 0; i < roots.length; i++) {
    if (proofRoots[i].index !== roots[i]) return null
  }

  if (!equals(this.id, hash.root(proofRoots))) return null

  this.blocks = blocks
  return roots
}

function getHashes (hashes, prefix, proof, cb) {
  var i = 0
  loop()

  function loop () {
    if (i === proof.length) return cb(null, proof)
    hashes.get(prefix + proof[i].index, next)
  }

  function next (err, hash) {
    if (err) return cb(err)
    proof[i++].hash = hash
    loop()
  }
}

function noop () {}

function fullBitfield (size) {
  var rem = size % 8
  var buf = new Buffer((size - rem) / 8 + (rem ? 1 : 0))
  buf.fill(255)
  if (rem) buf[buf.length - 1] = (255 << (8 - rem)) & 255
  return bitfield(buf)
}

function Cache (id, db) {
  this.prefix = prefix(id.toString('hex'), '')
  this.db = db
}

Cache.prototype.get = function (key, opts, cb) {
  this.db.get(this.prefix + key, opts, cb)
}

Cache.prototype.put = function (key, val, opts, cb) {
  this.db.put(this.prefix + key, val, opts, cb)
}
