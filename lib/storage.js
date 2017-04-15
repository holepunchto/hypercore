var uint64be = require('uint64be')
var flat = require('flat-tree')
var alru = require('array-lru')

module.exports = Storage

var noarr = []
var blank = new Buffer(64)
blank.fill(0)

function Storage (create) {
  if (!(this instanceof Storage)) return new Storage(create)

  this.cache = alru(65536, {indexedValues: true})
  this.key = create('key')
  this.secretKey = create('secret_key')
  this.tree = create('tree')
  this.data = create('data')
  this.bitfield = create('bitfield')
  this.signatures = create('signatures')
}

Storage.prototype.putData = function (index, data, nodes, cb) {
  if (!cb) cb = noop
  var self = this
  if (!data.length) return cb(null)
  this.dataOffset(index, nodes, function (err, offset, size) {
    if (err) return cb(err)
    if (size !== data.length) return cb(new Error('Unexpected data size'))
    self.data.write(offset, data, cb)
  })
}

Storage.prototype.getData = function (index, cb) {
  var self = this
  this.dataOffset(index, noarr, function (err, offset, size) {
    if (err) return cb(err)
    self.data.read(offset, size, cb)
  })
}

Storage.prototype.getSignature = function (index, cb) {
  this.signatures.read(32 + 64 * index, 64, function (err, signature) {
    if (err) return cb(err)
    if (isBlank(signature)) return cb(new Error('No signature found'))
    cb(null, signature)
  })
}

Storage.prototype.putSignature = function (index, signature, cb) {
  this.signatures.write(32 + 64 * index, signature, cb)
}

Storage.prototype.dataOffset = function (index, cachedNodes, cb) {
  var roots = flat.fullRoots(2 * index)
  var self = this
  var offset = 0
  var pending = roots.length
  var error = null
  var blk = 2 * index

  if (!pending) this.getNode(2 * index, onlast)

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

Storage.prototype.getNode = function (index, cb) {
  if (this.cache) {
    var cached = this.cache.get(index)
    if (cached) return cb(null, cached)
  }

  var self = this

  this.tree.read(32 + 40 * index, 40, function (err, buf) {
    if (err) return cb(err)

    var hash = buf.slice(0, 32)
    var size = uint64be.decode(buf, 32)

    if (!size && isBlank(hash)) return cb(new Error('No node found'))

    var val = new Node(index, hash, size, null)
    if (self.cache) self.cache.set(index, val)
    cb(null, val)
  })
}

Storage.prototype.putNode = function (index, node, cb) {
  if (!cb) cb = noop

  // TODO: re-enable put cache. currently this causes a memleak
  // because node.hash is a slice of the big data buffer on replicate
  // if (this.cache) this.cache.set(index, node)

  var buf = new Buffer(40)

  node.hash.copy(buf, 0)
  uint64be.encode(node.size, buf, 32)
  this.tree.write(32 + 40 * index, buf, cb)
}

Storage.prototype.putBitfield = function (offset, data, cb) {
  this.bitfield.write(32 + offset, data, cb)
}

Storage.prototype.close = function (cb) {
  if (!cb) cb = noop
  var missing = 5
  var error = null

  close(this.bitfield, done)
  close(this.tree, done)
  close(this.data, done)
  close(this.key, done)
  close(this.secretKey, done)

  function done (err) {
    if (err) error = err
    if (--missing) return
    cb(error)
  }
}

Storage.prototype.open = function (cb) {
  var self = this
  var error = null
  var missing = 5

  var result = {
    bitfield: new Buffer(0),
    secretKey: null,
    key: null
  }

  this.bitfield.write(0, header(0, 3328, null), function (err) {
    if (err) return cb(err)
    self.bitfield.read(32, self.bitfield.length - 32, function (err, data) {
      if (data) result.bitfield = data
      done(err)
    })
  })

  this.signatures.write(0, header(1, 64, 'Ed25519'), done)
  this.tree.write(0, header(2, 40, 'BLAKE2b'), done)

  // TODO: Improve the error handling here.
  // I.e. if secretKey length === 64 and it fails, error

  this.secretKey.read(0, 64, function (_, data) {
    if (data) result.secretKey = data
    done(null)
  })

  this.key.read(0, 32, function (_, data) {
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
  var buf = new Buffer(32)
  buf.fill(0)

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
