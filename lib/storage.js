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
  this.secretKey = create('secret.key')
  this.tree = create('tree')
  this.data = create('data')
  this.dataBitfield = create('data.bitfield')
  this.treeBitfield = create('tree.bitfield')
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

  var leaf = !(index & 1)
  var offset = 40 * index + 64 * Math.ceil(index / 2)
  var length = leaf ? 104 : 40
  var self = this

  this.tree.read(offset, length, function (err, buf) {
    if (err) return cb(err)

    var hash = buf.slice(0, 32)
    var size = uint64be.decode(buf, 32)

    if (!size && !notBlank(hash)) return cb(new Error('Index not found ' + index + ' '))

    var val = new Node(index, hash, size, leaf ? notBlank(buf.slice(40)) : null)
    if (self.cache) self.cache.set(index, val)
    cb(null, val)
  })
}

Storage.prototype.putNode = function (index, node, cb) {
  if (!cb) cb = noop

  // TODO: re-enable put cache. currently this causes a memleak
  // because node.hash is a slice of the big data buffer on replicate
  // if (this.cache) this.cache.set(index, node)

  var leaf = !(index & 1)
  var length = leaf ? 104 : 40
  var offset = 40 * index + 64 * Math.ceil(index / 2)
  var buf = new Buffer(length)

  node.hash.copy(buf, 0)
  uint64be.encode(node.size, buf, 32)

  if (leaf) {
    if (node.signature) node.signature.copy(buf, 40)
    else blank.copy(buf, 40)
  }

  this.tree.write(offset, buf, cb)
}

Storage.prototype.close = function (cb) {
  var missing = 6
  var error = null

  close(this.treeBitfield, done)
  close(this.dataBitfield, done)
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
  var error = null
  var missing = 4

  var result = {
    treeBitfield: new Buffer(0),
    dataBitfield: new Buffer(0),
    secretKey: null,
    key: null
  }

  readBitfield(this.treeBitfield, function (err, data) {
    if (data) result.treeBitfield = data
    done(err)
  })

  readBitfield(this.dataBitfield, function (err, data) {
    if (data) result.dataBitfield = data
    done(err)
  })

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

function Node (index, hash, size, sig) {
  this.index = index
  this.hash = hash
  this.size = size
  this.signature = sig
}

function findNode (nodes, index) {
  for (var i = 0; i < nodes.length; i++) {
    if (nodes[i].index === index) return nodes[i]
  }
  return null
}

function notBlank (buf) {
  for (var i = 0; i < buf.length; i++) {
    if (buf[i]) return buf
  }
  return null
}

function readBitfield (st, cb) {
  st.open(function (err) {
    if (err) return cb(err)
    st.read(0, st.length, function (err, data) {
      if (err) return cb(err)
      cb(null, coerce(data))
    })
  })
}

function coerce (data) {
  var remainder = 1024 - (data.length & 1023)
  if (remainder === 1024) return data

  var blank = new Buffer(remainder)
  blank.fill(0)
  return Buffer.concat([data, blank])
}

function close (st, cb) {
  if (st.close) st.close(cb)
  else cb()
}
