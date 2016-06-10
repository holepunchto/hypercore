var CreateHmac = require('create-hmac')
var CreateHash = require('create-hash')
var uint64be = require('uint64be')

// https://en.wikipedia.org/wiki/Merkle_tree#Second_preimage_attack
var LEAF_TYPE = Buffer([0])
var PARENT_TYPE = Buffer([1])
var ROOT_TYPE = Buffer([2])
var HYPERCORE = Buffer('hypercore')

var tmp = Buffer([0, 0, 0, 0, 0, 0, 0, 0])

exports.data = function (data) {
  return createHash()
    .update(LEAF_TYPE)
    .update(encodeUInt64(data.length))
    .update(data)
    .digest()
}

exports.leaf = function (leaf) {
  return exports.data(leaf.data)
}

exports.parent = function (a, b) {
  if (a.index > b.index) {
    var tmp = a
    a = b
    b = tmp
  }

  return createHash()
    .update(PARENT_TYPE)
    .update(encodeUInt64(a.size + b.size))
    .update(a.hash)
    .update(b.hash)
    .digest()
}

exports.tree = function (roots, password) {
  var hash = (password ? createHmac(password) : createHash()).update(ROOT_TYPE)
  for (var i = 0; i < roots.length; i++) {
    var r = roots[i]
    hash.update(r.hash)
    hash.update(encodeUInt64(r.index))
    hash.update(encodeUInt64(r.size))
  }
  return hash.digest()
}

exports.discoveryKey = function (tree) {
  return CreateHmac('sha256', tree).update(HYPERCORE).digest()
}

function createHash () {
  return CreateHash('sha256')
}

function createHmac (password) {
  return CreateHmac('sha256', password)
}

function encodeUInt64 (n) {
  return uint64be.encode(n, tmp)
}
