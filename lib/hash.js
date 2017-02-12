var sodium = require('sodium-native')
var uint64be = require('uint64be')

// https://en.wikipedia.org/wiki/Merkle_tree#Second_preimage_attack
var LEAF_TYPE = new Buffer([0])
var PARENT_TYPE = new Buffer([1])
var ROOT_TYPE = new Buffer([2])
var HYPERCORE = new Buffer('hypercore')

var tmp = new Buffer([0, 0, 0, 0, 0, 0, 0, 0])

exports.data = function (data) {
  return blake2b([
    LEAF_TYPE,
    encodeUInt64(data.length),
    data
  ])
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

  return blake2b([
    PARENT_TYPE,
    encodeUInt64(a.size + b.size),
    a.hash,
    b.hash
  ])
}

exports.tree = function (roots) {
  var buffers = new Array(3 * roots.length + 1)
  var j = 0

  buffers[j++] = ROOT_TYPE

  for (var i = 0; i < roots.length; i++) {
    var r = roots[i]
    buffers[j++] = r.hash
    buffers[j++] = encodeUInt64(r.index)
    buffers[j++] = encodeUInt64(r.size)
  }

  return blake2b(buffers)
}

exports.discoveryKey = function (tree) {
  var digest = new Buffer(32)
  sodium.crypto_generichash(digest, HYPERCORE, tree)
  return digest
}

function encodeUInt64 (n) {
  return uint64be.encode(n, tmp)
}

function blake2b (buffers) {
  var digest = new Buffer(32)
  sodium.crypto_generichash_batch(digest, buffers)
  return digest
}
