var sodium = require('sodium-universal')
var uint64be = require('uint64be')

// https://en.wikipedia.org/wiki/Merkle_tree#Second_preimage_attack
var LEAF_TYPE = new Buffer([0])
var PARENT_TYPE = new Buffer([1])
var ROOT_TYPE = new Buffer([2])
var HYPERCORE = new Buffer('hypercore')

exports.keyPair = function (seed) {
  var publicKey = new Buffer(sodium.crypto_sign_PUBLICKEYBYTES)
  var secretKey = new Buffer(sodium.crypto_sign_SECRETKEYBYTES)

  if (seed) sodium.crypto_sign_seed_keypair(publicKey, secretKey, seed)
  else sodium.crypto_sign_keypair(publicKey, secretKey)

  return {
    publicKey: publicKey,
    secretKey: secretKey
  }
}

exports.sign = function (message, secretKey) {
  var signature = new Buffer(sodium.crypto_sign_BYTES)
  sodium.crypto_sign_detached(signature, message, secretKey)
  return signature
}

exports.verify = function (message, signature, publicKey) {
  return sodium.crypto_sign_verify_detached(signature, message, publicKey)
}

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

exports.randomBytes = function (n) {
  var buf = new Buffer(n)
  sodium.randombytes_buf(buf)
  return buf
}

exports.discoveryKey = function (tree) {
  var digest = new Buffer(32)
  sodium.crypto_generichash(digest, HYPERCORE, tree)
  return digest
}

function encodeUInt64 (n) {
  return uint64be.encode(n, new Buffer(8))
}

function blake2b (buffers) {
  var digest = new Buffer(32)
  sodium.crypto_generichash_batch(digest, buffers)
  return digest
}
