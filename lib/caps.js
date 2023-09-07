const crypto = require('hypercore-crypto')
const sodium = require('sodium-universal')
const b4a = require('b4a')
const c = require('compact-encoding')

// TODO: rename this to "crypto" and move everything hashing related etc in here
// Also lets move the tree stuff from hypercore-crypto here

const [TREE, REPLICATE_INITIATOR, REPLICATE_RESPONDER, MANIFEST, DEFAULT_NAMESPACE] = crypto.namespace('hypercore', 5)

exports.MANIFEST = MANIFEST
exports.DEFAULT_NAMESPACE = DEFAULT_NAMESPACE

exports.replicate = function (isInitiator, key, handshakeHash) {
  const out = b4a.allocUnsafe(32)
  sodium.crypto_generichash_batch(out, [isInitiator ? REPLICATE_INITIATOR : REPLICATE_RESPONDER, key], handshakeHash)
  return out
}

exports.treeSignable = function (namespace, hash, length, fork) {
  const state = { start: 0, end: 112, buffer: b4a.allocUnsafe(112) }
  c.raw.encode(state, TREE)
  c.raw.encode(state, namespace)
  c.raw.encode(state, hash)
  c.uint64.encode(state, length)
  c.uint64.encode(state, fork)
  return state.buffer
}

exports.treeSignableCompat = function (hash, length, fork, noHeader) {
  const end = noHeader ? 48 : 80
  const state = { start: 0, end, buffer: b4a.allocUnsafe(end) }
  if (!noHeader) c.raw.encode(state, TREE) // ultra legacy mode, kill in future major
  c.raw.encode(state, hash)
  c.uint64.encode(state, length)
  c.uint64.encode(state, fork)
  return state.buffer
}
