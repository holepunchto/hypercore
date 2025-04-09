const crypto = require('hypercore-crypto')
const sodium = require('sodium-universal')
const b4a = require('b4a')
const c = require('compact-encoding')

// TODO: rename this to "crypto" and move everything hashing related etc in here
// Also lets move the tree stuff from hypercore-crypto here

const [
  TREE,
  REPLICATE_INITIATOR,
  REPLICATE_RESPONDER,
  MANIFEST,
  DEFAULT_NAMESPACE,
  DEFAULT_ENCRYPTION
] = crypto.namespace('hypercore', 6)

exports.MANIFEST = MANIFEST
exports.DEFAULT_NAMESPACE = DEFAULT_NAMESPACE
exports.DEFAULT_ENCRYPTION = DEFAULT_ENCRYPTION

exports.replicate = function (isInitiator, key, handshakeHash) {
  const out = b4a.allocUnsafe(32)
  sodium.crypto_generichash_batch(out, [isInitiator ? REPLICATE_INITIATOR : REPLICATE_RESPONDER, key], handshakeHash)
  return out
}

exports.treeSignable = function (manifestHash, treeHash, length, fork) {
  const state = { start: 0, end: 112, buffer: b4a.allocUnsafe(112) }
  c.fixed32.encode(state, TREE)
  c.fixed32.encode(state, manifestHash)
  c.fixed32.encode(state, treeHash)
  c.uint64.encode(state, length)
  c.uint64.encode(state, fork)
  return state.buffer
}

exports.treeSignableCompat = function (hash, length, fork, noHeader) {
  const end = noHeader ? 48 : 80
  const state = { start: 0, end, buffer: b4a.allocUnsafe(end) }
  if (!noHeader) c.fixed32.encode(state, TREE) // ultra legacy mode, kill in future major
  c.fixed32.encode(state, hash)
  c.uint64.encode(state, length)
  c.uint64.encode(state, fork)
  return state.buffer
}
