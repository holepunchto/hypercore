const crypto = require('hypercore-crypto')
const sodium = require('sodium-universal')
const b4a = require('b4a')
const c = require('compact-encoding')
const m = require('./messages')

// TODO: rename this to "crypto" and move everything hashing related etc in here
// Also lets move the tree stuff from hypercore-crypto here

const [TREE, REPLICATE_INITIATOR, REPLICATE_RESPONDER, MANIFEST, DEFAULT_NAMESPACE] = crypto.namespace('hypercore', 5)

exports.DEFAULT_NAMESPACE = DEFAULT_NAMESPACE

exports.replicate = function (isInitiator, key, handshakeHash) {
  const out = b4a.allocUnsafe(32)
  sodium.crypto_generichash_batch(out, [isInitiator ? REPLICATE_INITIATOR : REPLICATE_RESPONDER, key], handshakeHash)
  return out
}

exports.manifestHash = function (manifest) {
  const state = { start: 0, end: 32, buffer: null }
  m.manifest.preencode(state, manifest)
  state.buffer = b4a.allocUnsafe(state.end)
  c.raw.encode(state, MANIFEST)
  m.manifest.encode(state, manifest)
  const out = b4a.allocUnsafe(32)
  sodium.crypto_generichash(out, state.buffer)
  return out
}

exports.treeSignable = function (hash, length, fork) {
  const state = { start: 0, end: 80, buffer: b4a.allocUnsafe(80) }
  c.raw.encode(state, TREE)
  c.raw.encode(state, hash)
  c.uint64.encode(state, length)
  c.uint64.encode(state, fork)
  return state.buffer
}

exports.treeSignableLegacy = function (hash, length, fork) {
  const state = { start: 0, end: 48, buffer: b4a.allocUnsafe(48) }
  c.raw.encode(state, hash)
  c.uint64.encode(state, length)
  c.uint64.encode(state, fork)
  return state.buffer
}
