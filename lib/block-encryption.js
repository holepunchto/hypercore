const sodium = require('sodium-universal')
const c = require('compact-encoding')
const b4a = require('b4a')

const nonce = b4a.alloc(sodium.crypto_stream_NONCEBYTES)

module.exports = exports = class BlockEncryption {
  constructor (opts = {}) {
    const {
      encryptionKey,
      nonce
    } = opts

    this.encryptionKey = encryptionKey
    this.nonce = nonce
  }

  get padding () {
    return 8
  }

  encrypt (index, block, id) {
    const padding = block.subarray(0, this.padding)
    block = block.subarray(this.padding)

    c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)

    nonce.fill(0, 8, 8 + padding.byteLength)

    this.nonce(id, padding, nonce)

    nonce.set(padding, 8)

    sodium.crypto_stream_xor(
      block,
      block,
      nonce,
      this.encryptionKey(id)
    )
  }

  decrypt (index, block) {
    const padding = block.subarray(0, this.padding)
    block = block.subarray(this.padding)

    const id = c.uint64.decode({ start: 0, end: 8, buffer: padding })

    c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)

    nonce.set(padding, 8)

    sodium.crypto_stream_xor(
      block,
      block,
      nonce,
      this.encryptionKey(id)
    )
  }
}

exports.defaultEncryption = function defaultEncryption (encryptionKey, hypercoreKey) {
  const subKeys = b4a.alloc(2 * sodium.crypto_stream_KEYBYTES)

  const blockKey = subKeys.subarray(0, sodium.crypto_stream_KEYBYTES)
  const blindingKey = subKeys.subarray(sodium.crypto_stream_KEYBYTES)

  sodium.crypto_generichash(blockKey, encryptionKey, hypercoreKey)
  sodium.crypto_generichash(blindingKey, blockKey)

  return {
    encryptionKey (id) {
      return blockKey
    },

    nonce (id, padding, nonce) {
      c.uint64.encode({ start: 0, end: 8, buffer: padding }, id)

      // Blind the fork ID, possibly risking reusing the nonce on a reorg of the
      // Hypercore. This is fine as the blinding is best-effort and the latest
      // fork ID shared on replication anyway.
      sodium.crypto_stream_xor(
        padding,
        padding,
        nonce,
        blindingKey
      )
    }
  }
}
