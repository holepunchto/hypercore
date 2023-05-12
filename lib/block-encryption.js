const sodium = require('sodium-universal')
const c = require('compact-encoding')
const b4a = require('b4a')

const nonce = b4a.alloc(sodium.crypto_stream_NONCEBYTES)

module.exports = exports = class BlockEncryption {
  constructor (opts = {}) {
    const {
      key,
      nonce
    } = opts

    this.key = key
    this.nonce = nonce
  }

  get padding () {
    return 8
  }

  async encrypt (index, block, id) {
    const padding = block.subarray(0, this.padding)
    block = block.subarray(this.padding)

    c.uint64.encode({ start: 0, end: 8, buffer: padding }, id)
    c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)

    nonce.fill(0, 8, 8 + padding.byteLength)

    await this.nonce(id, padding, nonce)

    nonce.set(padding, 8)

    sodium.crypto_stream_xor(
      block,
      block,
      nonce,
      await this.key(id)
    )
  }

  async decrypt (index, block) {
    const padding = block.subarray(0, this.padding)
    block = block.subarray(this.padding)

    const id = c.uint64.decode({ start: 0, end: 8, buffer: padding })

    c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)

    nonce.set(padding, 8)

    sodium.crypto_stream_xor(
      block,
      block,
      nonce,
      await this.key(id)
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
    async key (id) {
      return blockKey
    },

    async nonce (id, padding, nonce) {
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
