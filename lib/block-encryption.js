const sodium = require('sodium-universal')
const c = require('compact-encoding')
const b4a = require('b4a')

const nonce = b4a.alloc(sodium.crypto_stream_NONCEBYTES)

module.exports = exports = class BlockEncryption {
  constructor (opts = {}) {
    const {
      id,
      key
    } = opts

    this.id = id
    this.key = key
  }

  get padding () {
    return 8
  }

  async encrypt (index, block, core) {
    const id = block.subarray(0, this.padding)
    block = block.subarray(this.padding)

    c.uint64.encode(c.state(0, 8, nonce), index)

    this.id(index, core, id)

    nonce.set(id, 8)

    sodium.crypto_stream_xor(
      block,
      block,
      nonce,
      await this.key(id)
    )
  }

  async decrypt (index, block) {
    const id = block.subarray(0, this.padding)
    block = block.subarray(this.padding)

    c.uint64.encode(c.state(0, 8, nonce), index)

    nonce.set(id, 8)

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
    id (index, core, id) {
      c.uint64.encode(c.state(0, 8, id), core.fork)

      // Zero out any previous fork ID.
      nonce.fill(0, 8, 8 + id.byteLength)

      // Blind the fork ID, possibly risking reusing the nonce on a reorg of the
      // Hypercore. This is fine as the blinding is best-effort and the latest
      // fork ID shared on replication anyway.
      sodium.crypto_stream_xor(
        id,
        id,
        nonce,
        blindingKey
      )
    },

    async key (id) {
      return blockKey
    }
  }
}
