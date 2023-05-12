const sodium = require('sodium-universal')
const c = require('compact-encoding')
const b4a = require('b4a')

const nonce = b4a.alloc(sodium.crypto_stream_NONCEBYTES)

module.exports = class BlockEncryption {
  constructor (encryptionKey, hypercoreKey) {
    const subKeys = b4a.alloc(2 * sodium.crypto_stream_KEYBYTES)

    this.key = encryptionKey
    this.blockKey = subKeys.subarray(0, sodium.crypto_stream_KEYBYTES)
    this.blindingKey = subKeys.subarray(sodium.crypto_stream_KEYBYTES)
    this.padding = 16

    sodium.crypto_generichash(this.blockKey, encryptionKey, hypercoreKey)
    sodium.crypto_generichash(this.blindingKey, this.blockKey)
  }

  encrypt (index, block, fork, key = 0) {
    const padding = block.subarray(0, this.padding)
    block = block.subarray(this.padding)

    c.uint64.encode({ start: 0, end: 8, buffer: padding }, key)
    c.uint64.encode({ start: 8, end: 16, buffer: padding }, fork)
    c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)

    // Zero out any previous fork ID.
    nonce.fill(0, 8, 16)

    // Blind the fork ID, possibly risking reusing the nonce on a reorg of the
    // Hypercore. This is fine as the blinding is best-effort and the latest
    // fork ID shared on replication anyway.
    sodium.crypto_stream_xor(
      padding.subarray(8, 16),
      padding.subarray(8, 16),
      nonce,
      this.blindingKey
    )

    nonce.set(padding.subarray(8, 16), 8)

    // The combination of a (blinded) fork ID and a block index is unique for a
    // given Hypercore and is therefore a valid nonce for encrypting the block.
    sodium.crypto_stream_xor(
      block,
      block,
      nonce,
      this.blockKey
    )
  }

  decrypt (index, block) {
    const padding = block.subarray(0, this.padding)
    block = block.subarray(this.padding)

    // TODO: Pick block key based on this
    const key = c.uint64.decode({ start: 0, end: 8, buffer: padding })

    c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)

    nonce.set(padding.subarray(8, 16), 8)

    // Decrypt the block using the blinded fork ID.
    sodium.crypto_stream_xor(
      block,
      block,
      nonce,
      this.blockKey
    )
  }
}
