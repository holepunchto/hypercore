const sodium = require('sodium-universal')
const c = require('compact-encoding')
const b4a = require('b4a')
const { BLOCK_ENCRYPTION } = require('./caps')

const nonce = b4a.alloc(sodium.crypto_stream_NONCEBYTES)

module.exports = class BlockEncryption {
  constructor (encryptionKey, hypercoreKey, { isBlockKey = false, compat = true } = {}) {
    const subKeys = b4a.alloc(2 * sodium.crypto_stream_KEYBYTES)

    this.key = encryptionKey
    this.blockKey = isBlockKey ? encryptionKey : subKeys.subarray(0, sodium.crypto_stream_KEYBYTES)
    this.blindingKey = subKeys.subarray(sodium.crypto_stream_KEYBYTES)
    this.padding = 8
    this.compat = compat
    this.isBlockKey = isBlockKey

    if (!isBlockKey) {
      if (compat) sodium.crypto_generichash_batch(this.blockKey, [encryptionKey], hypercoreKey)
      else sodium.crypto_generichash_batch(this.blockKey, [BLOCK_ENCRYPTION, hypercoreKey, encryptionKey])
    }

    sodium.crypto_generichash(this.blindingKey, this.blockKey)
  }

  encrypt (index, block, fork) {
    const padding = block.subarray(0, this.padding)
    block = block.subarray(this.padding)

    c.uint64.encode({ start: 0, end: 8, buffer: padding }, fork)
    c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)

    // Zero out any previous padding.
    nonce.fill(0, 8, 8 + padding.byteLength)

    // Blind the fork ID, possibly risking reusing the nonce on a reorg of the
    // Hypercore. This is fine as the blinding is best-effort and the latest
    // fork ID shared on replication anyway.
    sodium.crypto_stream_xor(
      padding,
      padding,
      nonce,
      this.blindingKey
    )

    nonce.set(padding, 8)

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

    c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)

    nonce.set(padding, 8)

    // Decrypt the block using the blinded fork ID.
    sodium.crypto_stream_xor(
      block,
      block,
      nonce,
      this.blockKey
    )
  }
}
