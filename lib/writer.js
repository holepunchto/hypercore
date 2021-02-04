const mutexify = require('mutexify/promise')

module.exports = class Writer {
  constructor (secretKey, core) {
    this.secretKey = secretKey
    this.core = core
    this.lock = mutexify()

    // shorthand these core caps to make the below code less verbose
    this.bitfield = core.bitfield
    this.blocks = core.blocks
    this.info = core.info
    this.tree = core.tree
    this.replicator = core.replicator
    this.crypto = core.crypto
    this.valueEncoding = core.valueEncoding
  }

  sign (signable) {
    return this.crypto.sign(signable, this.info.secretKey)
  }

  async truncate (newLength, fork = -1) {
    const release = await this.lock()

    let oldLength = 0

    try {
      if (fork === -1) fork = this.info.fork + 1

      const batch = await this.tree.truncate(newLength, { fork })

      const signature = await this.sign(batch.signable())

      this.info.fork = fork
      this.info.signature = signature

      oldLength = this.tree.length

      // TODO: same thing as in append
      await this.info.flush()

      for (let i = newLength; i < oldLength; i++) {
        this.bitfield.set(i, false)
      }
      batch.commit()
    } finally {
      release()
    }

    this.core.ontruncate(oldLength, newLength)
    this.replicator.broadcastInfo()

    // Same note about background processing as below in append
    await this.tree.flush()
    await this.bitfield.flush()
  }

  async append (blocks) {
    const release = await this.lock()

    let oldLength = 0
    let newLength = 0

    try {
      const batch = this.tree.batch()
      const buffers = new Array(blocks.length)

      for (let i = 0; i < blocks.length; i++) {
        const blk = blocks[i]

        const buf = Buffer.isBuffer(blk)
          ? blk
          : this.valueEncoding
            ? this.valueEncoding.encode(blk)
            : Buffer.from(blk)

        buffers[i] = buf
        batch.append(buf)
      }

      // write the blocks, if this fails, we'll just overwrite them later
      await this.blocks.putBatch(this.tree.length, buffers)

      const signature = await this.sign(batch.signable())

      // TODO: needs to written first, then updated
      this.info.signature = signature

      oldLength = this.tree.length
      newLength = oldLength + buffers.length

      // TODO: atomically persist that we wanna write these blocks now
      // to the info file, so we can recover if the post-commit stuff fails
      await this.info.flush()

      for (let i = oldLength; i < newLength; i++) {
        this.bitfield.set(i, true)
      }
      batch.commit()
    } finally {
      release()
    }

    this.core.onappend(oldLength, newLength)

    // TODO: all these broadcasts should be one
    this.replicator.broadcastInfo()
    for (let i = oldLength; i < newLength; i++) {
      this.replicator.broadcastBlock(i)
    }

    // technically we could run these in the background for more perf
    // as soon as we persist more stuff to the info as the writes here
    // are recoverable from the above blocks

    await this.tree.flush()
    await this.bitfield.flush()

    return oldLength
  }
}
