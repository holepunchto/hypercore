const { ASSERTION } = require('hypercore-errors')

module.exports = class ReadBatch {
  constructor(core) {
    this.core = core
    this.rx = core.state.storage.read()

    this.reads = new Map()
    this.index = -1

    this.destroyed = false
  }

  async destroy() {
    this.core._removeReadBatch(this)
    this.destroyed = true
    this.rx.destroy()
  }

  async get(index, opts = {}) {
    if (!isValidIndex(index)) throw ASSERTION('block index is invalid', this.discoveryKey)

    if (this.core.onseq !== null) this.core.onseq(index, this.core)

    const block = await this._get(index)
    if (this.destroyed) return null

    if (block) return this.core._handleBlock(index, block, opts)

    return this.core.get(index, opts)
  }

  _get(index) {
    if (this.reads.has(index)) return this.reads.get(index)

    const promise = this.rx.getBlock(index)
    this.reads.set(index, promise)

    return promise
  }

  tryFlush() {
    this.rx.tryFlush()
    this.core._removeReadBatch(this)
  }
}

function isValidIndex(index) {
  return index === 0 || index > 0
}
