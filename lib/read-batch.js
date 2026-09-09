const { ASSERTION } = require('hypercore-errors')

module.exports = class ReadBatch {
  constructor(core) {
    this.core = core
    this.rx = core.state.storage.read()

    this.index = -1

    this.destroyed = false
  }

  async destroy() {
    if (this.destroyed) return
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
    return this.rx.getBlock(index)
  }

  tryFlush() {
    this.rx.tryFlush()
    this.core._removeReadBatch(this)
  }
}

function isValidIndex(index) {
  return index === 0 || index > 0
}
