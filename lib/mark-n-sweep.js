const MarkBitfield = require('./mark-bitfield')

module.exports = class MarkNSweep {
  constructor(session) {
    this.session = session
    this._storage = session.state.storage
    this._marks = new MarkBitfield(this._storage)
  }

  async clear() {
    if (this.session.opened === false) await this.session.opening

    if (this._marks === null) {
      this._marks = new MarkBitfield(this._storage)
    }

    await this._marks.clear()
    this._marks = null
  }

  async sweep({ batchSize = 1000 } = {}) {
    if (this.session.opened === false) await this.session.opening

    let clearing = []
    let prevIndex = this.session.length
    for await (const index of this._marks.createMarkStream({ reverse: true })) {
      if (index + 1 === prevIndex) {
        prevIndex = index
        continue
      }
      clearing.push(this.session.clear(index + 1, prevIndex))
      if (clearing.length >= batchSize) {
        await Promise.all(clearing)
        clearing = []
      }
      prevIndex = index
    }
    // Clear range from the very start if not marked
    if (prevIndex > 0) clearing.push(this.session.clear(0, prevIndex))
    await Promise.all(clearing)

    this.session._marking = false
    await this.clear()
  }

  async mark(blockIndex) {
    if (this._marks === null) {
      this._marks = new MarkBitfield(this._storage)
    }

    return this._marks.set(blockIndex, true)
  }
}
