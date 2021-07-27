module.exports = class Flushable {
  constructor () {
    this._flushing = null
  }

  async _flush () {
    // impl me
  }

  async flush () {
    if (this._flushing !== null) {
      try {
        await this._flushing
      } catch (_) {
        // ignore - do not fail on old errors
      }
    }

    // another "thread" beat us to it, just piggy pack on that one
    if (this._flushing !== null) return this._flushing

    this._flushing = this._flush()

    try {
      return await this._flushing
    } finally {
      this._flushing = null
    }
  }
}
