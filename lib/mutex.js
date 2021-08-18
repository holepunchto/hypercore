module.exports = class Mutex {
  constructor () {
    this.locked = false
    this.destroyed = false

    this._destroying = null
    this._destroyError = null
    this._queue = []
    this._enqueue = (resolve, reject) => this._queue.push([resolve, reject])
  }

  lock () {
    if (this.destroyed) return Promise.reject(this._destroyError)
    if (this.locked) return new Promise(this._enqueue)
    this.locked = true
    return Promise.resolve()
  }

  unlock () {
    if (!this._queue.length) {
      this.locked = false
      return
    }
    this._queue.shift()[0]()
  }

  destroy (err) {
    if (!this._destroying) this._destroying = this.locked ? this.lock().catch(() => {}) : Promise.resolve()

    this.destroyed = true
    this._destroyError = err || new Error('Mutex has been destroyed')

    if (err) {
      while (this._queue.length) this._queue.shift()[1](err)
    }

    return this._destroying
  }
}
