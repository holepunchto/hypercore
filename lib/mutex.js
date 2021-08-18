module.exports = class Mutex {
  constructor () {
    this.locked = false
    this.destroyed = false

    this._resolveDrained = null
    this._eagerDestroy = false
    this._destroying = null
    this._err = null

    this._queue = []

    this._enqueue = (resolve, reject) => this._queue.push([resolve, reject])
    // Will only be called once in destroy
    this._waitDrained = resolve => {
      this._resolveDrained = resolve
    }
  }

  lock () {
    if (this.destroyed) return Promise.reject(new Error('Mutex has been destroyed'))
    if (this.locked) return new Promise(this._enqueue)
    this.locked = true
    return Promise.resolve()
  }

  unlock () {
    if (!this._queue.length) {
      this.locked = false
      if (this._resolveDrained) {
        this._resolveDrained()
        this._resolveDrained = null
      }
      return
    }
    const [resolve, reject] = this._queue.shift()
    if (!this._err) resolve()
    else reject(this._err)
  }

  destroy (err) {
    if (!this._queue.length && !this.locked) {
      this.destroyed = true
      return Promise.resolve()
    }
    if (err && !this._eagerDestroy) {
      this._eagerDestroy = true
      while (this._queue.length) {
        this._queue.shift()[1](err)
      }
    }
    if (this._destroying) return this._destroying
    this.destroyed = true
    this._destroying = new Promise(this._waitDrained)
    return this._destroying
  }
}
