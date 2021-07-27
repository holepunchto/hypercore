module.exports = class Mutex {
  constructor () {
    this.locked = false
    this._unlocked = null
    this._queue = []
    this._enqueue = resolve => this._queue.push(resolve)
    this._onunlock = () => {
      this._unlocked = null
      this.unlock()
    }
  }

  unlocked () {
    if (this._unlocked) return this._unlocked
    this._unlocked = this.lock().then(this._onunlock)

    return this._unlocked
  }

  lock () {
    this._unlocked = null
    if (this.locked) return new Promise(this._enqueue)
    this.locked = true
    return Promise.resolve()
  }

  unlock () {
    if (!this._queue.length) {
      this.locked = false
      return
    }
    this._queue.shift()()
  }
}
