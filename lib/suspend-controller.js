const { EventEmitter } = require('events')

class SuspendSignal extends EventEmitter {
  constructor() {
    super()

    this._suspended = false
    this.setMaxListeners(0) // one listener per core
  }

  get suspended() {
    return this._suspended
  }
}

/**
 * Controls replication suspension for one or more cores, like an
 * AbortController. The owner holds the controller and passes its signal to
 * cores via the `suspendSignal` option. While suspended, replication pauses
 * both ways but peers stay connected, and a core opened while suspended
 * comes up suspended. On resume, requests queued while suspended are served
 * and dropped data is re-requested. A core without a signal never suspends.
 */
module.exports = class SuspendController {
  static Signal = SuspendSignal

  constructor() {
    this.signal = new SuspendSignal()
  }

  get suspended() {
    return this.signal.suspended
  }

  suspend() {
    if (this.signal._suspended) return
    this.signal._suspended = true
    this.signal.emit('suspend')
  }

  resume() {
    if (!this.signal._suspended) return
    this.signal._suspended = false
    this.signal.emit('resume')
  }
}
