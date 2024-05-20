const FIFO = require('fast-fifo')

module.exports = class ReceiverQueue {
  constructor () {
    this.queue = new FIFO()
    this.priority = []
    this.requests = new Map()
    this.length = 0
  }

  push (req) {
    // TODO: use a heap at some point if we wanna support multiple prios
    if (req.priority > 0) this.priority.push(req)
    else this.queue.push(req)

    this.requests.set(req.id, req)
    this.length++
  }

  shift () {
    while (this.priority.length > 0) {
      const msg = this.priority.pop()
      const req = this._processRequest(msg)
      if (req !== null) return req
    }

    while (this.queue.length > 0) {
      const msg = this.queue.shift()
      const req = this._processRequest(msg)
      if (req !== null) return req
    }

    return null
  }

  _processRequest (req) {
    if (req.block || req.hash || req.seek || req.upgrade || req.manifest) {
      this.requests.delete(req.id)
      this.length--
      return req
    }

    return null
  }

  clear () {
    this.queue.clear()
    this.priority = []
    this.length = 0
    this.requests.clear()
  }

  delete (id) {
    const req = this.requests.get(id)
    if (!req) return

    req.block = null
    req.hash = null
    req.seek = null
    req.upgrade = null
    req.manifest = false

    this.requests.delete(id)
    this.length--

    if (this.length === 0) {
      this.queue.clear()
      this.priority = []
    }
  }
}
