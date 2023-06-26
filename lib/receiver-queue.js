const FIFO = require('fast-fifo')

module.exports = class ReceiverQueue {
  constructor () {
    this.queue = new FIFO()
    this.requests = new Map()
    this.length = 0
  }

  push (req) {
    this.queue.push(req)
    this.requests.set(req.id, req)
    this.length++
  }

  shift () {
    while (this.length > 0) {
      const req = this.queue.shift()

      if (req.block || req.hash || req.seek || req.upgrade) {
        this.requests.delete(req.id)
        this.length--
        return req
      }
    }

    return null
  }

  delete (id) {
    const req = this.requests.get(id)
    if (!req) return

    req.block = null
    req.hash = null
    req.seek = null
    req.upgrade = null

    this.requests.delete(id)
    this.length--

    if (this.length === 0) this.queue.clear()
  }
}
