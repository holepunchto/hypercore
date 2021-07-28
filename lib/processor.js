const Flushable = require('./flushable')
const Mutex = require('./mutex')

class Op {
  constructor (type, batch, index, values) {
    this.type = type
    this.batch = batch
    this.index = index
    this.values = values

    this.mutation = {
      type: this.type,
      fork: this.batch.fork,
      length: this.batch.length,
      nodes: this.batch.nodes,
      startBlock: this.index,
      signature: this.batch.signature
    }
  }
}
Op.APPEND = 1
Op.PUT = 2
Op.TRUNCATE = 3
Op.UPGRADE = 4
Op.REMOTE_TRUNCATE = 5

module.exports = class Processor extends Flushable {
  constructor (core, opts = {}) {
    super()
    this.oplog = core.oplog
    this.bitfield = core.bitfield
    this.blocks = core.blocks
    this.tree = core.tree

    this._onflush = opts.onflush || noop

    this._mutex = new Mutex()
    this._queue = []
    this._puts = null
  }

  async _flush () {
    if (!this._queue.length) return
    const queue = this._queue
    this._queue = []

    for (const op of queue) {
      console.log('op.values:', op.values)
      if (op.values) {
        const byteOffset = await op.batch.byteOffset(op.index * 2)
        // TODO: Need a better abstraction for this (in the block store)
        await this.blocks._write(byteOffset, op.values.length > 1 ? Buffer.concat(op.values) : op.values[0])
      }
    }

    await this.oplog.append(queue)

    for (const op of queue) {
      op.batch.commit()
      if (op.values) {
        for (let i = 0; i < op.values.length; i++) {
          this.bitfield.set(op.index + i, true)
        }
      }
    }

    this.oplog.fork = tree.fork
    this.oplog.length = tree.length
    this.oplog.signature = tree.signature

    const digest = {
      fork:
    }
  }

  async append (batch, values) {
    await this._mutex.lock()
    try {
      await this.flush()
      if (!batch.commitable()) return false
      this._queue.push(new Op(Op.APPEND, batch, batch.length, values))
      await this.flush()
      return true
    } finally {
      this._mutex.unlock()
    }
  }

  async put (batch, index, value) {
    await this._mutex.unlocked()
    if (!batch.commitable()) return false
    this._queue.push(new Op(Op.PUT, batch, index, value ? [value] : null))
    await this.flush()
    return true
  }

  async truncate (batch) {
    await this._mutex.lock()
    try {
      await this.flush()
      if (!batch.commitable()) return false
      this._queue.push(new Op(Op.TRUNCATE, batch, 0, []))
      await this.flush()
      return true
    } finally {
      this._mutex.unlock()
    }
  }

  async upgrade (batch, index, value) {
    await this._mutex.lock()
    try {
      await this.flush()
      if (!batch.commitable()) return false
      this._queue.push(new Op(Op.UPGRADE, batch, index, value ? [value] : null))
      await this.flush()
      return true
    } finally {
      this._mutex.unlock()
    }
  }

  async remoteTruncate (batch) {
    await this._mutex.lock()
    try {
      await this.flush()
      if (!batch.commitable()) return false
      this._queue.push(new Op(Op.REMOTE_TRUNCATE, batch, 0, []))
      await this.flush()
      return true
    } finally {
      this._mutex.unlock()
    }
  }
}

function noop () {}
