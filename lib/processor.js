const Flushable = require('./flushable')
const Mutex = require('./mutex')

class Op {
  constructor (type, batch, index, values) {
    this.type = type
    this.batch = batch
    this.index = index
    this.values = values
  }

  async precommit (writer) {
    if (this.type !== Op.APPEND) return
    // TODO: Write data to disk
  }

  async postcommit (writer) {
    if (this.type === Op.APPEND) return
    //  TODO: Write values to disk
  }
}
Op.APPEND = 1
Op.PUT = 2
Op.TRUNCATE = 3
Op.UPGRADE = 4
Op.REMOTE_TRUNCATE = 5

module.exports = class Processor extends Flushable {
  constructor (core, oplog) {
    super()
    this.core = core
    this.oplog = oplog

    this._mutex = new Mutex()
    this._queue = []
    this._puts = null
    this._writer = null
  }

  async _flush () {
    if (!this._queue.length) return
    const queue = this._queue
    this._queue = []

    for (const op of this._queue) {
      await op.precommit(this._writer)
    }

    for (const op of this._queue) {
      await
    }

    console.log('queue:', queue)
    await new Promise(resolve => setTimeout(resolve, 1000))
  }

  async append (batch, values) {
    await this._mutex.lock()
    try {
      await this.flush()
      if (!batch.commitable()) return false
      this._queue.push(new Op(Op.APPEND, batch, batch.length, values)
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
      this._queue.push(new Op(Op.UPGRADE, batch, index, value ? [value] : null)
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
