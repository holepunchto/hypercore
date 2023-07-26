const { SESSION_NOT_WRITABLE, BATCH_ALREADY_FLUSHED } = require('hypercore-errors')

module.exports = class Batch {
  constructor (session) {
    this.session = session
    this.flushed = false

    this._appends = []
    this._byteLength = 0
  }

  get length () {
    return this.session.length + this._appends.length
  }

  ready () {
    return this.session.ready()
  }

  async info (opts) {
    const session = this.session
    const info = await session.info(opts)

    if (info.contiguousLength === info.length) {
      info.contiguousLength = info.length += this._appends.length
    } else {
      info.length += this._appends.length
    }

    info.byteLength += this._byteLength

    return info
  }

  async get (index, opts) {
    const session = this.session
    if (session.opened === false) await session.opening

    const length = this.session.length
    if (index < length) return this.session.get(index, opts)

    return this._appends[index - length] || null
  }

  async truncate (newLength) {
    if (this.flushed) throw BATCH_ALREADY_FLUSHED()

    const session = this.session
    if (session.opened === false) await session.opening
    if (session.writable === false) throw SESSION_NOT_WRITABLE()

    const length = session.length
    if (newLength < length) throw new Error('Cannot truncate committed blocks')

    this._appends.length = newLength - length
  }

  async append (blocks) {
    if (this.flushed) throw BATCH_ALREADY_FLUSHED()

    const session = this.session
    if (session.opened === false) await session.opening
    if (session.writable === false) throw SESSION_NOT_WRITABLE()

    blocks = Array.isArray(blocks) ? blocks : [blocks]

    const buffers = session.encodeBatch !== null
      ? session.encodeBatch(blocks)
      : new Array(blocks.length)

    if (session.encodeBatch === null) {
      for (let i = 0; i < blocks.length; i++) {
        const buffer = session._encode(session.valueEncoding, blocks[i])
        buffers[i] = buffer
        this._byteLength += buffer.byteLength
      }
    }

    this._appends.push(...buffers)

    const byteLength = session.byteLength + this._byteLength

    return { length: this.length, byteLength }
  }

  async flush () {
    if (this.flushed) throw BATCH_ALREADY_FLUSHED()
    this.flushed = true

    try {
      if (this._appends.length) await this.session.append(this._appends)
    } finally {
      this._clearBatch()
      this._clearAppends()
    }
  }

  async close () {
    if (this.flushed) throw BATCH_ALREADY_FLUSHED()
    this.flushed = true

    this._clearBatch()
    this._clearAppends()
  }

  _clearAppends () {
    this._appends = []
    this._byteLength = 0
  }

  _clearBatch () {
    for (const session of this.session.sessions) session._batch = null
  }
}
