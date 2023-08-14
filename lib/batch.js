const { SESSION_NOT_WRITABLE, BATCH_ALREADY_FLUSHED, BLOCK_NOT_AVAILABLE } = require('hypercore-errors')
const EventEmitter = require('events')

module.exports = class HypercoreBatch extends EventEmitter {
  constructor (session) {
    super()

    this.session = session
    this.flushed = false
    this.opened = session.opened
    this.opening = null

    this._appends = []
    this._byteLength = 0
    this._fork = 0

    this.opening = this.ready().catch(noop)
  }

  get indexedLength () {
    return this.session.length
  }

  get indexedByteLength () {
    return this.session.byteLength
  }

  get length () {
    return this.session.length + this._appends.length
  }

  get byteLength () {
    return this.session.byteLength + this._byteLength
  }

  get writable () {
    return this.session.writable
  }

  async ready () {
    await this.session.ready()

    if (this.opened === false) {
      this.opened = true
      this.emit('ready')
    }
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

  async seek (bytes, opts) {
    if (this.opened === false) await this.opening

    if (bytes < this.session.byteLength) return await this.session.seek(bytes, opts)

    bytes -= this.session.byteLength

    let i = 0

    for (const blk of this._appends) {
      if (bytes < blk.byteLength) return [this.session.length + i, bytes]
      i++
      bytes -= blk.byteLength
    }

    if (bytes === 0) return [this.session.length + i, 0]

    throw BLOCK_NOT_AVAILABLE()
  }

  async get (index, opts) {
    if (this.opened === false) await this.opening

    const length = this.session.length
    if (index < length) return this.session.get(index, opts)

    return this._appends[index - length] || null
  }

  async truncate (newLength) {
    if (this.flushed) throw BATCH_ALREADY_FLUSHED()

    const session = this.session
    if (this.opened === false) await this.opening
    if (this.writable === false) throw SESSION_NOT_WRITABLE()

    const length = session.length
    if (newLength < length) throw new Error('Cannot truncate committed blocks')

    this._appends.length = newLength - length
    this._fork++

    this.emit('truncate', newLength, this.fork)
  }

  async append (blocks) {
    if (this.flushed) throw BATCH_ALREADY_FLUSHED()

    const session = this.session
    if (this.opened === false) await this.opening
    if (this.writable === false) throw SESSION_NOT_WRITABLE()

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

    this.emit('append')

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
    this._fork = 0
  }

  _clearBatch () {
    for (const session of this.session.sessions) session._batch = null
  }
}

function noop () {}
