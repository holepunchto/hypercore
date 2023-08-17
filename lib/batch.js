const { SESSION_NOT_WRITABLE, BLOCK_NOT_AVAILABLE, SESSION_CLOSED } = require('hypercore-errors')
const EventEmitter = require('events')
const c = require('compact-encoding')

module.exports = class HypercoreBatch extends EventEmitter {
  constructor (session, autoClose) {
    super()

    this.session = session
    this.opened = false
    this.closed = false
    this.opening = null
    this.closing = null
    this.autoClose = autoClose

    this._appends = []
    this._byteLength = 0
    this._fork = 0
    this._sessionLength = 0
    this._sessionByteLength = 0
    this._flushing = null

    this.opening = this.ready().catch(noop)
  }

  get id () {
    return this.session.id
  }

  get key () {
    return this.session.key
  }

  get discoveryKey () {
    return this.session.discoveryKey
  }

  get indexedLength () {
    return this._sessionLength
  }

  get indexedByteLength () {
    return this._sessionByteLength
  }

  get length () {
    return this._sessionLength + this._appends.length
  }

  get byteLength () {
    return this._sessionByteLength + this._byteLength
  }

  get writable () {
    return this.session.writable
  }

  get core () {
    return this.session.core
  }

  async ready () {
    await this.session.ready()
    if (this.opened) return
    this._sessionLength = this.session.length
    this._sessionByteLength = this.session.byteLength
    this.opened = true
    this.emit('ready')
  }

  async update (opts) {
    if (this.opened === false) await this.ready()
    await this.session.update(opts)
  }

  setUserData (key, value, opts) {
    return this.session.setUserData(key, value, opts)
  }

  getUserData (key, opts) {
    return this.session.getUserData(key, opts)
  }

  async info (opts) {
    const session = this.session
    const info = await session.info(opts)

    info.length = this._sessionLength

    if (info.contiguousLength >= info.length) {
      info.contiguousLength = info.length += this._appends.length
    } else {
      info.length += this._appends.length
    }

    info.byteLength = this._sessionByteLength + this._byteLength

    return info
  }

  async seek (bytes, opts) {
    if (this.opened === false) await this.opening
    if (this.closing) throw SESSION_CLOSED()

    if (bytes < this._sessionByteLength) return await this.session.seek(bytes, opts)

    bytes -= this._sessionByteLength

    let i = 0

    for (const blk of this._appends) {
      if (bytes < blk.byteLength) return [this._sessionLength + i, bytes]
      i++
      bytes -= blk.byteLength
    }

    if (bytes === 0) return [this._sessionLength + i, 0]

    throw BLOCK_NOT_AVAILABLE()
  }

  async get (index, opts) {
    if (this.opened === false) await this.opening
    if (this.closing) throw SESSION_CLOSED()

    const length = this._sessionLength
    if (index < length) return this.session.get(index, opts)

    const buffer = this._appends[index - length] || null
    if (!buffer) throw BLOCK_NOT_AVAILABLE()

    const encoding = (opts && opts.valueEncoding && c.from(opts.valueEncoding)) || this.session.valueEncoding
    if (!encoding) return buffer

    return c.decode(encoding, buffer)
  }

  async _waitForFlush () {
    // wait for any pending flush...
    while (this._flushing) {
      await this._flushing
      await Promise.resolve() // yield in case a new flush is queued
    }
  }

  createTreeBatch (length, blocks = []) {
    if (!length && length !== 0) length = this.length + blocks.length

    const maxLength = this.length + blocks.length
    const b = this.session.createTreeBatch()
    const len = Math.min(length, this.length)

    if (len < this._sessionLength || length > maxLength) return null

    for (let i = 0; i < len - this._sessionLength; i++) {
      b.append(this._appends[i])
    }

    if (len < this.length) return b

    for (let i = 0; i < length - len; i++) {
      b.append(blocks[i])
    }

    return b
  }

  async truncate (newLength) {
    if (this.opened === false) await this.opening
    if (this.writable === false) throw SESSION_NOT_WRITABLE()
    if (this.closing) throw SESSION_CLOSED()

    // wait for any pending flush... (prop needs a lock)
    await this._waitForFlush()

    const length = this._sessionLength
    if (newLength < length) throw new Error('Cannot truncate committed blocks')

    this._appends.length = newLength - length
    this._fork++

    this.emit('truncate', newLength, this.fork)
  }

  async append (blocks) {
    const session = this.session

    if (this.opened === false) await this.opening
    if (this.writable === false) throw SESSION_NOT_WRITABLE()
    if (this.closing) throw SESSION_CLOSED()

    // wait for any pending flush... (prop needs a lock)
    await this._waitForFlush()

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

    const info = { length: this.length, byteLength: this.byteLength }
    this.emit('append')

    return info
  }

  async flush (length = this._appends.length, auth) {
    if (this.opened === false) await this.opening
    if (this.closing) throw SESSION_CLOSED()

    while (this._flushing) await this._flushing
    this._flushing = this._flush(length, auth)

    try {
      await this._flushing
    } finally {
      this._flushing = null
    }

    if (this.autoClose) await this.close()
  }

  async _flush (length, auth) { // TODO: make this safe to interact with a parallel truncate...
    if (this._appends.length === 0) return

    const flushingLength = Math.min(length, this._appends.length)
    const info = await this.session.append(flushingLength < this._appends.length ? this._appends.slice(0, flushingLength) : this._appends, { auth })
    const delta = info.byteLength - this._sessionByteLength

    this._sessionLength = info.length
    this._sessionByteLength = info.byteLength
    this._appends = this._appends.slice(flushingLength)
    this._byteLength -= delta

    this.emit('flush')
  }

  close () {
    if (!this.closing) this.closing = this._close()
    return this.closing
  }

  async _close () {
    this._clearBatch()
    this._clearAppends()

    await this.session.close()

    this.closed = true
    this.emit('close')
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
