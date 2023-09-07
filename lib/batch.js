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
    this.fork = 0

    this.batch = null

    this._appends = []
    this._byteLength = 0
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

  get manifest () {
    return this.session.manifest
  }

  async ready () {
    await this.session.ready()
    if (this.opened) return
    this._sessionLength = this.session.length
    this._sessionByteLength = this.session.byteLength
    this.fork = this.session.fork
    this.treeBatch = this.session.core.tree.batch()

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
    const b = this.treeBatch.clone()
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

  async truncate (newLength = 0, opts = {}) {
    if (this.opened === false) await this.opening
    if (this.writable === false) throw SESSION_NOT_WRITABLE()
    if (this.closing) throw SESSION_CLOSED()

    // wait for any pending flush... (prop needs a lock)
    await this._waitForFlush()

    const { fork = this.fork + 1, force = false } = typeof opts === 'number' ? { fork: opts } : opts

    const length = this._sessionLength
    if (newLength < length) {
      if (!force) throw new Error('Cannot truncate committed blocks')
      this._appends.length = 0
      this._byteLength = 0
      await this.session.truncate(newLength, { fork, force: true })
      this._sessionLength = this.session.length
      this._sessionByteLength = this.session.byteLength
    } else {
      for (let i = newLength - length; i < this._appends.length; i++) this._byteLength -= this._appends[i].byteLength
      this._appends.length = newLength - length
    }

    this.fork = fork

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

  async flush (opts = {}) {
    if (this.opened === false) await this.opening
    if (this.closing) throw SESSION_CLOSED()

    const { length = this._appends.length, keyPair = this.session.keyPair, signature = null } = opts

    while (this._flushing) await this._flushing
    this._flushing = this._flush(length, keyPair, signature)

    let updated = false
    try {
      updated = await this._flushing
    } finally {
      this._flushing = null
    }

    if (this.autoClose) await this.close()
    return updated
  }

  async _flush (length, keyPair, signature) { // TODO: make this safe to interact with a parallel truncate...
    if (this._appends.length === 0) return false

    let flushingLength = Math.min(length, this._appends.length)
    const expectedLength = this._sessionLength + flushingLength

    const values = this._appends.slice(0, flushingLength)
    const info = await this.session.append(values, {
      keyPair,
      signature,
      preappend: async (values) => {
        if (this.session.length > this._sessionLength) {
          let shared = await this._catchupUnsafe(flushingLength)
          flushingLength -= shared
          while (shared-- > 0) values.shift()
        }
      }
    })

    if (info.length > expectedLength) {
      this.emit('flush')
      return false
    }

    const delta = info.byteLength - this._sessionByteLength

    this._sessionLength = info.length
    this._sessionByteLength = info.byteLength
    this._appends = this._appends.slice(flushingLength)
    this._byteLength -= delta

    this.treeBatch = this.session.core.tree.batch()

    this.emit('flush')

    return true
  }

  // has to be in preappend, requires mutex
  async _catchupUnsafe (length, auth) { // todo: better name
    const stop = Math.min(this.session.length - this._sessionLength, length)

    const blocks = []
    const start = this._sessionLength
    const byteOffset = this._sessionByteLength

    for (let i = 0; i < stop; i++) {
      const local = this._appends.shift()
      blocks.push(local)

      await this.treeBatch.append(local)

      this._sessionLength++
      this._sessionByteLength += local.byteLength
      this._byteLength -= local.byteLength
    }

    await this.session.core._fillBlocksUnsafe(blocks, start, byteOffset, this.treeBatch.nodes)

    return stop
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
    this.fork = 0
  }

  _clearBatch () {
    for (const session of this.session.sessions) session._batch = null
  }
}

function noop () {}
