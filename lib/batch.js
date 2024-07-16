const { BLOCK_NOT_AVAILABLE, SESSION_CLOSED } = require('hypercore-errors')
const EventEmitter = require('events')
const c = require('compact-encoding')
const b4a = require('b4a')
const safetyCatch = require('safety-catch')

module.exports = class HypercoreBatch extends EventEmitter {
  constructor (session, checkoutLength, autoClose, restore, clear) {
    super()

    this.session = session
    this.opened = false
    this.closed = false
    this.opening = null
    this.closing = null
    this.writable = true // always writable...
    this.autoClose = autoClose
    this.restore = restore
    this.fork = 0

    this._appends = []
    this._appendsActual = null
    this._checkoutLength = checkoutLength
    this._byteLength = 0
    this._sessionLength = 0
    this._sessionByteLength = 0
    this._sessionBatch = null
    this._cachedBatch = null
    this._flushing = null
    this._clear = clear

    this.opening = this._open()
    this.opening.catch(safetyCatch)
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
    return Math.min(this._sessionLength, this.session.core === null ? 0 : this.session.core.tree.length)
  }

  get flushedLength () {
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

  get core () {
    return this.session.core
  }

  get manifest () {
    return this.session.manifest
  }

  ready () {
    return this.opening
  }

  async _open () {
    await this.session.ready()

    if (this._clear) this._checkoutLength = this.core.tree.length

    if (this._checkoutLength !== -1) {
      const batch = await this.session.core.tree.restoreBatch(this._checkoutLength)
      batch.treeLength = this._checkoutLength
      this._sessionLength = batch.length
      this._sessionByteLength = batch.byteLength
      this._sessionBatch = batch
      if (this._clear) await this.core.clearBatch()
    } else {
      const last = this.restore ? this.session.core.bitfield.findFirst(false, this.session.length) : 0

      if (last > this.session.length) {
        const batch = await this.session.core.tree.restoreBatch(last)
        this._sessionLength = batch.length
        this._sessionByteLength = batch.byteLength - this.session.padding * batch.length
        this._sessionBatch = batch
      } else {
        this._sessionLength = this.session.length
        this._sessionByteLength = this.session.byteLength
        this._sessionBatch = this.session.createTreeBatch()
      }
    }

    this._appendsActual = this.session.encryption ? [] : this._appends
    this.fork = this.session.fork
    this.opened = true
    this.emit('ready')
  }

  async has (index) {
    if (this.opened === false) await this.ready()
    if (index >= this._sessionLength) return index < this.length
    return this.session.has(index)
  }

  async update (opts) {
    if (this.opened === false) await this.ready()
    await this.session.update(opts)
  }

  treeHash () {
    return this._sessionBatch.hash()
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

  async seek (bytes, opts = {}) {
    if (this.opened === false) await this.opening
    if (this.closing) throw SESSION_CLOSED()

    if (bytes < this._sessionByteLength) return await this.session.seek(bytes, { ...opts, tree: this._sessionBatch })

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

  async get (index, opts = {}) {
    if (this.opened === false) await this.opening
    if (this.closing) throw SESSION_CLOSED()

    const length = this._sessionLength

    if (index < length) {
      return this.session.get(index, { ...opts, tree: this._sessionBatch })
    }

    if (opts && opts.raw) {
      return this._appendsActual[index - length] || null
    }

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

  async restoreBatch (length, blocks) {
    if (this.opened === false) await this.opening
    if (length >= this._sessionLength) return this.createTreeBatch(length, blocks)
    return this.session.core.tree.restoreBatch(length)
  }

  _catchupBatch (clone) {
    if (this._cachedBatch === null) this._cachedBatch = this._sessionBatch.clone()

    if (this.length > this._cachedBatch.length) {
      const offset = this._cachedBatch.length - this._sessionBatch.length

      for (let i = offset; i < this._appendsActual.length; i++) {
        this._cachedBatch.append(this._appendsActual[i])
      }
    }

    return clone ? this._cachedBatch.clone() : this._cachedBatch
  }

  createTreeBatch (length, opts = {}) {
    if (Array.isArray(opts)) opts = { blocks: opts }

    const { blocks = [], clone = true } = opts
    if (!length && length !== 0) length = this.length + blocks.length

    const maxLength = this.length + blocks.length
    const b = this._catchupBatch(clone || (blocks.length > 0 || length !== this.length))
    const len = Math.min(length, this.length)

    if (len < this._sessionLength || length > maxLength) return null
    if (len < b.length) b.checkout(len, this._sessionBatch.roots)

    for (let i = 0; i < length - len; i++) {
      b.append(this._appendsActual === this._appends ? blocks[i] : this._encrypt(b.length, blocks[i]))
    }

    return b
  }

  async truncate (newLength = 0, opts = {}) {
    if (this.opened === false) await this.opening
    if (this.closing) throw SESSION_CLOSED()

    // wait for any pending flush... (prop needs a lock)
    await this._waitForFlush()

    if (typeof opts === 'number') opts = { fork: opts }
    const { fork = this.fork + 1, force = false } = opts

    this._cachedBatch = null

    const length = this._sessionLength
    if (newLength < length) {
      if (!force) throw new Error('Cannot truncate committed blocks')
      this._appends.length = 0
      this._byteLength = 0
      await this.session.truncate(newLength, { fork, force: true, ...opts })
      this._sessionLength = this.session.length
      this._sessionByteLength = this.session.byteLength
      this._sessionBatch = this.session.createTreeBatch()
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
    if (this.closing) throw SESSION_CLOSED()

    // wait for any pending flush... (prop needs a lock)
    await this._waitForFlush()

    blocks = Array.isArray(blocks) ? blocks : [blocks]

    const buffers = session.encodeBatch !== null
      ? session.encodeBatch(blocks)
      : new Array(blocks.length)

    if (session.encodeBatch === null) {
      for (let i = 0; i < blocks.length; i++) {
        const buffer = this._encode(session.valueEncoding, blocks[i])
        buffers[i] = buffer
        this._byteLength += buffer.byteLength
      }
    }
    if (this._appends !== this._appendsActual) {
      for (let i = 0; i < buffers.length; i++) {
        this._appendsActual.push(this._encrypt(this._sessionLength + this._appendsActual.length, buffers[i]))
      }
    }

    for (const b of buffers) this._appends.push(b)

    const info = { length: this.length, byteLength: this.byteLength }
    this.emit('append')

    return info
  }

  _encode (enc, val) {
    const state = { start: 0, end: 0, buffer: null }

    if (b4a.isBuffer(val)) {
      if (state.start === 0) return val
      state.end += val.byteLength
    } else if (enc) {
      enc.preencode(state, val)
    } else {
      val = b4a.from(val)
      if (state.start === 0) return val
      state.end += val.byteLength
    }

    state.buffer = b4a.allocUnsafe(state.end)

    if (enc) enc.encode(state, val)
    else state.buffer.set(val, state.start)

    return state.buffer
  }

  _encrypt (index, buffer) {
    const block = b4a.allocUnsafe(buffer.byteLength + 8)
    block.set(buffer, 8)
    this.session.encryption.encrypt(index, block, this.fork)
    return block
  }

  async flush (opts = {}) {
    if (this.opened === false) await this.opening
    if (this.closing) throw SESSION_CLOSED()

    const { length = this.length, keyPair = this.session.keyPair, signature = null, pending = !signature && !keyPair } = opts

    while (this._flushing) await this._flushing
    this._flushing = this._flush(length, keyPair, signature, pending)

    let flushed = false

    try {
      flushed = await this._flushing
    } finally {
      this._flushing = null
    }

    if (this.autoClose) await this.close()

    return flushed
  }

  async _flush (length, keyPair, signature, pending) { // TODO: make this safe to interact with a parallel truncate...
    if (this._sessionBatch.fork !== this.session.fork) return false // no truncs supported atm

    if (this.session.replicator._upgrade) {
      for (const req of this.session.replicator._upgrade.inflight) {
        // yield to the remote inflight upgrade, TODO: if the remote upgrade fails, retry flushing...
        if (req.upgrade && (req.upgrade.start + req.upgrade.length) > length) {
          return false
        }
      }
    }

    const flushingLength = Math.min(length - this._sessionLength, this._appends.length)
    if (flushingLength <= 0) {
      if (this._sessionLength > this.core.tree.length && length > this.core.tree.length && !pending) {
        const batch = await this.restoreBatch(length)
        const info = await this.core.insertBatch(batch, [], { keyPair, signature, pending, treeLength: length })
        return info !== null
      }
      return true
    }

    const batch = this.createTreeBatch(this._sessionLength + flushingLength)
    if (batch === null) return false

    const info = await this.core.insertBatch(batch, this._appendsActual, { keyPair, signature, pending, treeLength: this._sessionLength })
    if (info === null) return false

    const delta = info.byteLength - this._sessionByteLength
    const newBatch = info.length !== this.session.length ? await this.session.core.tree.restoreBatch(info.length) : this.session.createTreeBatch()

    this._sessionLength = info.length
    this._sessionByteLength = info.byteLength
    this._sessionBatch = newBatch

    if (this._cachedBatch !== null) this._cachedBatch.prune(info.length)

    const same = this._appends === this._appendsActual

    this._appends = this._appends.slice(flushingLength)
    this._appendsActual = same ? this._appends : this._appendsActual.slice(flushingLength)
    this._byteLength -= delta

    this.emit('flush')

    return true
  }

  close () {
    if (!this.closing) this.closing = this._close()
    return this.closing
  }

  async _close () {
    this._clearAppends()

    await this.session.close()

    this.closed = true
    this.emit('close')
  }

  _clearAppends () {
    this._appends = []
    this._appendsActual = []
    this._byteLength = 0
    this.fork = 0
  }
}
