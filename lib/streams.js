const { Writable, Readable } = require('streamx')

class ReadStream extends Readable {
  constructor (core, opts = {}) {
    super()

    this.core = core
    this.start = opts.start || 0
    this.end = typeof opts.end === 'number' ? opts.end : -1
    this.snapshot = !opts.live && opts.snapshot !== false
    this.live = !!opts.live
  }

  _open (cb) {
    this._openP().then(cb, cb)
  }

  _read (cb) {
    this._readP().then(cb, cb)
  }

  async _openP () {
    if (this.end === -1) await this.core.update()
    else await this.core.ready()
    if (this.snapshot && this.end === -1) this.end = this.core.length
  }

  async _readP () {
    const end = this.live ? -1 : (this.end === -1 ? this.core.length : this.end)
    if (end >= 0 && this.start >= end) {
      this.push(null)
      return
    }

    this.push(await this.core.get(this.start++))
  }
}

exports.ReadStream = ReadStream

class WriteStream extends Writable {
  constructor (core) {
    super()
    this.core = core
  }

  _writev (batch, cb) {
    this._writevP(batch).then(cb, cb)
  }

  async _writevP (batch) {
    await this.core.append(batch)
  }
}

exports.WriteStream = WriteStream

class ByteStream extends Readable {
  constructor (core, opts = {}) {
    super()

    this._core = core
    this._index = 0
    this._range = null

    this.byteOffset = opts.byteOffset || 0
    this.byteLength = typeof opts.byteLength === 'number' ? opts.byteLength : -1
    this.prefetch = typeof opts.prefetch === 'number' ? opts.prefetch : 32

    this._applyOffset = this.byteOffset > 0
  }

  _open (cb) {
    this._openp().then(cb, cb)
  }

  _read (cb) {
    this._readp().then(cb, cb)
  }

  async _openp () {
    if (this.byteLength === -1) {
      await this._core.update()
      this.byteLength = Math.max(this._core.byteLength - this.byteOffset, 0)
    }
  }

  async _readp () {
    let data = null

    if (this.byteLength === 0) {
      this.push(null)
      return
    }

    let relativeOffset = 0

    if (this._applyOffset) {
      this._applyOffset = false

      const [block, byteOffset] = await this._core.seek(this.byteOffset)
      this._index = block

      relativeOffset = byteOffset
    }

    this._prefetch(this._index + 1)
    data = await this._core.get(this._index++)

    if (relativeOffset > 0) data = data.subarray(relativeOffset)

    const final = data.length >= this.byteLength
    if (final) data = data.subarray(0, this.byteLength)
    this.byteLength -= data.length

    this.push(data)
    if (final) this.push(null)
  }

  _prefetch (index) {
    if (this._range) this._range.destroy()
    this._range = this._core.download({ start: index, end: index + this.prefetch, linear: true })
  }

  _destroy (cb) {
    if (this._range) this._range.destroy()
    cb(null)
  }
}

exports.ByteStream = ByteStream
