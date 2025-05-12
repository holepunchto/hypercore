const { Writable, Readable } = require('streamx')

class ReadStream extends Readable {
  constructor (core, opts = {}) {
    super()

    this.core = core
    this.start = opts.start || 0
    this.end = typeof opts.end === 'number' ? opts.end : -1
    this.snapshot = !opts.live && opts.snapshot !== false
    this.live = this.end === -1 ? !!opts.live : false
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

    this._byteOffset = opts.byteOffset || 0
    this._byteLength = typeof opts.byteLength === 'number' ? opts.byteLength : -1
    this._prefetch = typeof opts.prefetch === 'number' ? opts.prefetch : 32

    this._applyOffset = this._byteOffset > 0
  }

  _open (cb) {
    this._openp().then(cb, cb)
  }

  _read (cb) {
    this._readp().then(cb, cb)
  }

  async _openp () {
    if (this._byteLength === -1) {
      await this._core.update()
      this._byteLength = Math.max(this._core.byteLength - this._byteOffset, 0)
    }
  }

  async _readp () {
    let data = null

    if (this._byteLength === 0) {
      this.push(null)
      return
    }

    let relativeOffset = 0

    if (this._applyOffset) {
      this._applyOffset = false

      const [block, byteOffset] = await this._core.seek(this._byteOffset)

      this._index = block
      relativeOffset = byteOffset
    }

    this._predownload(this._index + 1)
    data = await this._core.get(this._index++, { valueEncoding: 'binary' })

    if (relativeOffset > 0) data = data.subarray(relativeOffset)

    if (data.byteLength > this._byteLength) data = data.subarray(0, this._byteLength)
    this._byteLength -= data.byteLength

    this.push(data)
    if (this._byteLength === 0) this.push(null)
  }

  _predownload (index) {
    if (this._range) this._range.destroy()
    this._range = this._core.download({ start: index, end: index + this._prefetch, linear: true })
  }

  _destroy (cb) {
    if (this._range) this._range.destroy()
    cb(null)
  }
}

exports.ByteStream = ByteStream
