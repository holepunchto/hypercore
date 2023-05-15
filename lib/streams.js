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
  constructor (core, byteOffset, byteLength) {
    super()

    this.core = core
    this.byteOffset = byteOffset
    this.byteLength = byteLength

    this.index = 0
    this.range = null
  }

  async _read (cb) {
    let data = null

    if (!this.byteLength) {
      this.push(null)
      return cb(null)
    }

    let relativeOffset = 0

    if (this.byteOffset > 0) {
      const [block, byteOffset] = await this.core.seek(this.byteOffset)

      this.byteOffset = 0
      this.index = block

      relativeOffset = byteOffset
    }

    this._prefetch(this.index + 1)
    data = await this.core.get(this.index++)

    if (relativeOffset > 0) data = data.subarray(relativeOffset)

    if (data.length >= this.byteLength) {
      data = data.subarray(0, this.byteLength)
      this.push(data)
      this.push(null)
    } else {
      this.push(data)
    }

    this.byteLength -= data.length

    cb(null)
  }

  _prefetch (index) {
    if (this.range) this.range.destroy()
    this.range = this.core.download({ start: index, end: index + 32, linear: true })
  }

  _destroy (cb) {
    if (this.range) this.range.destroy()
    cb(null)
  }
}

exports.ByteStream = ByteStream
