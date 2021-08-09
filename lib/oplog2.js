const cenc = require('compact-encoding')
const crc32 = require('crc32-universal')

module.exports = class Oplog {
  constructor (storage, { pageSize = 4096, headerEncoding = cenc.raw, entryEncoding = cenc.raw } = {}) {
    this.storage = storage
    this.headerEncoding = headerEncoding
    this.entryEncoding = entryEncoding
    this.flushed = false
    this.byteLength = 0
    this.length = 0

    this._headers = [1, 0]
    this._pageSize = pageSize
    this._entryOffset = pageSize * 2
  }

  _addHeader (state, headerBit) {
    state.start = 4
    cenc.uint32.encode(state, ((state.end - 8) << 1) | headerBit)

    // crc32 the length + header-bit + content and prefix it

    state.start = 0
    cenc.uint32.encode(state, crc32(state.buffer.subarray(4, state.end)))
  }

  _decodeEntry (state, enc) {
    if (state.end - state.start < 8) return null
    const cksum = cenc.uint32.decode(state)
    const l = cenc.uint32.decode(state)
    const length = l >>> 1
    const headerBit = l & 1

    if (state.end - state.start < length) return null

    const end = state.start + length

    if (crc32(state.buffer.subarray(state.start - 4, end)) !== cksum) {
      return null
    }

    const result = { header: headerBit, message: null }

    try {
      result.message = enc.decode({ start: state.start, end, buffer: state.buffer })
    } catch {
      return null
    }

    state.start = end

    return result
  }

  async open ({ onheader = noop, onentry = noop } = {}) {
    const buffer = await this._readAll() // TODO: stream the oplog in on load maybe?
    const state = { start: 0, end: buffer.byteLength, buffer }

    const h1 = this._decodeEntry(state, this.headerEncoding)
    state.start = this._pageSize

    const h2 = this._decodeEntry(state, this.headerEncoding)
    state.start = this._entryOffset

    if (!h1 && !h2) {
      if (buffer.byteLength >= this._entryOffset) {
        throw new Error('Oplog file appears corrupt or out of date')
      }
      return false
    }

    this.flushed = true

    if (h1 && !h2) {
      this._headers[0] = h1.header
      this._headers[1] = h1.header
    } else if (!h1 && h2) {
      this._headers[0] = (h2.header + 1) & 1
      this._headers[1] = h2.header
    } else {
      this._headers[0] = h1.header
      this._headers[1] = h2.header
    }

    const header = (this._headers[0] + this._headers[1]) & 1

    await onheader(header ? h2.message : h1.message)

    while (true) {
      const entry = this._decodeEntry(state, this.entryEncoding)
      if (!entry) break
      if (entry.header !== header) break

      await onentry(entry.message)

      this.length++
      this.byteLength = state.start - this._entryOffset
    }

    const size = this.byteLength + this._entryOffset

    if (size === buffer.byteLength) return true

    await new Promise((resolve, reject) => {
      this.storage.del(size, buffer.byteLength - size, err => {
        if (err) return reject(err)
        resolve()
      })
    })

    return true
  }

  _readAll () {
    return new Promise((resolve, reject) => {
      this.storage.open(err => {
        if (err && err.code !== 'ENOENT') return reject(err)
        if (err) return resolve(Buffer.alloc(0))
        this.storage.stat((err, stat) => {
          if (err && err.code !== 'ENOENT') return reject(err)
          this.storage.read(0, stat.size, (err, buf) => {
            if (err) return reject(err)
            resolve(buf)
          })
        })
      })
    })
  }

  flush (header) {
    const state = { start: 8, end: 8, buffer: null }
    const i = this._headers[0] === this._headers[1] ? 1 : 0
    const bit = (this._headers[i] + 1) & 1

    this.headerEncoding.preencode(state, header)
    state.buffer = Buffer.allocUnsafe(state.end)
    this.headerEncoding.encode(state, header)
    this._addHeader(state, bit)

    return this._writeHeaderAndTruncate(i, bit, state.buffer)
  }

  _writeHeaderAndTruncate (i, bit, buf) {
    return new Promise((resolve, reject) => {
      this.storage.write(i === 0 ? 0 : this._pageSize, buf, err => {
        if (err) return reject(err)

        this.storage.del(this._entryOffset, this.byteLength, err => {
          if (err) return reject(err)

          this._headers[i] = bit
          this.byteLength = 0
          this.length = 0
          this.flushed = true

          resolve()
        })
      })
    })
  }

  append (entry) {
    const state = { start: 8, end: 8, buffer: null }
    const bit = (this._headers[0] + this._headers[1]) & 1

    this.entryEncoding.preencode(state, entry)
    state.buffer = Buffer.allocUnsafe(state.end)
    this.entryEncoding.encode(state, entry)
    this._addHeader(state, bit)

    return this._appendOne(state.buffer)
  }

  close () {
    return new Promise((resolve, reject) => {
      this.storage.close(err => {
        if (err) return reject(err)
        resolve()
      })
    })
  }

  _appendOne (buf) {
    return new Promise((resolve, reject) => {
      this.storage.write(this._entryOffset + this.byteLength, buf, err => {
        if (err) return reject(err)

        this.byteLength += buf.byteLength
        this.length++

        resolve()
      })
    })
  }
}

function noop () {}
