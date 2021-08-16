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

  _addHeader (state, len, headerBit, partialBit) {
    // add the uint header (frame length and flush info)
    state.start = state.start - len - 4
    cenc.uint32.encode(state, (len << 2) | headerBit | partialBit)

    // crc32 the length + header-bit + content and prefix it
    state.start -= 8
    cenc.uint32.encode(state, crc32(state.buffer.subarray(state.start + 4, state.start + 8 + len)))
    state.start += len + 4
  }

  _decodeEntry (state, enc) {
    if (state.end - state.start < 8) return null
    const cksum = cenc.uint32.decode(state)
    const l = cenc.uint32.decode(state)
    const length = l >>> 2
    const headerBit = l & 1
    const partialBit = l & 2

    if (state.end - state.start < length) return null

    const end = state.start + length

    if (crc32(state.buffer.subarray(state.start - 4, end)) !== cksum) {
      return null
    }

    const result = { header: headerBit, partial: partialBit !== 0, byteLength: length + 8, message: null }

    try {
      result.message = enc.decode({ start: state.start, end, buffer: state.buffer })
    } catch {
      return null
    }

    state.start = end

    return result
  }

  async open () {
    const buffer = await this._readAll() // TODO: stream the oplog in on load maybe?
    const state = { start: 0, end: buffer.byteLength, buffer }
    const result = { header: null, entries: [] }

    this.byteLength = 0
    this.length = 0

    const h1 = this._decodeEntry(state, this.headerEncoding)
    state.start = this._pageSize

    const h2 = this._decodeEntry(state, this.headerEncoding)
    state.start = this._entryOffset

    if (!h1 && !h2) {
      // reset state...
      this.flushed = false
      this._headers[0] = 1
      this._headers[1] = 0

      if (buffer.byteLength >= this._entryOffset) {
        throw new Error('Oplog file appears corrupt or out of date')
      }
      return result
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
    const decoded = []

    result.header = header ? h2.message : h1.message

    while (true) {
      const entry = this._decodeEntry(state, this.entryEncoding)
      if (!entry) break
      if (entry.header !== header) break

      decoded.push(entry)
    }

    while (decoded.length > 0 && decoded[decoded.length - 1].partial) decoded.pop()

    for (const e of decoded) {
      result.entries.push(e.message)
      this.byteLength += e.byteLength
      this.length++
    }

    const size = this.byteLength + this._entryOffset

    if (size === buffer.byteLength) return result

    await new Promise((resolve, reject) => {
      this.storage.del(size, Infinity, err => {
        if (err) return reject(err)
        resolve()
      })
    })

    return result
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
    this._addHeader(state, state.end - 8, bit, 0)

    return this._writeHeaderAndTruncate(i, bit, state.buffer)
  }

  _writeHeaderAndTruncate (i, bit, buf) {
    return new Promise((resolve, reject) => {
      this.storage.write(i === 0 ? 0 : this._pageSize, buf, err => {
        if (err) return reject(err)

        this.storage.del(this._entryOffset, Infinity, err => {
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

  append (batch, atomic = true) {
    if (!Array.isArray(batch)) batch = [batch]

    const state = { start: 0, end: batch.length * 8, buffer: null }
    const bit = (this._headers[0] + this._headers[1]) & 1

    for (let i = 0; i < batch.length; i++) {
      this.entryEncoding.preencode(state, batch[i])
    }

    state.buffer = Buffer.allocUnsafe(state.end)

    for (let i = 0; i < batch.length; i++) {
      const start = state.start += 8 // space for header
      const partial = (atomic && i < batch.length - 1) ? 2 : 0
      this.entryEncoding.encode(state, batch[i])
      this._addHeader(state, state.start - start, bit, partial)
    }

    return this._append(state.buffer, batch.length)
  }

  close () {
    return new Promise((resolve, reject) => {
      this.storage.close(err => {
        if (err) return reject(err)
        resolve()
      })
    })
  }

  _append (buf, count) {
    return new Promise((resolve, reject) => {
      this.storage.write(this._entryOffset + this.byteLength, buf, err => {
        if (err) return reject(err)

        this.byteLength += buf.byteLength
        this.length += count

        resolve()
      })
    })
  }
}
