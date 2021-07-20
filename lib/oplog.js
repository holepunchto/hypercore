const c = require('compact-encoding')
const hypercoreCrypto = require('hypercore-crypto')

const crc32 = require('./crc32')

const HEADER_LENGTH = 4096
const LOG_OFFSET = HEADER_LENGTH * 2

/**
 * (uint length)(32-bit checksum)(sub-encoding)
 */
function checksumEncoding (enc) {
  return {
    preencode (state, obj) {
      state.end += 4 * 2 // checksum + 32-bit uint length
      enc.preencode(state, obj)
    },
    encode (state, obj) {
      const start = state.start
      const lengthOffset = start + 4
      const prefixOffset = lengthOffset + 4

      state.start = prefixOffset // obj should be stored after checksum/length
      enc.encode(state, obj)
      const end = state.start

      state.start = start
      c.uint32.encode(state, state.end - prefixOffset)
      c.uint32.encode(state, computeChecksum(state.buffer.subarray(prefixOffset, state.end)))

      state.start = end
    },
    decode (state) {
      const length = c.uint32.decode(state)
      const checksum = c.uint32.decode(state)

      const buf = state.buffer.subarray(state.start, state.start + length)
      if (checksum !== computeChecksum(buf)) throw new Error('Checksum test failed')

      const start = state.start
      const decoded = enc.decode(state)
      if ((state.start - start) !== length) throw new Error('Invalid length')
      return decoded
    }
  }
}

const Header = {
  preencode (state, h) {
    c.uint.preencode(state, h.flags)
    c.uint.preencode(state, h.flushes)
    c.buffer.preencode(state, h.publicKey)
    c.buffer.preencode(state, h.secretKey)
    c.uint.preencode(state, h.fork)
    c.uint.preencode(state, h.length)
    c.buffer.preencode(state, h.signature)
    c.buffer.preencode(state, h.rootHash)
  },
  encode (state, h) {
    c.uint.encode(state, h.flags)
    c.uint.encode(state, h.flushes)
    c.buffer.encode(state, h.publicKey)
    c.buffer.encode(state, h.secretKey)
    c.uint.encode(state, h.fork)
    c.uint.encode(state, h.length)
    c.buffer.encode(state, h.signature)
    c.buffer.encode(state, h.rootHash)
  },
  decode (state) {
    return {
      flags: c.uint.decode(state),
      flushes: c.uint.decode(state),
      publicKey: c.buffer.decode(state),
      secretKey: c.buffer.decode(state),
      fork: c.uint.decode(state),
      length: c.uint.decode(state),
      signature: c.buffer.decode(state),
      rootHash: c.buffer.decode(state)
    }
  }
}

const Op = {
  preencode (state, op) {
    c.uint.preencode(state, op.flags)
    c.uint.preencode(state, op.flushes)
  },
  encode (state, op) {
    c.uint.encode(state, op.flags)
    c.uint.encode(state, op.flushes)
  },
  decode (state) {
    return {
      flags: c.uint.decode(state),
      flushes: c.uint.decode(state)
    }
  }
}

const HeaderEncoding = checksumEncoding(Header)
const OpEncoding = checksumEncoding(Op)

module.exports = class Oplog {
  constructor (storage, opts = {}) {
    this.storage = storage

    this.flags = 0
    this.flushes = 0

    this.publicKey = null
    this.secretKey = null

    this.signature = null
    this.rootHash = null
    this.fork = 0
    this.length = 0

    this._rawEntries = null //  Set in _loadEntries
    this._decodedEntries = []
    this._length = 0

    this._appending = false
    this._flushing = false
  }

  _keygen ({ crypto = hypercoreCrypto, secretKey, publicKey }) {
    if (!this.publicKey) {
      if (publicKey) {
        this.publicKey = publicKey
        this.secretKey = secretKey || null
      } else {
        const keys = crypto.keyPair()
        this.publicKey = keys.publicKey
        this.secretKey = keys.secretKey
      }
    } else if (publicKey && !this.publicKey.equals(publicKey)) {
      throw new Error('Another hypercore is stored here')
    }
  }

  _updateInfo (header) {
    this.flags = header.flags
    this.flushes = header.flushes
    this.publicKey = header.publicKey
    this.secretKey = header.secretKey
    this.signature = header.signature
    this.rootHash = header.rootHash
    this.fork = header.fork
    this.length = header.length

  }

  _loadEntries () {
    this._rawEntries = null
    return new Promise(resolve => {
      this.storage.read(LOG_OFFSET, this._length - LOG_OFFSET, (err, buf) => {
        if (err) return resolve(null)
        this._rawEntries = buf
        return resolve()
      })
    })
  }

  async _loadLatestHeader () {
    const readOpts = { encoding: HeaderEncoding }
    const [firstHeader, secondHeader] = await Promise.all([
      maybeRead(this.storage, 0, HEADER_LENGTH, readOpts),
      maybeRead(this.storage, HEADER_LENGTH, HEADER_LENGTH, readOpts)
    ])

    if (!firstHeader && !secondHeader) {
      if (this._length) throw new Error('Could not decode info file header -- Hypercore is corrupted')
      return {}
    }

    let header = null
    let first = false
    if (hasLaterFlush(firstHeader, secondHeader)) {
      header = firstHeader
      first = true
    } else {
      header = secondHeader
      first = false
    }

    return { header, first }
  }

  async _saveHeader () {
    const { first } = await this._loadLatestHeader()
    const encoded = c.encode(HeaderEncoding, this)
    const offset = first ? HEADER_LENGTH : 0 // If the first header is the latest, write to the second -- alternate

    return new Promise((resolve, reject) => {
      this.storage.write(offset, encoded, err => {
        if (err) return reject(err)
        return resolve()
      })
    })
  }

  async open (opts = {}) {
    const stat = await new Promise((resolve, reject) => {
      this.storage.open(err => {
        if (err && err.code !== 'ENOENT') return reject(err)
        if (err) return resolve(null)
        this.storage.stat((err, stat) => {
          if (err && err.code !== 'ENOENT') return reject(err)
          return resolve(stat)
        })
      })
    })
    if (stat && stat.size !== 0) {
      this._length = stat.size
      const { header } = await this._loadLatestHeader()
      this._updateInfo(header)
      await this._loadEntries(LOG_OFFSET, stat.size)
    } else {
      this._keygen(opts)
      await this._saveHeader()
      this._length = LOG_OFFSET
    }
  }

  async * [Symbol.asyncIterator] () {
    if (!this._rawEntries) {
      if (this._length > HEADER_LENGTH * 2) await this._loadEntries(LOG_OFFSET, this._length)
      else return null
    }
    const state = { start: 0, end: this._rawEntries.length, buffer: this._rawEntries }
    while (state.start < state.end) {
      yield OpEncoding.decode(state)
    }
  }

  async flush () {
    if (this._flushing) throw new Error('Concurrent flush')
    this._flushing = true
    this.flushes++
    try {
      await this._saveHeader() // If this throws, the log will not be truncated.
      await new Promise((resolve, reject) => {
        this.storage.del(LOG_OFFSET, +Infinity, err => {
          if (err) return reject(err)
          return resolve()
        })
      })
      this._length = LOG_OFFSET
      this._rawEntries = null
    } finally {
      this._flushing = false
    }
  }

  async append (op) {
    if (this._appending) throw new Error('Concurrent append')
    this._appending = true
    try {
      op.flushes = this.flushes
      const encoded = c.encode(OpEncoding, op)
      await new Promise((resolve, reject) => {
        this.storage.write(this._length, encoded, err => {
          if (err) return reject(err)
          this._length += encoded.length
          return resolve()
        })
      })
    } finally {
      this._appending = false
    }
  }

  static async open (storage, opts) {
    const oplog = new this(storage, opts)
    await oplog.open(opts)
    return oplog
  }
}

async function maybeRead (storage, start, length, opts) {
  const raw = await new Promise(resolve => {
    storage.read(start, length, (err, buf) => {
      if (err) return resolve(null)
      return resolve(buf)
    })
  })
  if (!(opts && opts.encoding)) return raw
  try {
    return c.decode(opts.encoding, raw)
  } catch (_) {
    return null
  }
}

function zigzagEncode (n) {
  // 0, -1, 1, -2, 2, ...
  return n < 0 ? (2 * -n) - 1 : n === 0 ? 0 : 2 * n
}

function computeChecksum (buf) {
  return zigzagEncode(crc32(buf))
}

function hasLaterFlush (a, b) {
  if (!a) return false
  if (!b) return true
  return ((a.flush - b.flush) & 255) < 128
}
