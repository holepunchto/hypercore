const uint64le = require('uint64le')
const c = require('compact-encoding')

const crc32 = require('./crc32')

const HEADER_LENGTH = 4096

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
      const checksum = crc32(state.buffer.subarray(prefixOffset, state.end))
      c.uint32.encode(state, zigzagEncode(checksum))

      state.start = end
    },
    decode (state) {
      const length = c.uint32.decode(state)
      const checksum = c.uint32.decode(state)

      if (length !== state.end - state.start) throw new Error('Invalid length')
      const buf = state.buffer.subarray(state.start, state.end)
      if (checksum !== zigzagEncode(crc32(buf))) throw new Error('Checksum test failed')

      return enc.decode(state)
    }
  }
}

function headerEncoding (infoEncoding) {
  return {
    preencode (state, h) {
      c.uint.preencode(state, h.timestamp)
      infoEncoding.preencode(state, h.info)
    },
    encode (state, h) {
      c.uint.encode(state, h.timestamp)
      infoEncoding.encode(state, h.info)
    },
    decode (state) {
      return {
        timestamp: c.uint.decode(state),
        info: infoEncoding.decode(state)
      }
    }
  }
}

class Oplog {
  constructor (storage, opts = {}) {
    this.storage = storage
    this.valueEncoding = checksumEncoding(opts.valueEncoding || c.raw)
    this.headerEncoding = checksumEncoding(headerEncoding(opts.infoEncoding || c.raw))
    this.info = opts.info
  }

  _decodeHeader (firstBlock, secondBlock) {
    const firstHeader = tryDecode(firstBlock, this.headerEncoding)
    const secondHeader = tryDecode(secondBlock, this.headerEncoding)
    if (!firstHeader && !secondHeader) throw new Error('Could not decode info file header -- Hypercore is corrupted')
    if (firstHeader && secondHeader) {
      if (firstHeader.timestamp > secondHeader.timestamp) this.info = firstHeader.info
      else this.info = secondHeader.info
    } else if (firstHeader) {
      this.info = firstHeader.info
    } else {
      this.info = secondHeader.info
    }
  }

  _loadHeader () {
    return new Promise((resolve, reject) => {
      this.storage.read(0, HEADER_LENGTH, (err, firstBlock) =>{
        if (err) return reject(err)
        this.storage.read(HEADER_LENGTH, HEADER_LENGTH, (err, secondBlock) => {
          if (err) return reject(err)
          return this._decodeHeader(firstBlock, secondBlock)
        })
      })
    })
  }

  _saveHeader () {

  }

  async open (opts) {
    const stat = await new Promise((resolve, reject) => {
      this.storage.stat((err, stat) => {
        if (err) return reject(err)
        return resolve(stat)
      })
    })
    if (stat.size !== 0) await this._loadHeader()
    else await this._saveHeader()
  }

  batch () {
    return new OplogBatch(this)
  }
}

class OplogBatch {
  constructor (oplog) {
    this.oplog = oplog
    this._log = []
  }

  append (entry) {

  }

  flush () {

  }
}

module.exports = class Info {
  constructor (storage) {
    this.storage = storage
    this.secretKey = null
    this.publicKey = null
    this.signature = null
    this.fork = 0
  }

  async _keygen ({ crypto, secretKey, publicKey }) {
    if (!this.publicKey) {
      if (publicKey) {
        this.publicKey = publicKey
        this.secretKey = secretKey || null
      } else {
        const keys = crypto.keyPair()
        this.publicKey = keys.publicKey
        this.secretKey = keys.secretKey
      }
      await this.flush()
    } else if (publicKey && !this.publicKey.equals(publicKey)) {
      throw new Error('Another hypercore is stored here')
    }
  }

  async open (opts) {
    await new Promise((resolve) => {
      this.storage.read(0, 64 + 32 + 64 + 8, (_, buf) => {
        if (buf) {
          this.secretKey = notZero(buf.slice(0, 64))
          this.publicKey = buf.slice(64, 64 + 32)
          this.signature = notZero(buf.slice(64 + 32, 64 + 32 + 64))
          this.fork = uint64le.decode(buf, 64 + 32 + 64)
        }
        resolve()
      })
    })
    return this._keygen(opts)
  }

  static async open (storage, opts) {
    const info = new Info(storage)
    await info.open(opts)
    return info
  }

  commit () {
    const buf = Buffer.alloc(64 + 32 + 64 + 8)
    if (this.secretKey !== null) this.secretKey.copy(buf)
    this.publicKey.copy(buf, 64)
    if (this.signature) this.signature.copy(buf, 64 + 32)
    uint64le.encode(this.fork, buf, 64 + 32 + 64)
    return buf
  }

  close () {
    return new Promise((resolve, reject) => {
      this.storage.close((err) => {
        if (err) reject(err)
        else resolve()
      })
    })
  }

  flush () {
    const buf = this.commit()
    return new Promise((resolve, reject) => {
      this.storage.write(0, buf, (err) => {
        if (err) return reject(err)
        resolve()
      })
    })
  }
}

function notZero (b) {
  for (let i = 0; i < b.length; i++) {
    if (b[i] !== 0) return b
  }
  return null
}

function tryDecode (buf, encoding) {
  try {
    return encoding.decode(buf)
  } catch (err) {
    return null
  }
}

function zigzagEncode (n) {
  // 0, -1, 1, -2, 2, ...
  return n < 0 ? (2 * -n) - 1 : n === 0 ? 0 : 2 * n
}
