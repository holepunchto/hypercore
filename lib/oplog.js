const c = require('compact-encoding')
const hypercoreCrypto = require('hypercore-crypto')
const { oplog: oplogMessages } = require('./messages')

const INITIALIZED = Buffer.from('bdc2e916caf1041e6aceb55d1211a92d7a2f5a242a6a4dda26d331a2bc317605', 'hex')

const HEADER_LENGTH = 4096
const HEADERS_LENGTH = HEADER_LENGTH * 2
const METADATA_LENGTH = HEADERS_LENGTH + INITIALIZED.length

module.exports = class Oplog {
  constructor (storage, opts = {}) {
    this.storage = storage

    // Saved in header
    this.publicKey = opts.publicKey
    this.secretKey = opts.secretKey
    this.flags = 0
    this.flushes = 0
    this.fork = 0
    this.length = 0
    this.signature = null
    this.rootHash = null
    this.locked = false

    this._logLength = 0
    this._appending = false
    this._flushing = false
    this._opening = null
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
    if (this._logLength === 0) return null
    return new Promise((resolve, reject) => {
      this.storage.read(METADATA_LENGTH, this._logLength, (err, buf) => {
        if (err) return reject(err)
        return resolve(buf)
      })
    })
  }

  async _loadMetadata () {
    const [firstHeader, secondHeader, initialized] = await Promise.allSettled([
      read(this.storage, 0, HEADER_LENGTH, oplogMessages.header),
      read(this.storage, HEADER_LENGTH, HEADER_LENGTH, oplogMessages.header),
      read(this.storage, HEADERS_LENGTH, INITIALIZED.length)
    ])

    // If the hypercore was never initialized, or partially initialized, reinititialize
    if (!initialized.value || !initialized.value.equals(INITIALIZED)) return null

    // If neither header is readable post-init, the hypercore is corrupted
    if (!firstHeader.value && !secondHeader.value) throw new Error('Could not decode info file header -- Info file is corrupted')

    const first = firstHeader.value
    const second = secondHeader.value

    return {
      latestHeader: hasLaterFlush(first, second) ? 0 : 1,
      headers: [first, second]
    }
  }

  async _saveMetadata (initialize = false) {
    const metadata = await this._loadMetadata()
    const encoded = c.encode(oplogMessages.header, this)
    const offset = (metadata && metadata.latestHeader === 0) ? HEADER_LENGTH : 0 // If the first header is the latest, or this is the first write, write to the second -- alternate

    return new Promise((resolve, reject) => {
      this.storage.write(offset, encoded, err => {
        if (err) return reject(err)
        if (!initialize) return resolve()
        this.storage.write(HEADERS_LENGTH, INITIALIZED, err => {
          if (err) return reject(err)
          return resolve()
        })
      })
    })
  }

  async open (opts = {}) {
    const st = await stat(this.storage)
    const metadata = await this._loadMetadata()
    if (metadata) {
      this._updateInfo(metadata.headers[metadata.latestHeader])
      this._keygen(opts)
      this._logLength = st.size - METADATA_LENGTH
      await this._loadEntries()
    } else {
      this._keygen(opts)
      await this._saveMetadata(true)
    }
  }

  close () {
    return new Promise((resolve, reject) => {
      this.storage.close(err => {
        if (err) return reject(err)
        return resolve()
      })
    })
  }

  async * [Symbol.asyncIterator] () {
    const rawEntries = await this._loadEntries()
    if (!rawEntries) return null
    const state = { start: 0, end: rawEntries.length, buffer: rawEntries }
    while (state.start < state.end) {
      yield oplogMessages.op.decode(state)
    }
  }

  async flush () {
    if (this._flushing) throw new Error('Concurrent flush')
    this._flushing = true
    this.flushes++
    try {
      await this._saveMetadata() // If this throws, the log will not be truncated.
      await new Promise((resolve, reject) => {
        this.storage.del(METADATA_LENGTH, +Infinity, err => {
          if (err) return reject(err)
          return resolve()
        })
      })
      this._logLength = 0
    } finally {
      this._flushing = false
    }
  }

  async append (ops, state = c.state()) {
    if (this._appending) throw new Error('Concurrent append')
    this._appending = true

    if (!Array.isArray(ops)) ops = [ops]

    try {
      for (const op of ops) {
        op.flushes = this.flushes
        oplogMessages.op.preencode(state, op)
      }

      state.buffer = Buffer.allocUnsafe(state.end)
      state.start = 0

      for (const op of ops) {
        oplogMessages.op.encode(state, op)
      }

      await write(this.storage, METADATA_LENGTH + this._logLength, state.buffer)
      this._logLength += state.buffer.length
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

async function read (storage, start, length, encoding) {
  const raw = await new Promise((resolve, reject) => {
    storage.read(start, length, (err, buf) => {
      if (err) return reject(err)
      return resolve(buf)
    })
  })
  return encoding ? c.decode(encoding, raw) : raw
}

async function write (storage, offset, buf) {
  return new Promise((resolve, reject) => {
    storage.write(offset, buf, err => {
      if (err) return reject(err)
      return resolve()
    })
  })
}

function stat (storage) {
  return new Promise((resolve, reject) => {
    storage.open(err => {
      if (err && err.code !== 'ENOENT') return reject(err)
      if (err) return resolve(null)
      storage.stat((err, stat) => {
        if (err && err.code !== 'ENOENT') return reject(err)
        return resolve(stat)
      })
    })
  })
}

function hasLaterFlush (a, b) {
  if (!a) return false
  if (!b) return true
  return ((a.flushes - b.flushes) & 255) < 128
}
