const uint64le = require('uint64le')

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
