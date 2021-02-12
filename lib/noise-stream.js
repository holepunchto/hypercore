const { Duplex } = require('streamx')
const { Pull, Push, HEADERBYTES, KEYBYTES, ABYTES } = require('sodium-secretstream')
const sodium = require('sodium-universal')
const noise = require('noise-protocol')
const borc = require('borc') // TODO: replace me with something schema like
const { generateKeypair, generateSeedKeypair } = require('noise-protocol/dh')

const inspect = Symbol.for('nodejs.util.inspect.custom')
const PROLOUGE = Buffer.from('hypercore')
const EMPTY = Buffer.alloc(0)

module.exports = class NoiseStream extends Duplex {
  constructor (isInitiator, opts = {}) {
    super()

    const keyPair = opts.keyPair || noise.keygen()

    this.isInitiator = isInitiator
    this.remotePublicKey = null
    this.publicKey = keyPair.publicKey

    this._keyPair = keyPair
    this._setup = true
    this._handshake = null
    this._handshakeBuffer = Buffer.alloc(100)

    this._tx = null
    this._rx = null
    this._corks = 1
    this._corked = []
    this._state = 0
    this._len = 0
    this._tmp = 1
    this._message = null
  }

  [inspect] (depth, opts) {
    let indent = ''
    if (typeof opts.indentationLvl === 'number') {
      while (indent.length < opts.indentationLvl) indent += ' '
    }

    return this.constructor.name + ' {\n' +
      indent + '  isInitiator: ' + opts.stylize(this.isInitiator, 'boolean') + '\n' +
      indent + '  publicKey: ' + opts.stylize(hex(this.publicKey, 'boolean'), 'string') + '\n' +
      indent + '  remotePublicKey: ' + opts.stylize(hex(this.remotePublicKey), 'string') + '\n' +
      indent + '  corked: ' + opts.stylize(this.corked, 'boolean') + '\n' +
      indent + '  destroyed: ' + opts.stylize(this.destroyed, 'boolean') + '\n' +
      indent + '}'
  }

  static keyPair (seed, publicKey = Buffer.alloc(noise.PKLEN), secretKey = Buffer.alloc(noise.SKLEN)) {
    if (seed) generateSeedKeypair(seed, publicKey, secretKey)
    else generateKeypair(publicKey, secretKey)
    return { publicKey, secretKey }
  }

  get corked () {
    return this._corks > 0
  }

  // TODO: impl encode schemas for each message here
  _encode (type, message) {
    return borc.encode(message)
  }

  // TODO: impl decode schemas for each message here
  _decode (type, buf, start, end) {
    try {
      return borc.decodeFirst(buf.slice(start, end))
    } catch (err) {
      this.destroy(err)
    }
  }

  _open (cb) {
    this._handshake = noise.initialize('XX', this.isInitiator, PROLOUGE, this._keyPair, null, null)
    if (this.isInitiator) this._handshakeRT()
    cb(null)
  }

  _write (data, cb) {
    let offset = 0

    do {
      switch (this._state) {
        case 0: {
          while (this._tmp !== 16777216 && offset < data.length) {
            const v = data[offset++]
            this._len += this._tmp * v
            this._tmp *= 256
          }

          if (this._tmp === 16777216) {
            this._tmp = 0
            this._state = 1
          }

          break
        }

        case 1: {
          const missing = this._len - this._tmp
          const end = missing + offset

          if (this._message === null && end <= data.length) {
            this._message = data.slice(offset, end)
            offset += missing
            this._onmessage()
            break
          }

          if (this._message === null) {
            this._message = Buffer.allocUnsafe(this._len)
          }

          data.copy(this._message, this._tmp, offset)

          if (end <= data.length) {
            offset += missing
            this._onmessage()
          } else {
            offset += data.length - offset
          }

          break
        }
      }
    } while (offset < data.length && !this.destroying)

    cb(null)
  }

  _onmessage () {
    const message = this._message

    this._state = 0
    this._len = 0
    this._tmp = 1
    this._message = null

    if (this._setup) {
      if (this._handshake !== null) {
        const split = noise.readMessage(this._handshake, message, EMPTY)
        if (split) this._onhandshake(split)
        else this._handshakeRT()
        return
      }
      // last message, receiving header
      this._rx.init(message)
      this._setup = false
      return
    }

    if (message.length < ABYTES) {
      this.destroy(new Error('Invalid message received'))
      return
    }

    const plain = message.slice(0, message.length - ABYTES)
    this._rx.next(message, plain)

    const type = plain[0]

    if (type === 0) { // batch!
      let offset = 1
      while (offset < plain.length && !this.destroying) {
        const len = readUint24le(plain, offset)
        const type = plain[offset + 3]
        const start = offset + 4
        const end = offset += 3 + len
        this.emit('message', type, this._decode(type, plain, start, end), true)
      }
    } else {
      this.emit('message', type, this._decode(type, plain, 1, plain.length), false)
    }
  }

  cork () {
    if (++this._corks > 1) return
    this._corked = []
  }

  uncork () {
    if (--this._corks > 0) return

    const corked = this._corked
    this._corked = null

    if (corked.length === 0) return

    let length = 1 + ABYTES
    for (let i = 0; i < corked.length; i++) {
      length += corked[i][1].length + 4
    }

    const all = Buffer.allocUnsafe(3 + length)

    // write plain to +1 offset so we can do inplace enc
    const plain = all.slice(4, all.length - ABYTES + 1)
    const cipher = all.slice(3)

    plain[0] = 0 // type type
    let offset = 1

    for (let i = 0; i < corked.length; i++) {
      const [type, msg] = corked[i]
      writeUint24le(msg.length + 1, plain, offset)
      plain[offset + 3] = type
      msg.copy(plain, offset + 4)
      offset += 4 + msg.length
    }

    writeUint24le(length, all, 0)

    this._tx.next(plain, cipher)
    this.push(all)
  }

  _handshakeRT () {
    const split = noise.writeMessage(this._handshake, EMPTY, this._handshakeBuffer.slice(3))
    writeUint24le(noise.writeMessage.bytes, this._handshakeBuffer, 0)
    this.push(this._handshakeBuffer.slice(0, 3 + noise.writeMessage.bytes))
    if (split) this._onhandshake(split)
  }

  _onhandshake ({ tx, rx }) {
    this.remotePublicKey = Buffer.from(this._handshake.rs)

    noise.destroy(this._handshake)
    this._handshake = null

    // the key copy is suboptimal but to reduce secure memory overhead on linux with default settings
    // better fix is to batch mallocs in noise-protocol

    const header = Buffer.allocUnsafe(HEADERBYTES + 3)
    this._tx = new Push(Buffer.from(tx.slice(0, KEYBYTES)), undefined, header.slice(3))
    this._rx = new Pull(Buffer.from(rx.slice(0, KEYBYTES)))

    sodium.sodium_free(rx)
    sodium.sodium_free(tx)

    this.emit('handshake')
    if (this.destroying) return

    writeUint24le(HEADERBYTES, header, 0)
    this.push(header)
    this.uncork()
  }

  _destroy (cb) {
    if (this._handshake !== null) {
      noise.destroy(this._handshake)
      this._handshake = null
    }

    cb(null)
  }

  send (type, msg) {
    if (this._corked !== null) {
      this._corked.push([type, this._encode(type, msg)])
      return
    }

    // TODO: inplace encode it instead
    const buf = this._encode(type, msg)

    const all = Buffer.allocUnsafe(buf.length + 4 + ABYTES)
    const cipher = all.slice(3)
    const plain = all.slice(4, all.length - ABYTES + 1)

    writeUint24le(buf.length + ABYTES + 1, all, 0)
    plain[0] = type
    buf.copy(plain, 1)

    this._tx.next(plain, cipher)
    this.push(all)
  }
}

function readUint24le (buf, offset) {
  return buf[offset] + 256 * buf[offset + 1] + 65536 * buf[offset + 2]
}

function writeUint24le (n, buf, offset) {
  buf[offset] = (n & 255)
  buf[offset + 1] = (n >>> 8) & 255
  buf[offset + 2] = (n >>> 16) & 255
}

function hex (buf) {
  return buf && buf.toString('hex')
}
