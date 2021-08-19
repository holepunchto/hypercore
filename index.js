const { EventEmitter } = require('events')
const raf = require('random-access-file')
const isOptions = require('is-options')
const hypercoreCrypto = require('hypercore-crypto')
const c = require('compact-encoding')
const NoiseSecretStream = require('noise-secret-stream')
const codecs = require('codecs')

const fsctl = requireMaybe('fsctl') || { lock: noop, sparse: noop }

const Replicator = require('./lib/replicator')
const Extensions = require('./lib/extensions')
const Core = require('./lib/core')

const promises = Symbol.for('hypercore.promises')
const inspect = Symbol.for('nodejs.util.inspect.custom')

module.exports = class Hypercore extends EventEmitter {
  constructor (storage, key, opts) {
    super()

    if (isOptions(storage)) {
      opts = storage
      storage = null
      key = null
    } else if (isOptions(key)) {
      opts = key
      key = null
    }

    if (!opts) opts = {}
    if (!storage) storage = opts.storage

    this[promises] = true

    this.storage = null
    this.crypto = opts.crypto || hypercoreCrypto
    this.core = null
    this.replicator = null
    this.extensions = opts.extensions || new Extensions(this)

    this.valueEncoding = null
    this.key = key || null
    this.discoveryKey = null
    this.readable = true
    this.writable = false
    this.opened = false
    this.closed = false
    this.sessions = opts._sessions || [this]
    this.sign = opts.sign || null

    this.opening = opts._opening || this._open(key, storage, opts)
    this.opening.catch(noop)
  }

  [inspect] (depth, opts) {
    let indent = ''
    if (typeof opts.indentationLvl === 'number') {
      while (indent.length < opts.indentationLvl) indent += ' '
    }

    return this.constructor.name + '(\n' +
      indent + '  key: ' + opts.stylize((toHex(this.key)), 'string') + '\n' +
      indent + '  discoveryKey: ' + opts.stylize(toHex(this.discoveryKey), 'string') + '\n' +
      indent + '  opened: ' + opts.stylize(this.opened, 'boolean') + '\n' +
      indent + '  writable: ' + opts.stylize(this.writable, 'boolean') + '\n' +
      indent + '  sessions: ' + opts.stylize(this.sessions.length, 'number') + '\n' +
      indent + '  peers: [ ' + opts.stylize(this.peers.length, 'number') + ' ]\n' +
      indent + '  length: ' + opts.stylize(this.length, 'number') + '\n' +
      indent + '  byteLength: ' + opts.stylize(this.byteLength, 'number') + '\n' +
      indent + ')'
  }

  static createProtocolStream (isInitiator, opts) {
    const noiseStream = new NoiseSecretStream(isInitiator, null, opts)
    return noiseStream.rawStream
  }

  session (opts = {}) {
    const Clz = opts.class || Hypercore
    const keyPair = opts.keyPair && opts.keyPair.secretKey && { ...opts.keyPair }

    // This only works if the hypercore was fully loaded,
    // but we only do this to validate the keypair to help catch bugs so yolo
    if (this.key && keyPair) keyPair.publicKey = this.key

    const s = new Clz(this.storage, this.key, {
      ...opts,
      sign: opts.sign || (keyPair && keyPair.secretKey && Core.createSigner(this.crypto, keyPair)) || this.sign,
      valueEncoding: this.valueEncoding,
      extensions: this.extensions,
      _opening: this.opening,
      _sessions: this.sessions
    })

    s._initSession(this)
    this.sessions.push(s)

    return s
  }

  _initSession (o) {
    if (!this.sign) this.sign = o.sign
    this.crypto = o.crypto
    this.opened = o.opened
    this.key = o.key
    this.discoveryKey = o.discoveryKey
    this.core = o.core
    this.replicator = o.replicator
    this.writable = !!this.sign
  }

  async close () {
    await this.opening

    const i = this.sessions.indexOf(this)
    if (i === -1) return

    this.sessions.splice(i, 1)
    this.readable = false
    this.writable = false
    this.closed = true

    if (this.sessions.length) {
      // wait a tick
      await Promise.resolve()
      // emit "fake" close as this is a session
      this.emit('close', false)
      return
    }

    await this.core.close()

    this.emit('close', true)
  }

  replicate (isInitiator, opts = {}) {
    let outerStream = isStream(isInitiator)
      ? isInitiator
      : opts.stream
    let noiseStream = null

    if (outerStream) {
      noiseStream = outerStream.noiseStream
    } else {
      outerStream = Hypercore.createProtocolStream(isInitiator, opts)
      noiseStream = outerStream.noiseStream
    }
    if (!noiseStream) throw new Error('Invalid stream passed to replicate')

    if (!noiseStream.userData) {
      const protocol = Replicator.createProtocol(noiseStream)
      noiseStream.userData = protocol
      noiseStream.on('error', noop) // All noise errors already propagate through outerStream
    }

    const protocol = noiseStream.userData
    if (this.opened) {
      this.replicator.joinProtocol(protocol, this.key, this.discoveryKey)
    } else {
      this.opening.then(() => this.replicator.joinProtocol(protocol, this.key, this.discoveryKey), protocol.destroy.bind(protocol))
    }

    return outerStream
  }

  get length () {
    return this.core === null ? 0 : this.core.tree.length
  }

  get byteLength () {
    return this.core === null ? 0 : this.core.tree.byteLength
  }

  get fork () {
    return this.core === null ? 0 : this.core.tree.fork
  }

  get peers () {
    return this.replicator === null ? [] : this.replicator.peers
  }

  ready () {
    return this.opening
  }

  async _open (key, storage, opts) {
    if (opts.preload) opts = { ...opts, ...(await opts.preload()) }

    this.valueEncoding = opts.valueEncoding ? c.from(codecs(opts.valueEncoding)) : null

    const keyPair = (key && opts.keyPair)
      ? { ...opts.keyPair, publicKey: key }
      : key
        ? { publicKey: key, secretKey: null }
        : opts.keyPair

    if (opts.from) {
      const from = opts.from
      await from.opening
      this._initSession(from)
      this.sessions = from.sessions
      this.storage = from.storage
      if (!this.sign && keyPair && keyPair.secretKey) this.sign = Core.createSigner(this.crypto, keyPair)
      return
    }

    if (!this.storage) this.storage = defaultStorage(opts.storage || storage)

    this.core = await Core.open(this.storage, {
      crypto: this.crypto,
      keyPair,
      onupdate: this._oncoreupdate.bind(this)
    })

    this.replicator = new Replicator(this.core, {
      onupdate: this._onpeerupdate.bind(this)
    })

    if (!this.sign) this.sign = opts.sign || this.core.defaultSign

    this.discoveryKey = this.crypto.discoveryKey(this.core.header.signer.publicKey)
    this.key = this.core.header.signer.publicKey
    this.writable = !!this.sign

    this.opened = true

    for (let i = 0; i < this.sessions.length; i++) {
      const s = this.sessions[i]
      if (s !== this) s._initSession(this)
      s.emit('ready')
    }
  }

  _oncoreupdate (status, bitfield, value, from) {
    if (status !== 0) {
      for (let i = 0; i < this.sessions.length; i++) {
        if ((status & 0b10) !== 0) this.sessions[i].emit('truncate', this.core.tree.fork)
        if ((status & 0b01) !== 0) this.sessions[i].emit('append')
      }

      this.replicator.broadcastInfo()
    }

    if (bitfield && !bitfield.drop) { // TODO: support drop!
      for (let i = 0; i < bitfield.length; i++) {
        this.replicator.broadcastBlock(bitfield.start + i)
      }
    }

    if (value) {
      for (let i = 0; i < this.sessions.length; i++) {
        this.sessions[i].emit('download', bitfield.start, value, from)
      }
    }
  }

  _onpeerupdate (added, peer) {
    if (added) this.extensions.update(peer)
    const name = added ? 'peer-add' : 'peer-remove'

    for (let i = 0; i < this.sessions.length; i++) {
      this.sessions[i].emit(name, peer)
    }
  }

  async update () {
    if (this.opened === false) await this.opening
    // TODO: add an option where a writer can bootstrap it's state from the network also
    if (this.writable) return false
    return this.replicator.requestUpgrade()
  }

  async seek (bytes) {
    if (this.opened === false) await this.opening

    const s = this.core.tree.seek(bytes)

    return (await s.update()) || this.replicator.requestSeek(s)
  }

  async has (index) {
    if (this.opened === false) await this.opening

    return this.core.bitfield.get(index)
  }

  async get (index, opts) {
    if (this.opened === false) await this.opening
    const encoding = (opts && opts.valueEncoding && c.from(codecs(opts.valueEncoding))) || this.valueEncoding

    if (this.core.bitfield.get(index)) return decode(encoding, await this.core.blocks.get(index))
    if (opts && opts.onwait) opts.onwait(index)

    return decode(encoding, await this.replicator.requestBlock(index))
  }

  download (range) {
    const start = (range && range.start) || 0
    const end = typeof (range && range.end) === 'number' ? range.end : -1 // download all
    const linear = !!(range && range.linear)

    // TODO: support range.blocks

    const r = Replicator.createRange(start, end, linear)

    if (this.opened) this.replicator.addRange(r)
    else this.opening.then(() => this.replicator.addRange(r))

    return r
  }

  undownload (range) {
    range.destroy(null)
  }

  async truncate (newLength = 0, fork = -1) {
    if (this.opened === false) await this.opening
    if (this.writable === false) throw new Error('Core is not writable')

    if (fork === -1) fork = this.core.tree.fork + 1
    await this.core.truncate(newLength, fork, this.sign)

    // TODO: Should propagate from an event triggered by the oplog
    this.replicator.updateAll()
  }

  async append (blocks) {
    if (this.opened === false) await this.opening
    if (this.writable === false) throw new Error('Core is not writable')

    const blks = Array.isArray(blocks) ? blocks : [blocks]
    const buffers = new Array(blks.length)

    for (let i = 0; i < blks.length; i++) {
      const blk = blks[i]

      const buf = Buffer.isBuffer(blk)
        ? blk
        : this.valueEncoding
          ? c.encode(this.valueEncoding, blk)
          : Buffer.from(blk)

      buffers[i] = buf
    }

    return await this.core.append(buffers, this.sign)
  }

  registerExtension (name, handlers) {
    return this.extensions.register(name, handlers)
  }

  // called by the extensions
  onextensionupdate () {
    if (this.replicator !== null) this.replicator.broadcastOptions()
  }
}

function noop () {}

function defaultStorage (storage) {
  if (typeof storage !== 'string') return storage
  const directory = storage
  return function createFile (name) {
    const lock = name === 'oplog' ? fsctl.lock : null
    const sparse = name !== 'oplog' ? fsctl.sparse : null
    return raf(name, { directory, lock, sparse })
  }
}

function decode (enc, buf) {
  return enc ? c.decode(enc, buf) : buf
}

function isStream (s) {
  return typeof s === 'object' && s && typeof s.pipe === 'function'
}

function requireMaybe (name) {
  try {
    return require(name)
  } catch (_) {
    return null
  }
}

function toHex (buf) {
  return buf && buf.toString('hex')
}
