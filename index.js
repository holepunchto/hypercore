const { EventEmitter } = require('events')
const raf = require('random-access-file')
const isOptions = require('is-options')
const hypercoreCrypto = require('hypercore-crypto')
const c = require('compact-encoding')
const b4a = require('b4a')
const Xache = require('xache')
const NoiseSecretStream = require('@hyperswarm/secret-stream')
const Protomux = require('protomux')
const codecs = require('codecs')

const fsctl = requireMaybe('fsctl') || { lock: noop, sparse: noop }

const Replicator = require('./lib/replicator')
const Core = require('./lib/core')
const BlockEncryption = require('./lib/block-encryption')
const { ReadStream, WriteStream } = require('./lib/streams')
const { BAD_ARGUMENT, SESSION_CLOSED, SESSION_NOT_WRITABLE, SNAPSHOT_NOT_AVAILABLE } = require('./lib/errors')

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

    if (key && typeof key === 'string') {
      key = b4a.from(key, 'hex')
    }

    if (!opts) opts = {}

    if (!opts.crypto && key && key.byteLength !== 32) {
      throw BAD_ARGUMENT('Hypercore key should be 32 bytes')
    }

    if (!storage) storage = opts.storage

    this[promises] = true

    this.storage = null
    this.crypto = opts.crypto || hypercoreCrypto
    this.core = null
    this.replicator = null
    this.encryption = null
    this.extensions = new Map()
    this.cache = opts.cache === true ? new Xache({ maxSize: 65536, maxAge: 0 }) : (opts.cache || null)

    this.valueEncoding = null
    this.encodeBatch = null
    this.activeRequests = []

    this.key = key || null
    this.keyPair = null
    this.readable = true
    this.writable = false
    this.opened = false
    this.closed = false
    this.snapshotted = !!opts.snapshot
    this.sparse = opts.sparse !== false
    this.sessions = opts._sessions || [this]
    this.auth = opts.auth || null
    this.autoClose = !!opts.autoClose

    this.closing = null
    this.opening = this._openSession(key, storage, opts)
    this.opening.catch(noop)

    this._preappend = preappend.bind(this)
    this._snapshot = null
    this._findingPeers = 0
  }

  [inspect] (depth, opts) {
    let indent = ''
    if (typeof opts.indentationLvl === 'number') {
      while (indent.length < opts.indentationLvl) indent += ' '
    }

    let peers = ''
    const min = Math.min(this.peers.length, 5)

    for (let i = 0; i < min; i++) {
      const peer = this.peers[i]

      peers += indent + '    Peer(\n'
      peers += indent + '      remotePublicKey: ' + opts.stylize(toHex(peer.remotePublicKey), 'string') + '\n'
      peers += indent + '      remoteLength: ' + opts.stylize(peer.remoteLength, 'number') + '\n'
      peers += indent + '      remoteFork: ' + opts.stylize(peer.remoteFork, 'number') + '\n'
      peers += indent + '      remoteCanUpgrade: ' + opts.stylize(peer.remoteCanUpgrade, 'boolean') + '\n'
      peers += indent + '    )' + '\n'
    }

    if (this.peers.length > 5) {
      peers += indent + '  ... and ' + (this.peers.length - 5) + ' more\n'
    }

    if (peers) peers = '[\n' + peers + indent + '  ]'
    else peers = '[ ' + opts.stylize(0, 'number') + ' ]'

    return this.constructor.name + '(\n' +
      indent + '  key: ' + opts.stylize(toHex(this.key), 'string') + '\n' +
      indent + '  discoveryKey: ' + opts.stylize(toHex(this.discoveryKey), 'string') + '\n' +
      indent + '  opened: ' + opts.stylize(this.opened, 'boolean') + '\n' +
      indent + '  closed: ' + opts.stylize(this.closed, 'boolean') + '\n' +
      indent + '  snapshotted: ' + opts.stylize(this.snapshotted, 'boolean') + '\n' +
      indent + '  writable: ' + opts.stylize(this.writable, 'boolean') + '\n' +
      indent + '  length: ' + opts.stylize(this.length, 'number') + '\n' +
      indent + '  byteLength: ' + opts.stylize(this.byteLength, 'number') + '\n' +
      indent + '  fork: ' + opts.stylize(this.fork, 'number') + '\n' +
      indent + '  sessions: [ ' + opts.stylize(this.sessions.length, 'number') + ' ]\n' +
      indent + '  activeRequests: [ ' + opts.stylize(this.activeRequests.length, 'number') + ' ]\n' +
      indent + '  peers: ' + peers + '\n' +
      indent + ')'
  }

  static getProtocolMuxer (stream) {
    return stream.noiseStream.userData
  }

  static createProtocolStream (isInitiator, opts = {}) {
    let outerStream = Protomux.isProtomux(isInitiator)
      ? isInitiator.stream
      : isStream(isInitiator)
        ? isInitiator
        : opts.stream

    let noiseStream = null

    if (outerStream) {
      noiseStream = outerStream.noiseStream
    } else {
      noiseStream = new NoiseSecretStream(isInitiator, null, opts)
      outerStream = noiseStream.rawStream
    }
    if (!noiseStream) throw BAD_ARGUMENT('Invalid stream')

    if (!noiseStream.userData) {
      const protocol = new Protomux(noiseStream)

      if (opts.ondiscoverykey) {
        protocol.pair({ protocol: 'hypercore/alpha' }, opts.ondiscoverykey)
      }
      if (opts.keepAlive !== false) {
        noiseStream.setKeepAlive(5000)
        noiseStream.setTimeout(10000)
      }
      noiseStream.userData = protocol
    }

    return outerStream
  }

  static defaultStorage (storage, opts = {}) {
    if (typeof storage !== 'string') return storage
    const directory = storage
    const toLock = opts.lock || 'oplog'
    return function createFile (name) {
      const locked = name === toLock || name.endsWith('/' + toLock)
      const lock = locked ? fsctl.lock : null
      const sparse = locked ? null : null // fsctl.sparse, disable sparse on windows - seems to fail for some people. TODO: investigate
      return raf(name, { directory, lock, sparse })
    }
  }

  snapshot (opts) {
    return this.session({ ...opts, snapshot: true })
  }

  session (opts = {}) {
    if (this.closing) {
      // This makes the closing logic alot easier. If this turns out to be a problem
      // in practive, open an issue and we'll try to make a solution for it.
      throw SESSION_CLOSED('Cannot make sessions on a closing core')
    }

    const Clz = opts.class || Hypercore
    const s = new Clz(this.storage, this.key, {
      ...opts,
      _opening: this.opening,
      _sessions: this.sessions
    })

    s._passCapabilities(this)

    // Pass on the cache unless explicitly disabled.
    if (opts.cache !== false) s.cache = this.cache

    ensureEncryption(s, opts)

    this.sessions.push(s)

    return s
  }

  _passCapabilities (o) {
    if (!this.auth) this.auth = o.auth

    this.crypto = o.crypto
    this.key = o.key
    this.core = o.core
    this.replicator = o.replicator
    this.encryption = o.encryption
    this.writable = !!(this.auth && this.auth.sign)
    this.autoClose = o.autoClose

    if (this.snapshotted && this.core && !this._snapshot) this._updateSnapshot()
  }

  async _openFromExisting (from, opts) {
    await from.opening

    this._passCapabilities(from)
    this.sessions = from.sessions
    this.storage = from.storage
    this.replicator.findingPeers += this._findingPeers

    ensureEncryption(this, opts)

    this.sessions.push(this)
  }

  async _openSession (key, storage, opts) {
    const isFirst = !opts._opening

    if (!isFirst) await opts._opening
    if (opts.preload) opts = { ...opts, ...(await opts.preload()) }

    const keyPair = (key && opts.keyPair)
      ? { ...opts.keyPair, publicKey: key }
      : key
        ? { publicKey: key, secretKey: null }
        : opts.keyPair

    // This only works if the hypercore was fully loaded,
    // but we only do this to validate the keypair to help catch bugs so yolo
    if (this.key && keyPair) keyPair.publicKey = this.key

    if (opts.auth) {
      this.auth = opts.auth
    } else if (opts.sign) {
      this.auth = Core.createAuth(this.crypto, keyPair, opts)
    } else if (keyPair && keyPair.secretKey) {
      this.auth = Core.createAuth(this.crypto, keyPair)
    }

    if (isFirst) {
      await this._openCapabilities(keyPair, storage, opts)
      // Only the root session should pass capabilities to other sessions.
      for (let i = 0; i < this.sessions.length; i++) {
        const s = this.sessions[i]
        if (s !== this) s._passCapabilities(this)
      }
    }

    if (!this.auth) this.auth = this.core.defaultAuth
    this.writable = !!this.auth.sign

    if (opts.valueEncoding) {
      this.valueEncoding = c.from(codecs(opts.valueEncoding))
    }
    if (opts.encodeBatch) {
      this.encodeBatch = opts.encodeBatch
    }

    // Start continous replication if not in sparse mode.
    if (!this.sparse) this.download({ start: 0, end: -1 })

    // This is a hidden option that's only used by Corestore.
    // It's required so that corestore can load a name from userData before 'ready' is emitted.
    if (opts._preready) await opts._preready(this)

    this.opened = true
    this.emit('ready')
  }

  async _openCapabilities (keyPair, storage, opts) {
    if (opts.from) return this._openFromExisting(opts.from, opts)

    this.storage = Hypercore.defaultStorage(opts.storage || storage)

    this.core = await Core.open(this.storage, {
      force: opts.force,
      createIfMissing: opts.createIfMissing,
      overwrite: opts.overwrite,
      keyPair,
      crypto: this.crypto,
      legacy: opts.legacy,
      auth: opts.auth,
      onupdate: this._oncoreupdate.bind(this)
    })

    if (opts.userData) {
      for (const [key, value] of Object.entries(opts.userData)) {
        await this.core.userData(key, value)
      }
    }

    this.key = this.core.header.signer.publicKey
    this.keyPair = this.core.header.signer

    this.replicator = new Replicator(this.core, this.key, {
      eagerUpdate: true,
      allowFork: opts.allowFork !== false,
      onpeerupdate: this._onpeerupdate.bind(this),
      onupload: this._onupload.bind(this)
    })

    this.replicator.findingPeers += this._findingPeers

    if (!this.encryption && opts.encryptionKey) {
      this.encryption = new BlockEncryption(opts.encryptionKey, this.key)
    }
  }

  _updateSnapshot () {
    const prev = this._snapshot
    const next = this._snapshot = {
      length: this.core.tree.length,
      byteLength: this.core.tree.byteLength,
      fork: this.core.tree.fork,
      compatLength: this.core.tree.length
    }

    if (!prev) return true
    return prev.length !== next.length || prev.fork !== next.fork
  }

  close () {
    if (this.closing) return this.closing
    this.closing = this._close()
    return this.closing
  }

  async _close () {
    await this.opening

    const i = this.sessions.indexOf(this)
    if (i === -1) return

    this.sessions.splice(i, 1)
    this.readable = false
    this.writable = false
    this.closed = true
    this.opened = false

    const gc = []
    for (const ext of this.extensions.values()) {
      if (ext.session === this) gc.push(ext)
    }
    for (const ext of gc) ext.destroy()

    if (this.replicator !== null) {
      this.replicator.findingPeers -= this._findingPeers
      this.replicator.clearRequests(this.activeRequests)
    }

    this._findingPeers = 0

    if (this.sessions.length) {
      // if this is the last session and we are auto closing, trigger that first to enforce error handling
      if (this.sessions.length === 1 && this.autoClose) await this.sessions[0].close()
      // emit "fake" close as this is a session
      this.emit('close', false)
      return
    }

    await this.core.close()

    this.emit('close', true)
  }

  replicate (isInitiator, opts = {}) {
    // Only limitation here is that ondiscoverykey doesn't work atm when passing a muxer directly,
    // because it doesn't really make a lot of sense.
    if (Protomux.isProtomux(isInitiator)) return this._attachToMuxer(isInitiator, opts)

    const protocolStream = Hypercore.createProtocolStream(isInitiator, opts)
    const noiseStream = protocolStream.noiseStream
    const protocol = noiseStream.userData

    this._attachToMuxer(protocol, opts)

    return protocolStream
  }

  _attachToMuxer (mux, opts) {
    // If the user wants to, we can make this replication run in a session
    // that way the core wont close "under them" during replication
    if (opts.session) {
      const s = this.session()
      mux.stream.on('close', () => s.close().catch(noop))
    }

    if (this.opened) {
      this.replicator.attachTo(mux)
    } else {
      this.opening.then(() => this.replicator.attachTo(mux), mux.destroy.bind(mux))
    }

    return mux
  }

  get discoveryKey () {
    return this.replicator === null ? null : this.replicator.discoveryKey
  }

  get length () {
    if (this._snapshot) return this._snapshot.length
    if (this.core === null) return 0
    if (!this.sparse) return this.contiguousLength
    return this.core.tree.length
  }

  get byteLength () {
    if (this._snapshot) return this._snapshot.byteLength
    if (this.core === null) return 0
    if (!this.sparse) return this.contiguousByteLength
    return this.core.tree.byteLength - (this.core.tree.length * this.padding)
  }

  get contiguousLength () {
    return this.core === null ? 0 : this.core.header.contiguousLength
  }

  get contiguousByteLength () {
    return 0
  }

  get fork () {
    return this.core === null ? 0 : this.core.tree.fork
  }

  get peers () {
    return this.replicator === null ? [] : this.replicator.peers
  }

  get encryptionKey () {
    return this.encryption && this.encryption.key
  }

  get padding () {
    return this.encryption === null ? 0 : this.encryption.padding
  }

  ready () {
    return this.opening
  }

  _onupload (index, value, from) {
    const byteLength = value.byteLength - this.padding

    for (let i = 0; i < this.sessions.length; i++) {
      this.sessions[i].emit('upload', index, byteLength, from)
    }
  }

  _oncoreupdate (status, bitfield, value, from) {
    if (status !== 0) {
      const truncated = (status & 0b10) !== 0
      const appended = (status & 0b01) !== 0

      if (truncated) {
        this.replicator.ontruncate(bitfield.start)
      }

      for (let i = 0; i < this.sessions.length; i++) {
        const s = this.sessions[i]

        if (truncated) {
          if (s.cache) s.cache.clear()
          s.emit('truncate', bitfield.start, this.core.tree.fork)
          // If snapshotted, make sure to update our compat so we can fail gets
          if (s._snapshot && bitfield.start < s._snapshot.compatLength) s._snapshot.compatLength = bitfield.start
        }

        if (appended) {
          s.emit('append')
        }
      }

      this.replicator.onupgrade()
    }

    if (bitfield) {
      this.replicator.onhave(bitfield.start, bitfield.length, bitfield.drop)
    }

    if (value) {
      const byteLength = value.byteLength - this.padding

      for (let i = 0; i < this.sessions.length; i++) {
        this.sessions[i].emit('download', bitfield.start, byteLength, from)
      }
    }
  }

  _onpeerupdate (added, peer) {
    const name = added ? 'peer-add' : 'peer-remove'

    for (let i = 0; i < this.sessions.length; i++) {
      this.sessions[i].emit(name, peer)

      if (added) {
        for (const ext of this.sessions[i].extensions.values()) {
          peer.extensions.set(ext.name, ext)
        }
      }
    }
  }

  async setUserData (key, value) {
    if (this.opened === false) await this.opening
    return this.core.userData(key, value)
  }

  async getUserData (key) {
    if (this.opened === false) await this.opening
    for (const { key: savedKey, value } of this.core.header.userData) {
      if (key === savedKey) return value
    }
    return null
  }

  findingPeers () {
    this._findingPeers++
    if (this.replicator !== null && !this.closing) this.replicator.findingPeers++

    let once = true

    return () => {
      if (this.closing || !once) return
      once = false
      this._findingPeers--
      if (this.replicator !== null && --this.replicator.findingPeers === 0) {
        this.replicator.updateAll()
      }
    }
  }

  async update (opts) {
    if (this.opened === false) await this.opening
    if (this.closing !== null) return false

    // TODO: add an option where a writer can bootstrap it's state from the network also
    if (this.writable) {
      if (!this.snapshotted) return false
      return this._updateSnapshot()
    }

    const activeRequests = (opts && opts.activeRequests) || this.activeRequests
    const req = this.replicator.addUpgrade(activeRequests)

    if (!this.snapshotted) return req.promise
    if (!(await req.promise)) return false

    return this._updateSnapshot()
  }

  async seek (bytes, opts) {
    if (this.opened === false) await this.opening

    const s = this.core.tree.seek(bytes, this.padding)

    const offset = await s.update()
    if (offset) return offset

    if (this.closing !== null) throw SESSION_CLOSED()

    const activeRequests = (opts && opts.activeRequests) || this.activeRequests
    const req = this.replicator.addSeek(activeRequests, s)

    return req.promise
  }

  async has (index) {
    if (this.opened === false) await this.opening

    return this.core.bitfield.get(index)
  }

  async get (index, opts) {
    if (this.opened === false) await this.opening
    if (this.closing !== null) throw SESSION_CLOSED()
    if (this._snapshot !== null && index >= this._snapshot.compatLength) throw SNAPSHOT_NOT_AVAILABLE()

    const encoding = (opts && opts.valueEncoding && c.from(codecs(opts.valueEncoding))) || this.valueEncoding

    let req = this.cache && this.cache.get(index)
    if (!req) req = this._get(index, opts)

    let block = await req
    if (!block) return null

    if (this.encryption) {
      // Copy the block as it might be shared with other sessions.
      block = b4a.from(block)

      this.encryption.decrypt(index, block)
    }

    return this._decode(encoding, block)
  }

  async _get (index, opts) {
    let req

    if (this.core.bitfield.get(index)) {
      req = this.core.blocks.get(index)

      if (this.cache) this.cache.set(index, req)
    } else {
      if (opts && opts.wait === false) return null
      if (opts && opts.onwait) opts.onwait(index)

      const activeRequests = (opts && opts.activeRequests) || this.activeRequests

      req = this._cacheOnResolve(
        index,
        this.replicator
          .addBlock(activeRequests, index)
          .promise,
        this.core.tree.fork
      )
    }

    return req
  }

  async _cacheOnResolve (index, req, fork) {
    const block = await req

    if (this.cache && fork === this.core.tree.fork) {
      this.cache.set(index, Promise.resolve(block))
    }

    return block
  }

  createReadStream (opts) {
    return new ReadStream(this, opts)
  }

  createWriteStream (opts) {
    return new WriteStream(this, opts)
  }

  download (range) {
    const reqP = this._download(range)

    // do not crash in the background...
    reqP.catch(noop)

    // TODO: turn this into an actual object...
    return {
      async downloaded () {
        const req = await reqP
        return req.promise
      },
      destroy () {
        reqP.then(req => req.context && req.context.detach(req), noop)
      }
    }
  }

  async _download (range) {
    if (this.opened === false) await this.opening
    const activeRequests = (range && range.activeRequests) || this.activeRequests
    return this.replicator.addRange(activeRequests, range)
  }

  // TODO: get rid of this / deprecate it?
  undownload (range) {
    range.destroy(null)
  }

  // TODO: get rid of this / deprecate it?
  cancel (request) {
    // Do nothing for now
  }

  async truncate (newLength = 0, fork = -1) {
    if (this.opened === false) await this.opening
    if (this.writable === false) throw SESSION_NOT_WRITABLE()

    if (fork === -1) fork = this.core.tree.fork + 1
    await this.core.truncate(newLength, fork, this.auth)

    // TODO: Should propagate from an event triggered by the oplog
    this.replicator.updateAll()
  }

  async append (blocks) {
    if (this.opened === false) await this.opening
    if (this.writable === false) throw SESSION_NOT_WRITABLE()

    blocks = Array.isArray(blocks) ? blocks : [blocks]

    const preappend = this.encryption && this._preappend

    const buffers = this.encodeBatch !== null ? this.encodeBatch(blocks) : new Array(blocks.length)

    if (this.encodeBatch === null) {
      for (let i = 0; i < blocks.length; i++) {
        buffers[i] = this._encode(this.valueEncoding, blocks[i])
      }
    }

    return await this.core.append(buffers, this.auth, { preappend })
  }

  async treeHash (length) {
    if (length === undefined) {
      await this.ready()
      length = this.core.length
    }

    const roots = await this.core.tree.getRoots(length)
    return this.crypto.tree(roots)
  }

  registerExtension (name, handlers = {}) {
    if (this.extensions.has(name)) {
      const ext = this.extensions.get(name)
      ext.handlers = handlers
      ext.encoding = c.from(codecs(handlers.encoding) || c.buffer)
      ext.session = this
      return ext
    }

    const ext = {
      name,
      handlers,
      encoding: c.from(codecs(handlers.encoding) || c.buffer),
      session: this,
      send (message, peer) {
        const buffer = c.encode(this.encoding, message)
        peer.extension(name, buffer)
      },
      broadcast (message) {
        const buffer = c.encode(this.encoding, message)
        for (const peer of this.session.peers) {
          peer.extension(name, buffer)
        }
      },
      destroy () {
        for (const peer of this.session.peers) {
          if (peer.extensions.get(name) === ext) peer.extensions.delete(name)
        }
        this.session.extensions.delete(name)
      },
      _onmessage (state, peer) {
        const m = this.encoding.decode(state)
        if (this.handlers.onmessage) this.handlers.onmessage(m, peer)
      }
    }

    this.extensions.set(name, ext)
    for (const peer of this.peers) {
      peer.extensions.set(name, ext)
    }

    return ext
  }

  _encode (enc, val) {
    const state = { start: this.padding, end: this.padding, buffer: null }

    if (b4a.isBuffer(val)) {
      if (state.start === 0) return val
      state.end += val.byteLength
    } else if (enc) {
      enc.preencode(state, val)
    } else {
      val = b4a.from(val)
      if (state.start === 0) return val
      state.end += val.byteLength
    }

    state.buffer = b4a.allocUnsafe(state.end)

    if (enc) enc.encode(state, val)
    else state.buffer.set(val, state.start)

    return state.buffer
  }

  _decode (enc, block) {
    if (this.padding) block = block.subarray(this.padding)
    if (enc) return c.decode(enc, block)
    return block
  }
}

function noop () {}

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
  return buf && b4a.toString(buf, 'hex')
}

function preappend (blocks) {
  const offset = this.core.tree.length
  const fork = this.core.tree.fork

  for (let i = 0; i < blocks.length; i++) {
    this.encryption.encrypt(offset + i, blocks[i], fork)
  }
}

function ensureEncryption (core, opts) {
  if (!opts.encryptionKey) return
  // Only override the block encryption if its either not already set or if
  // the caller provided a different key.
  if (core.encryption && b4a.equals(core.encryption.key, opts.encryptionKey)) return
  core.encryption = new BlockEncryption(opts.encryptionKey, core.key)
}
