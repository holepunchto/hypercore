const { EventEmitter } = require('events')
const isOptions = require('is-options')
const crypto = require('hypercore-crypto')
const CoreStorage = require('hypercore-storage')
const c = require('compact-encoding')
const b4a = require('b4a')
const NoiseSecretStream = require('@hyperswarm/secret-stream')
const Protomux = require('protomux')
const id = require('hypercore-id-encoding')
const safetyCatch = require('safety-catch')
const unslab = require('unslab')

const Core = require('./lib/core')
const BlockEncryption = require('./lib/block-encryption')
const Info = require('./lib/info')
const Download = require('./lib/download')
const { manifestHash, createManifest } = require('./lib/verifier')
const { ReadStream, WriteStream, ByteStream } = require('./lib/streams')
const {
  ASSERTION,
  BAD_ARGUMENT,
  SESSION_CLOSED,
  SESSION_NOT_WRITABLE,
  SNAPSHOT_NOT_AVAILABLE,
  DECODING_ERROR
} = require('hypercore-errors')

const inspect = Symbol.for('nodejs.util.inspect.custom')

// Hypercore actually does not have any notion of max/min block sizes
// but we enforce 15mb to ensure smooth replication (each block is transmitted atomically)
const MAX_SUGGESTED_BLOCK_SIZE = 15 * 1024 * 1024

class Hypercore extends EventEmitter {
  constructor (storage, key, opts) {
    super()

    if (isOptions(storage) && !storage.db) {
      opts = storage
      storage = null
      key = opts.key || null
    } else if (isOptions(key)) {
      opts = key
      key = opts.key || null
    }

    if (key && typeof key === 'string') key = id.decode(key)
    if (!opts) opts = {}

    if (!storage) storage = opts.storage

    this.core = opts.core || null
    this.state = null
    this.encryption = null
    this.extensions = new Map()

    this.valueEncoding = null
    this.encodeBatch = null
    this.activeRequests = []

    this.keyPair = opts.keyPair || null
    this.readable = true
    this.writable = false
    this.exclusive = false
    this.opened = false
    this.closed = false
    this.snapshotted = !!opts.snapshot
    this.draft = !!opts.draft
    this.onwait = opts.onwait || null
    this.wait = opts.wait !== false
    this.timeout = opts.timeout || 0
    this.closing = null
    this.opening = null

    this._readonly = opts.writable === false
    this._preappend = preappend.bind(this)
    this._snapshot = null
    this._findingPeers = 0
    this._active = opts.active !== false

    this.opening = this._open(storage, key, opts)
    this.opening.catch(safetyCatch)
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
      indent + '  id: ' + opts.stylize(this.id, 'string') + '\n' +
      indent + '  key: ' + opts.stylize(toHex(this.key), 'string') + '\n' +
      indent + '  discoveryKey: ' + opts.stylize(toHex(this.discoveryKey), 'string') + '\n' +
      indent + '  opened: ' + opts.stylize(this.opened, 'boolean') + '\n' +
      indent + '  closed: ' + opts.stylize(this.closed, 'boolean') + '\n' +
      indent + '  snapshotted: ' + opts.stylize(this.snapshotted, 'boolean') + '\n' +
      indent + '  writable: ' + opts.stylize(this.writable, 'boolean') + '\n' +
      indent + '  length: ' + opts.stylize(this.length, 'number') + '\n' +
      indent + '  fork: ' + opts.stylize(this.fork, 'number') + '\n' +
      indent + '  sessions: [ ' + opts.stylize(this.sessions.length, 'number') + ' ]\n' +
      indent + '  activeRequests: [ ' + opts.stylize(this.activeRequests.length, 'number') + ' ]\n' +
      indent + '  peers: ' + peers + '\n' +
      indent + ')'
  }

  static MAX_SUGGESTED_BLOCK_SIZE = MAX_SUGGESTED_BLOCK_SIZE

  static key (manifest, { compat, version, namespace } = {}) {
    if (b4a.isBuffer(manifest)) manifest = { version, signers: [{ publicKey: manifest, namespace }] }
    return compat ? manifest.signers[0].publicKey : manifestHash(createManifest(manifest))
  }

  static discoveryKey (key) {
    return crypto.discoveryKey(key)
  }

  static getProtocolMuxer (stream) {
    return stream.noiseStream.userData
  }

  static createCore (storage, opts) {
    return new Core(Hypercore.defaultStorage(storage), { autoClose: false, ...opts })
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
      const protocol = Protomux.from(noiseStream)

      if (opts.keepAlive !== false) {
        noiseStream.setKeepAlive(5000)
      }
      noiseStream.userData = protocol
    }

    if (opts.ondiscoverykey) {
      noiseStream.userData.pair({ protocol: 'hypercore/alpha' }, opts.ondiscoverykey)
    }

    return outerStream
  }

  static defaultStorage (storage, opts = {}) {
    if (typeof storage !== 'string') {
      // todo: validate it is rocksdb instance
      return storage
    }

    const directory = storage
    return new CoreStorage(directory, opts)
  }

  snapshot (opts) {
    return this.session({ ...opts, snapshot: true })
  }

  session (opts = {}) {
    if (this.closing) {
      // This makes the closing logic a lot easier. If this turns out to be a problem
      // in practice, open an issue and we'll try to make a solution for it.
      throw SESSION_CLOSED('Cannot make sessions on a closing core')
    }

    const wait = opts.wait === false ? false : this.wait
    const writable = opts.writable === false ? false : !this._readonly
    const onwait = opts.onwait === undefined ? this.onwait : opts.onwait
    const timeout = opts.timeout === undefined ? this.timeout : opts.timeout
    const Clz = opts.class || Hypercore
    const s = new Clz(null, this.key, {
      ...opts,
      core: this.core,
      wait,
      onwait,
      timeout,
      writable,
      parent: this
    })

    return s
  }

  async setEncryptionKey (encryptionKey, opts) {
    if (!this.opened) await this.opening
    this.encryption = encryptionKey ? new BlockEncryption(encryptionKey, this.key, { compat: this.core.compat, ...opts }) : null
  }

  setKeyPair (keyPair) {
    this.keyPair = keyPair
  }

  setActive (bool) {
    const active = !!bool
    if (active === this._active || this.closing) return
    this._active = active
    if (!this.opened) return
    this.core.replicator.updateActivity(this._active ? 1 : -1)
  }

  _setupSession (parent) {
    if (!this.keyPair) this.keyPair = parent.keyPair
    this.writable = this._isWritable()

    if (parent.state) {
      this.state = this.draft ? parent.state.memoryOverlay() : this.snapshotted ? parent.state.snapshot() : parent.state.ref()
    }

    if (this.snapshotted && this.core && !this._snapshot) this._updateSnapshot()
  }

  async _open (storage, key, opts) {
    if (opts.preload) opts = { ...opts, ...(await opts.preload) }

    if (storage === null) storage = opts.storage || null
    if (key === null) key = opts.key || null

    if (this.core === null) initOnce(this, storage, key, opts)

    this.core.addSession(this)

    try {
      await this._openSession(key, opts)
    } catch (err) {
      this.core.removeSession(this)
      if (this.core.autoClose && this.sessions.length === 0) await this.core.close()
      if (this.exclusive) this.core.unlockExclusive()
      this.emit('close', this.sessions.length === 0)
      throw err
    }

    this.emit('ready')
  }

  async _openSession (key, opts) {
    if (this.core.opened === false) await this.core.ready()

    if (this.keyPair === null) this.keyPair = opts.keyPair || this.core.header.keyPair

    if (!this.core.encryption && opts.encryptionKey) {
      this.core.encryption = new BlockEncryption(opts.encryptionKey, this.key, { compat: this.core.compat, isBlockKey: opts.isBlockKey })
    }

    if (this.core.encryption) this.encryption = this.core.encryption

    this.writable = this._isWritable()

    if (opts.valueEncoding) {
      this.valueEncoding = c.from(opts.valueEncoding)
    }
    if (opts.encodeBatch) {
      this.encodeBatch = opts.encodeBatch
    }

    if (opts.parent) {
      if (opts.parent.state === null) await opts.parent.ready()
      this._setupSession(opts.parent)
    }

    if (opts.exclusive) {
      this.exclusive = true
      await this.core.lockExclusive()
    }

    if (opts.name) {
      // todo: need to make named sessions safe before ready
      // atm we always copy the state in passCapabilities
      const checkout = opts.checkout === undefined ? -1 : opts.checkout
      const state = this.state
      this.state = await this.core.createSession(opts.name, checkout, !!opts.overwrite)
      if (state) state.unref() // ref'ed above in setup session

      if (checkout !== -1) {
        await this.state.truncate(checkout, this.fork)
      }
    } else if (this.state === null) {
      this.state = this.core.state.ref()
    }

    if (opts.userData) {
      const batch = this.state.storage.createWriteBatch()
      for (const [key, value] of Object.entries(opts.userData)) {
        this.core.setUserData(batch, key, value)
      }
      await batch.flush()
    }

    if (opts.manifest && !this.core.header.manifest) {
      await this.core.setManifest(opts.manifest)
    }

    this.core.replicator.updateActivity(this._active ? 1 : 0)
    this.opened = true
  }

  get replicator () {
    return this.core === null ? null : this.core.replicator
  }

  _getSnapshot () {
    return {
      length: this.state.tree.length,
      byteLength: this.state.tree.byteLength,
      fork: this.state.tree.fork,
      compatLength: this.state.tree.length
    }
  }

  _updateSnapshot () {
    const prev = this._snapshot
    const next = this._snapshot = this._getSnapshot()

    if (!prev) return true
    return prev.length !== next.length || prev.fork !== next.fork
  }

  _isWritable () {
    return !this._readonly && !!(this.keyPair && this.keyPair.secretKey)
  }

  close ({ error } = {}) {
    if (this.closing) return this.closing

    this.closing = this._close(error || null)
    return this.closing
  }

  async _close (error) {
    if (this.opened === false) await this.opening
    if (this.closed === true) return

    this.core.removeSession(this)

    this.readable = false
    this.writable = false
    this.opened = false

    const gc = []
    for (const ext of this.extensions.values()) {
      if (ext.session === this) gc.push(ext)
    }
    for (const ext of gc) ext.destroy()

    this.core.replicator.findingPeers -= this._findingPeers
    this.core.replicator.clearRequests(this.activeRequests, error)
    this.core.replicator.updateActivity(this._active ? -1 : 0)

    this._findingPeers = 0

    this.state.unref()

    if (this.exclusive) this.core.unlockExclusive()

    if (this.core.sessions.length) {
      // emit "fake" close as this is a session
      this.closed = true
      this.emit('close', false)
      return
    }

    if (this.core.autoClose) await this.core.close()

    this.closed = true
    this.emit('close', true)
  }

  replicate (isInitiator, opts = {}) {
    // Only limitation here is that ondiscoverykey doesn't work atm when passing a muxer directly,
    // because it doesn't really make a lot of sense.
    if (Protomux.isProtomux(isInitiator)) return this._attachToMuxer(isInitiator, opts)

    // if same stream is passed twice, ignore the 2nd one before we make sessions etc
    if (isStream(isInitiator) && this._isAttached(isInitiator)) return isInitiator

    const protocolStream = Hypercore.createProtocolStream(isInitiator, opts)
    const noiseStream = protocolStream.noiseStream
    const protocol = noiseStream.userData

    this._attachToMuxer(protocol)

    return protocolStream
  }

  _isAttached (stream) {
    return stream.userData && this.core && this.core.replicator && this.core.replicator.attached(stream.userData)
  }

  _attachToMuxer (mux) {
    if (this.opened) {
      this.core.replicator.attachTo(mux)
    } else {
      const replicator = this.core.replicator
      this.opening.then(replicator.attachTo.bind(replicator, mux), mux.destroy.bind(mux))
    }

    return mux
  }

  get id () {
    return this.core === null ? null : this.core.id
  }

  get key () {
    return this.core === null ? null : this.core.key
  }

  get discoveryKey () {
    return this.core === null ? null : this.core.discoveryKey
  }

  get manifest () {
    return this.core === null ? null : this.core.manifest
  }

  get length () {
    if (this._snapshot) return this._snapshot.length
    return this.opened === false ? 0 : this.state.tree.length
  }

  get flushedLength () {
    if (this.opened === false) return 0
    if (this.state === this.core.state) return this.core.tree.length
    const flushed = this.state.flushedLength()

    return flushed === -1 ? this.state.tree.length : flushed
  }

  /**
   * Deprecated. Use `const { byteLength } = await core.info()`.
   */
  get byteLength () {
    if (this.opened === false) return 0
    if (this._snapshot) return this._snapshot.byteLength
    return this.state.tree.byteLength - (this.state.tree.length * this.padding)
  }

  get contiguousLength () {
    if (this.opened === false) return 0
    return Math.min(this.core.tree.length, this.core.header.hints.contiguousLength)
  }

  get contiguousByteLength () {
    return 0
  }

  get fork () {
    if (this.opened === false) return 0
    return this.core.tree.fork
  }

  get peers () {
    return this.opened === false ? [] : this.core.replicator.peers
  }

  get encryptionKey () {
    return this.encryption && this.encryption.key
  }

  get padding () {
    return this.encryption === null ? 0 : this.encryption.padding
  }

  get globalCache () {
    return this.opened === false ? null : this.core.globalCache
  }

  get sessions () {
    return this.opened === false ? [] : this.core.sessions
  }

  ready () {
    return this.opening
  }

  async setUserData (key, value, { flush = false } = {}) {
    if (this.opened === false) await this.opening
    await this.state.setUserData(key, value)
  }

  async getUserData (key) {
    if (this.opened === false) await this.opening
    const batch = this.state.storage.createReadBatch()
    const p = batch.getUserData(key)
    batch.tryFlush()
    return p
  }

  createTreeBatch () {
    return this.state.tree.batch()
  }

  findingPeers () {
    this._findingPeers++
    if (this.core !== null && !this.closing) this.core.replicator.findingPeers++

    let once = true

    return () => {
      if (this.closing || !once) return
      once = false
      this._findingPeers--
      if (this.core !== null && --this.core.replicator.findingPeers === 0) {
        this.core.replicator.updateAll()
      }
    }
  }

  async info (opts) {
    if (this.opened === false) await this.opening

    return Info.from(this, opts)
  }

  async update (opts) {
    if (this.opened === false) await this.opening
    if (this.closing !== null) return false

    if (this.writable && (!opts || opts.force !== true)) {
      if (!this.snapshotted) return false
      return this._updateSnapshot()
    }

    const remoteWait = this._shouldWait(opts, this.core.replicator.findingPeers > 0)

    let upgraded = false

    if (await this.core.replicator.applyPendingReorg()) {
      upgraded = true
    }

    if (!upgraded && remoteWait) {
      const activeRequests = (opts && opts.activeRequests) || this.activeRequests
      const req = this.core.replicator.addUpgrade(activeRequests)

      upgraded = await req.promise
    }

    if (!upgraded) return false
    if (this.snapshotted) return this._updateSnapshot()
    return true
  }

  async seek (bytes, opts) {
    if (this.opened === false) await this.opening
    if (!isValidIndex(bytes)) throw ASSERTION('seek is invalid')

    const tree = (opts && opts.tree) || this.state.tree
    const s = tree.seek(bytes, this.padding)

    const offset = await s.update()
    if (offset) return offset

    if (this.closing !== null) throw SESSION_CLOSED()

    if (!this._shouldWait(opts, this.wait)) return null

    const activeRequests = (opts && opts.activeRequests) || this.activeRequests
    const req = this.core.replicator.addSeek(activeRequests, s)

    const timeout = opts && opts.timeout !== undefined ? opts.timeout : this.timeout
    if (timeout) req.context.setTimeout(req, timeout)

    return req.promise
  }

  async has (start, end = start + 1) {
    if (this.opened === false) await this.opening
    if (!isValidIndex(start) || !isValidIndex(end)) throw ASSERTION('has range is invalid')

    if (end === start + 1) return this.state.bitfield.get(start)

    const i = this.state.bitfield.firstUnset(start)
    return i === -1 || i >= end
  }

  async get (index, opts) {
    if (this.opened === false) await this.opening
    if (!isValidIndex(index)) throw ASSERTION('block index is invalid')

    if (this.closing !== null) throw SESSION_CLOSED()

    const encoding = (opts && opts.valueEncoding && c.from(opts.valueEncoding)) || this.valueEncoding

    const req = this._get(index, opts)

    let block = await req
    if (!block) return null

    if (opts && opts.raw) return block

    if (this.encryption && (!opts || opts.decrypt !== false)) {
      // Copy the block as it might be shared with other sessions.
      block = b4a.from(block)

      this.encryption.decrypt(index, block)
    }

    return this._decode(encoding, block)
  }

  async clear (start, end = start + 1, opts) {
    if (this.opened === false) await this.opening
    if (this.closing !== null) throw SESSION_CLOSED()

    if (typeof end === 'object') {
      opts = end
      end = start + 1
    }

    if (!isValidIndex(start) || !isValidIndex(end)) throw ASSERTION('clear range is invalid')

    const cleared = (opts && opts.diff) ? { blocks: 0 } : null

    if (start >= end) return cleared
    if (start >= this.length) return cleared

    await this.state.clear(start, end, cleared)

    return cleared
  }

  async purge () {
    await this._closeAllSessions(null)
    await this.core.purge()
  }

  async _get (index, opts) {
    if (this.core.isFlushing) await this.core.flushed()

    const block = await readBlock(this.state.storage.createReadBatch(), index)

    if (block !== null) return block

    if (this.closing !== null) throw SESSION_CLOSED()

    // snapshot should check if core has block
    if (this._snapshot !== null) {
      checkSnapshot(this._snapshot, index)
      const coreBlock = await readBlock(this.core.state.storage.createReadBatch(), index)

      checkSnapshot(this._snapshot, index)
      if (coreBlock !== null) return coreBlock
    }

    // lets check the bitfield to see if we got it during the above async calls
    // this is the last resort before replication, so always safe.
    if (this.core.state.bitfield.get(index)) {
      return readBlock(this.state.storage.createReadBatch(), index)
    }

    if (!this._shouldWait(opts, this.wait)) return null

    if (opts && opts.onwait) opts.onwait(index, this)
    if (this.onwait) this.onwait(index, this)

    const activeRequests = (opts && opts.activeRequests) || this.activeRequests

    const req = this.core.replicator.addBlock(activeRequests, index)
    req.snapshot = index < this.length

    const timeout = opts && opts.timeout !== undefined ? opts.timeout : this.timeout
    if (timeout) req.context.setTimeout(req, timeout)

    const replicatedBlock = await req.promise
    if (this._snapshot !== null) checkSnapshot(this._snapshot, index)

    return maybeUnslab(replicatedBlock)
  }

  async restoreBatch (length, blocks) {
    if (this.opened === false) await this.opening
    return this.state.tree.restoreBatch(length)
  }

  _shouldWait (opts, defaultValue) {
    if (opts) {
      if (opts.wait === false) return false
      if (opts.wait === true) return true
    }
    return defaultValue
  }

  createReadStream (opts) {
    return new ReadStream(this, opts)
  }

  createWriteStream (opts) {
    return new WriteStream(this, opts)
  }

  createByteStream (opts) {
    return new ByteStream(this, opts)
  }

  download (range) {
    const req = this._download(range)

    // do not crash in the background...
    req.catch(safetyCatch)

    return new Download(req)
  }

  async _download (range) {
    if (this.opened === false) await this.opening

    const activeRequests = (range && range.activeRequests) || this.activeRequests

    return this.core.replicator.addRange(activeRequests, range)
  }

  // TODO: get rid of this / deprecate it?
  undownload (range) {
    range.destroy(null)
  }

  // TODO: get rid of this / deprecate it?
  cancel (request) {
    // Do nothing for now
  }

  async truncate (newLength = 0, opts = {}) {
    if (this.opened === false) await this.opening

    const {
      fork = this.state.tree.fork + 1,
      keyPair = this.keyPair,
      signature = null
    } = typeof opts === 'number' ? { fork: opts } : opts

    const isDefault = this.state === this.core.state
    const writable = !this._readonly && !!(signature || (keyPair && keyPair.secretKey))
    if (isDefault && writable === false && (newLength > 0 || fork !== this.state.tree.fork)) throw SESSION_NOT_WRITABLE()

    await this.state.truncate(newLength, fork, { keyPair, signature })

    // TODO: Should propagate from an event triggered by the oplog
    if (this.state === this.core.state) this.core.replicator.updateAll()
  }

  async append (blocks, opts = {}) {
    if (this.opened === false) await this.opening

    const isDefault = this.state === this.core.state
    const defaultKeyPair = this.state.name === null ? this.keyPair : null

    const { keyPair = defaultKeyPair, signature = null } = opts
    const writable = !this._readonly && !!(signature || (keyPair && keyPair.secretKey))

    if (isDefault && writable === false) throw SESSION_NOT_WRITABLE()

    blocks = Array.isArray(blocks) ? blocks : [blocks]

    const preappend = this.encryption && this._preappend

    const buffers = this.encodeBatch !== null ? this.encodeBatch(blocks) : new Array(blocks.length)

    if (this.encodeBatch === null) {
      for (let i = 0; i < blocks.length; i++) {
        buffers[i] = this._encode(this.valueEncoding, blocks[i])
      }
    }
    for (const b of buffers) {
      if (b.byteLength > MAX_SUGGESTED_BLOCK_SIZE) {
        throw BAD_ARGUMENT('Appended block exceeds the maximum suggested block size')
      }
    }

    return this.state.append(buffers, { keyPair, signature, preappend })
  }

  async treeHash (length) {
    if (length === undefined) {
      await this.ready()
      length = this.state.tree.length
    }

    const roots = await this.state.tree.getRoots(length)
    return crypto.tree(roots)
  }

  registerExtension (name, handlers = {}) {
    if (this.extensions.has(name)) {
      const ext = this.extensions.get(name)
      ext.handlers = handlers
      ext.encoding = c.from(handlers.encoding || c.buffer)
      ext.session = this
      return ext
    }

    const ext = {
      name,
      handlers,
      encoding: c.from(handlers.encoding || c.buffer),
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
    try {
      if (enc) return c.decode(enc, block)
    } catch {
      throw DECODING_ERROR()
    }
    return block
  }
}

module.exports = Hypercore

function isStream (s) {
  return typeof s === 'object' && s && typeof s.pipe === 'function'
}

function toHex (buf) {
  return buf && b4a.toString(buf, 'hex')
}

function preappend (blocks) {
  const offset = this.state.tree.length
  const fork = this.state.tree.fork

  for (let i = 0; i < blocks.length; i++) {
    this.encryption.encrypt(offset + i, blocks[i], fork)
  }
}

function isValidIndex (index) {
  return index === 0 || index > 0
}

function maybeUnslab (block) {
  // Unslab only when it takes up less then half the slab
  return block !== null && 2 * block.byteLength < block.buffer.byteLength ? unslab(block) : block
}

function checkSnapshot (snapshot, index) {
  if (index >= snapshot.compatLength) throw SNAPSHOT_NOT_AVAILABLE()
}

function readBlock (reader, index) {
  const promise = reader.getBlock(index)
  reader.tryFlush()
  return promise
}

function initOnce (session, storage, key, opts) {
  session.core = opts.core || new Core(Hypercore.defaultStorage(storage), {
    eagerUpgrade: true,
    notDownloadingLinger: opts.notDownloadingLinger,
    allowFork: opts.allowFork !== false,
    inflightRange: opts.inflightRange,
    compat: opts.compat,
    force: opts.force,
    createIfMissing: opts.createIfMissing,
    discoveryKey: opts.discoveryKey,
    overwrite: opts.overwrite,
    key,
    keyPair: opts.keyPair,
    legacy: opts.legacy,
    manifest: opts.manifest,
    globalCache: opts.globalCache || null // session is a temp option, not to be relied on unless you know what you are doing (no semver guarantees)
  })
}
