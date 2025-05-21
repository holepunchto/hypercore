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
const Info = require('./lib/info')
const Download = require('./lib/download')
const DefaultEncryption = require('./lib/default-encryption')
const caps = require('./lib/caps')
const { manifestHash, createManifest } = require('./lib/verifier')
const { ReadStream, WriteStream, ByteStream } = require('./lib/streams')
const { MerkleTree } = require('./lib/merkle-tree')
const {
  ASSERTION,
  BAD_ARGUMENT,
  SESSION_CLOSED,
  SESSION_MOVED,
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

    this.core = null
    this.state = null
    this.encryption = null
    this.extensions = new Map()

    this.valueEncoding = null
    this.encodeBatch = null
    this.activeRequests = []
    this.sessions = null
    this.ongc = null

    this.keyPair = opts.keyPair || null
    this.readable = true
    this.writable = false
    this.exclusive = false
    this.opened = false
    this.closed = false
    this.weak = !!opts.weak
    this.snapshotted = !!opts.snapshot
    this.onwait = opts.onwait || null
    this.wait = opts.wait !== false
    this.timeout = opts.timeout || 0
    this.preload = null
    this.closing = null
    this.opening = null

    this._readonly = opts.writable === false
    this._preappend = preappend.bind(this)
    this._snapshot = null
    this._findingPeers = 0
    this._active = opts.weak ? !!opts.active : opts.active !== false

    this._sessionIndex = -1
    this._stateIndex = -1 // maintained by session state
    this._monitorIndex = -1 // maintained by replication state

    this.opening = this._open(storage, key, opts)
    this.opening.catch(safetyCatch)

    this.on('newListener', maybeAddMonitor)
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

  static DefaultEncryption = DefaultEncryption

  static key (manifest, { compat, version, namespace } = {}) {
    if (b4a.isBuffer(manifest)) manifest = { version, signers: [{ publicKey: manifest, namespace }] }
    return compat ? manifest.signers[0].publicKey : manifestHash(createManifest(manifest))
  }

  static discoveryKey (key) {
    return crypto.discoveryKey(key)
  }

  static blockEncryptionKey (key, encryptionKey) {
    return DefaultEncryption.blockEncryptionKey(key, encryptionKey)
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
    if (CoreStorage.isCoreStorage(storage)) return storage

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
    if (opts.checkout !== undefined && !opts.name && !opts.atom) {
      throw ASSERTION('Checkouts are only supported on atoms or named sessions')
    }

    const wait = opts.wait === false ? false : this.wait
    const writable = opts.writable === undefined ? !this._readonly : opts.writable === true
    const onwait = opts.onwait === undefined ? this.onwait : opts.onwait
    const timeout = opts.timeout === undefined ? this.timeout : opts.timeout
    const weak = opts.weak === undefined ? this.weak : opts.weak
    const Clz = opts.class || Hypercore
    const s = new Clz(null, this.key, {
      ...opts,
      wait,
      onwait,
      timeout,
      writable,
      weak,
      parent: this
    })

    return s
  }

  async setEncryptionKey (key, opts) {
    if (!this.opened) await this.opening
    const encryption = this._getEncryptionProvider({ key, block: !!(opts && opts.block) })
    return this.setEncryption(encryption, opts)
  }

  async setEncryption (encryption, opts) {
    if (!this.opened) await this.opening

    if (encryption === null) {
      this.encryption = encryption
      return
    }

    if (!isEncryptionProvider(encryption)) {
      throw ASSERTION('Provider does not satisfy HypercoreEncryption interface')
    }

    this.encryption = encryption
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

  async _open (storage, key, opts) {
    const preload = opts.preload || (opts.parent && opts.parent.preload)

    if (preload) {
      this.sessions = [] // in case someone looks at it like with peers
      this.preload = preload
      opts = { ...opts, ...(await this.preload) }
      this.preload = null
    }

    const parent = opts.parent || null
    const core = opts.core || (parent && parent.core)
    const sessions = opts.sessions || (parent && parent.sessions)
    const ongc = opts.ongc || (parent && parent.ongc)

    if (core) this.core = core
    if (ongc) this.ongc = ongc
    if (sessions) this.sessions = sessions

    if (this.sessions === null) this.sessions = []
    this._sessionIndex = this.sessions.push(this) - 1

    if (this.core === null) initOnce(this, storage, key, opts)
    if (this._monitorIndex === -2) this.core.addMonitor(this)

    try {
      await this._openSession(opts)
    } catch (err) {
      if (this.core.autoClose && this.core.hasSession() === false) await this.core.close()

      if (this.exclusive) this.core.unlockExclusive()

      this.core.removeMonitor(this)
      this._removeSession()

      if (this.state !== null) this.state.removeSession(this)

      this.closed = true
      this.emit('close')
      throw err
    }

    this.emit('ready')

    // if we are a weak session the core might have closed...
    if (this.core.closing) this.close().catch(safetyCatch)
  }

  _removeSession () {
    if (this._sessionIndex === -1) return
    const head = this.sessions.pop()
    if (head !== this) this.sessions[(head._sessionIndex = this._sessionIndex)] = head
    this._sessionIndex = -1
    if (this.ongc !== null) this.ongc(this)
  }

  async _openSession (opts) {
    if (this.core.opened === false) await this.core.ready()

    if (this.keyPair === null) this.keyPair = opts.keyPair || this.core.header.keyPair

    const parent = opts.parent || null
    if (parent && parent.encryption) this.encryption = parent.encryption

    const e = getEncryptionOption(opts)
    if (!this.encryption) this.encryption = this._getEncryptionProvider(e)

    this.writable = this._isWritable()

    if (opts.valueEncoding) {
      this.valueEncoding = c.from(opts.valueEncoding)
    }
    if (opts.encodeBatch) {
      this.encodeBatch = opts.encodeBatch
    }

    if (parent) {
      if (parent._stateIndex === -1) await parent.ready()
      if (!this.keyPair) this.keyPair = parent.keyPair

      const ps = parent.state

      if (ps) {
        const shouldSnapshot = this.snapshotted && !ps.isSnapshot()
        this.state = shouldSnapshot ? await ps.snapshot() : ps.ref()
      }

      if (this.snapshotted && this.core && !this._snapshot) {
        this._updateSnapshot()
      }
    }

    if (opts.exclusive && opts.writable !== false) {
      this.exclusive = true
      await this.core.lockExclusive()
    }

    const parentState = parent ? parent.state : this.core.state
    const checkout = opts.checkout === undefined ? -1 : opts.checkout
    const state = this.state

    if (opts.atom) {
      this.state = await parentState.createSession(null, false, opts.atom)
      if (state) state.unref()
    } else if (opts.name) {
      // todo: need to make named sessions safe before ready
      // atm we always copy the state in passCapabilities
      this.state = await parentState.createSession(opts.name, !!opts.overwrite, null)
      if (state) state.unref() // ref'ed above in setup session
    }

    if (this.state && checkout !== -1) {
      if (!opts.name && !opts.atom) throw ASSERTION('Checkouts must be named or atomized')
      if (checkout > this.state.length) throw ASSERTION('Invalid checkout ' + checkout + ' for ' + opts.name + ', length is ' + this.state.length)
      if (this.state.prologue && checkout < this.state.prologue.length) {
        throw ASSERTION('Invalid checkout ' + checkout + ' for ' + opts.name + ', prologue length is ' + this.state.prologue.length)
      }
      if (checkout < this.state.length) await this.state.truncate(checkout, this.fork)
    }

    if (this.state === null) {
      this.state = this.core.state.ref()
    }

    this.writable = this._isWritable()

    if (this.snapshotted && this.core) this._updateSnapshot()

    this.state.addSession(this)
    // TODO: we need to rework the core reference flow, as the state and session do not always agree now due to moveTo
    this.core = this.state.core // in case it was wrong...

    if (opts.userData) {
      const tx = this.state.storage.write()
      for (const [key, value] of Object.entries(opts.userData)) {
        tx.putUserData(key, value)
      }
      await tx.flush()
    }

    if (opts.manifest && !this.core.header.manifest) {
      await this.core.setManifest(createManifest(opts.manifest))
    }

    this.core.replicator.updateActivity(this._active ? 1 : 0)

    this.opened = true
  }

  get replicator () {
    return this.core === null ? null : this.core.replicator
  }

  _getSnapshot () {
    return {
      length: this.state.length,
      byteLength: this.state.byteLength,
      fork: this.state.fork
    }
  }

  _updateSnapshot () {
    const prev = this._snapshot
    const next = this._snapshot = this._getSnapshot()

    if (!prev) return true
    return prev.length !== next.length || prev.fork !== next.fork
  }

  _isWritable () {
    if (this._readonly) return false
    if (this.state && !this.state.isDefault()) return true
    return !!(this.keyPair && this.keyPair.secretKey)
  }

  close ({ error } = {}) {
    if (this.closing) return this.closing

    this.closing = this._close(error || null)
    return this.closing
  }

  async _close (error) {
    if (this.opened === false) {
      try {
        await this.opening
      } catch (err) {
        if (!this.closed) throw err
      }
    }

    if (this.closed === true) return

    this.core.removeMonitor(this)
    this.state.removeSession(this)
    this._removeSession()

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

    if (this.core.hasSession()) {
      // emit "fake" close as this is a session
      this.closed = true
      this.emit('close')
      return
    }

    if (this.core.autoClose) await this.core.close()

    this.closed = true
    this.emit('close')
  }

  async commit (session, opts) {
    await this.ready()
    await session.ready()

    return this.state.commit(session.state, { keyPair: this.keyPair, ...opts })
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
      this.opening.then(() => this.core.replicator.attachTo(mux), mux.destroy.bind(mux))
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
    return this.opened === false ? 0 : this.state.length
  }

  get signedLength () {
    return this.opened === false ? 0 : this.state.signedLength()
  }

  /**
   * Deprecated. Use `const { byteLength } = await core.info()`.
   */
  get byteLength () {
    if (this.opened === false) return 0
    if (this._snapshot) return this._snapshot.byteLength
    return this.state.byteLength - (this.state.length * this.padding)
  }

  get contiguousLength () {
    if (this.opened === false) return 0
    return Math.min(this.core.state.length, this.core.header.hints.contiguousLength)
  }

  get contiguousByteLength () {
    return 0
  }

  get fork () {
    if (this.opened === false) return 0
    return this.state.fork
  }

  get padding () {
    if (this.encryption && this.key && this.manifest) {
      return this.encryption.padding(this.core, this.length)
    }

    return 0
  }

  get peers () {
    return this.opened === false ? [] : this.core.replicator.peers
  }

  get globalCache () {
    return this.opened === false ? null : this.core.globalCache
  }

  ready () {
    return this.opening
  }

  async setUserData (key, value) {
    if (this.opened === false) await this.opening
    await this.state.setUserData(key, value)
  }

  async getUserData (key) {
    if (this.opened === false) await this.opening
    const batch = this.state.storage.read()
    const p = batch.getUserData(key)
    batch.tryFlush()
    return p
  }

  transferSession (core) {
    // todo: validate we can move

    if (this.weak === false) {
      this.core.activeSessions--
      core.activeSessions++
    }

    if (this._monitorIndex >= 0) {
      this.core.removeMonitor(this)
      core.addMonitor(this)
    }

    const old = this.core

    this.core = core

    old.replicator.clearRequests(this.activeRequests, SESSION_MOVED())

    this.emit('migrate', this.key)
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
    if (this.snapshotted) return false

    if (this.writable && (!opts || opts.force !== true)) return false

    const remoteWait = this._shouldWait(opts, this.core.replicator.findingPeers > 0)

    let upgraded = false

    if (await this.core.replicator.applyPendingReorg()) {
      upgraded = true
    }

    if (!upgraded && remoteWait) {
      const activeRequests = (opts && opts.activeRequests) || this.activeRequests
      const req = this.core.replicator.addUpgrade(activeRequests)

      try {
        upgraded = await req.promise
      } catch (err) {
        if (isSessionMoved(err)) return this.update(opts)
        throw err
      }
    }

    if (!upgraded) return false
    return true
  }

  async seek (bytes, opts) {
    if (this.opened === false) await this.opening
    if (!isValidIndex(bytes)) throw ASSERTION('seek is invalid')

    const activeRequests = (opts && opts.activeRequests) || this.activeRequests

    if (this.encryption && !this.core.manifest) {
      const req = this.replicator.addUpgrade(activeRequests)
      try {
        await req.promise
      } catch (err) {
        if (isSessionMoved(err)) return this.seek(bytes, opts)
        throw err
      }
    }

    const s = MerkleTree.seek(this.state, bytes, this.padding)

    const offset = await s.update()
    if (offset) return offset

    if (this.closing !== null) throw SESSION_CLOSED()

    if (!this._shouldWait(opts, this.wait)) return null

    const req = this.core.replicator.addSeek(activeRequests, s)

    const timeout = opts && opts.timeout !== undefined ? opts.timeout : this.timeout
    if (timeout) req.context.setTimeout(req, timeout)

    try {
      return await req.promise
    } catch (err) {
      if (isSessionMoved(err)) return this.seek(bytes, opts)
      throw err
    }
  }

  async has (start, end = start + 1) {
    if (this.opened === false) await this.opening
    if (!isValidIndex(start) || !isValidIndex(end)) throw ASSERTION('has range is invalid')

    if (this.state.isDefault()) {
      if (end === start + 1) return this.core.bitfield.get(start)

      const i = this.core.bitfield.firstUnset(start)
      return i === -1 || i >= end
    }

    if (end === start + 1) {
      const rx = this.state.storage.read()
      const block = rx.getBlock(start)
      rx.tryFlush()

      return (await block) !== null
    }

    let count = 0

    const stream = this.state.storage.createBlockStream({ gte: start, lt: end })
    for await (const block of stream) {
      if (block === null) return false
      count++
    }

    return count === (end - start)
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

      await this.encryption.decrypt(index, block, this.core)
    }

    return this._decode(encoding, block, index)
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
    const block = await readBlock(this.state.storage.read(), index)

    if (block !== null) return block

    if (this.closing !== null) throw SESSION_CLOSED()

    // snapshot should check if core has block
    if (this._snapshot !== null) {
      checkSnapshot(this, index)
      const coreBlock = await readBlock(this.core.state.storage.read(), index)

      checkSnapshot(this, index)
      if (coreBlock !== null) return coreBlock
    }

    // lets check the bitfield to see if we got it during the above async calls
    // this is the last resort before replication, so always safe.
    if (this.core.bitfield.get(index)) {
      const coreBlock = await readBlock(this.state.storage.read(), index)
      // TODO: this should not be needed, only needed atm in case we are doing a moveTo during this (we should fix)
      if (coreBlock !== null) return coreBlock
    }

    if (!this._shouldWait(opts, this.wait)) return null

    if (opts && opts.onwait) opts.onwait(index, this)
    if (this.onwait) this.onwait(index, this)

    const activeRequests = (opts && opts.activeRequests) || this.activeRequests

    const req = this.core.replicator.addBlock(activeRequests, index)
    req.snapshot = index < this.length

    const timeout = opts && opts.timeout !== undefined ? opts.timeout : this.timeout
    if (timeout) req.context.setTimeout(req, timeout)

    let replicatedBlock = null

    try {
      replicatedBlock = await req.promise
    } catch (err) {
      if (isSessionMoved(err)) return this._get(index, opts)
      throw err
    }

    if (this._snapshot !== null) checkSnapshot(this, index)
    return maybeUnslab(replicatedBlock)
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
    return new Download(this, range)
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
      fork = this.state.fork + 1,
      keyPair = this.keyPair,
      signature = null
    } = typeof opts === 'number' ? { fork: opts } : opts

    const isDefault = this.state === this.core.state
    const writable = !this._readonly && !!(signature || (keyPair && keyPair.secretKey))
    if (isDefault && writable === false && (newLength > 0 || fork !== this.state.fork)) throw SESSION_NOT_WRITABLE()

    await this.state.truncate(newLength, fork, { keyPair, signature })

    // TODO: Should propagate from an event triggered by the oplog
    if (this.state === this.core.state) this.core.replicator.updateAll()
  }

  async append (blocks, opts = {}) {
    if (this.opened === false) await this.opening

    const isDefault = this.state === this.core.state
    const defaultKeyPair = this.state.name === null ? this.keyPair : null

    const { keyPair = defaultKeyPair, signature = null } = opts
    const writable = !isDefault || !!signature || !!(keyPair && keyPair.secretKey) || opts.writable === true

    if (this._readonly || writable === false) throw SESSION_NOT_WRITABLE()

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

  async signable (length = -1, fork = -1) {
    if (this.opened === false) await this.opening
    if (length === -1) length = this.length
    if (fork === -1) fork = this.fork

    return caps.treeSignable(this.key, await this.treeHash(length), length, fork)
  }

  async treeHash (length = -1) {
    if (this.opened === false) await this.opening
    if (length === -1) length = this.length

    const roots = await MerkleTree.getRoots(this.state, length)
    return crypto.tree(roots)
  }

  async proof (opts) {
    if (this.opened === false) await this.opening
    const rx = this.state.storage.read()
    const promise = MerkleTree.proof(this.state, rx, opts)
    rx.tryFlush()
    return promise
  }

  async verifyFullyRemote (proof) {
    if (this.opened === false) await this.opening
    const batch = await MerkleTree.verifyFullyRemote(this.state, proof)
    await this.core._verifyBatchUpgrade(batch, proof.manifest)
    return batch
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

    if (this.core === null) this._monitorIndex = -2
    else this.core.addMonitor(this)

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

  _decode (enc, block, index) {
    if (this.encryption) block = block.subarray(this.encryption.padding(this.core, index))
    try {
      if (enc) return c.decode(enc, block)
    } catch {
      throw DECODING_ERROR()
    }
    return block
  }

  _getEncryptionProvider (e) {
    if (isEncryptionProvider(e)) return e
    if (!e || !e.key) return null
    return new DefaultEncryption(e.key, this.key, { block: e.block, compat: this.core.compat })
  }
}

module.exports = Hypercore

function isStream (s) {
  return typeof s === 'object' && s && typeof s.pipe === 'function'
}

function toHex (buf) {
  return buf && b4a.toString(buf, 'hex')
}

async function preappend (blocks) {
  const offset = this.state.length
  const fork = this.state.encryptionFork

  for (let i = 0; i < blocks.length; i++) {
    await this.encryption.encrypt(offset + i, blocks[i], fork, this.core)
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
  if (index >= snapshot.state.snapshotCompatLength) throw SNAPSHOT_NOT_AVAILABLE()
}

function readBlock (rx, index) {
  const promise = rx.getBlock(index)
  rx.tryFlush()
  return promise
}

function initOnce (session, storage, key, opts) {
  if (storage === null) storage = opts.storage || null
  if (key === null) key = opts.key || null

  session.core = new Core(Hypercore.defaultStorage(storage), {
    preopen: opts.preopen,
    eagerUpgrade: true,
    notDownloadingLinger: opts.notDownloadingLinger,
    allowFork: opts.allowFork !== false,
    inflightRange: opts.inflightRange,
    compat: opts.compat === true,
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

function maybeAddMonitor (name) {
  if (name === 'append' || name === 'truncate') return
  if (this._monitorIndex >= 0 || this.closing) return

  if (this.core === null) {
    this._monitorIndex = -2
  } else {
    this.core.addMonitor(this)
  }
}

function isSessionMoved (err) {
  return err.code === 'SESSION_MOVED'
}

function getEncryptionOption (opts) {
  // old style, supported for now but will go away
  if (opts.encryptionKey) return { key: opts.encryptionKey, block: !!opts.isBlockKey }
  if (!opts.encryption) return null
  return b4a.isBuffer(opts.encryption) ? { key: opts.encryption } : opts.encryption
}

function isEncryptionProvider (e) {
  return e && isFunction(e.padding) && isFunction(e.encrypt) && isFunction(e.decrypt)
}

function isFunction (fn) {
  return !!fn && typeof fn === 'function'
}
