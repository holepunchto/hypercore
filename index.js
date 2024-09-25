const { EventEmitter } = require('events')
const isOptions = require('is-options')
const hypercoreCrypto = require('hypercore-crypto')
const CoreStorage = require('hypercore-on-the-rocks')
const c = require('compact-encoding')
const b4a = require('b4a')
const NoiseSecretStream = require('@hyperswarm/secret-stream')
const Protomux = require('protomux')
const z32 = require('z32')
const id = require('hypercore-id-encoding')
const safetyCatch = require('safety-catch')
const { createTracer } = require('hypertrace')
const unslab = require('unslab')

const Replicator = require('./lib/replicator')
const Core = require('./lib/core')
const BlockEncryption = require('./lib/block-encryption')
const Info = require('./lib/info')
const Download = require('./lib/download')
const Batch = require('./lib/batch')
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

const promises = Symbol.for('hypercore.promises')
const inspect = Symbol.for('nodejs.util.inspect.custom')

// Hypercore actually does not have any notion of max/min block sizes
// but we enforce 15mb to ensure smooth replication (each block is transmitted atomically)
const MAX_SUGGESTED_BLOCK_SIZE = 15 * 1024 * 1024

module.exports = class Hypercore extends EventEmitter {
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

    this[promises] = true

    this.tracer = createTracer(this)
    this.storage = null
    this.crypto = opts.crypto || hypercoreCrypto
    this.core = null
    this.state = null
    this.replicator = null
    this.encryption = null
    this.extensions = new Map()

    this.valueEncoding = null
    this.encodeBatch = null
    this.activeRequests = []

    this.id = null
    this.key = key || null
    this.keyPair = opts.keyPair || null
    this.readable = true
    this.writable = false
    this.opened = false
    this.closed = false
    this.snapshotted = !!opts.snapshot
    this.draft = !!opts.draft
    this.sparse = opts.sparse !== false
    this.sessions = opts._sessions || [this]
    this.autoClose = !!opts.autoClose
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

    this.opening = this._openSession(key, storage, opts)
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
      indent + '  sparse: ' + opts.stylize(this.sparse, 'boolean') + '\n' +
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
    return hypercoreCrypto.discoveryKey(key)
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

    const sparse = opts.sparse === false ? false : this.sparse
    const wait = opts.wait === false ? false : this.wait
    const writable = opts.writable === false ? false : !this._readonly
    const onwait = opts.onwait === undefined ? this.onwait : opts.onwait
    const timeout = opts.timeout === undefined ? this.timeout : opts.timeout
    const Clz = opts.class || Hypercore
    const s = new Clz(this.storage, this.key, {
      ...opts,
      sparse,
      wait,
      onwait,
      timeout,
      writable,
      _opening: this.opening,
      _sessions: this.sessions
    })

    s._passCapabilities(this)

    if (this.opened) ensureEncryption(s, opts)
    this._addSession(s)

    return s
  }

  _addSession (s) {
    this.sessions.push(s)
  }

  async setEncryptionKey (encryptionKey, opts) {
    if (!this.opened) await this.opening
    this.encryption = encryptionKey ? new BlockEncryption(encryptionKey, this.key, { compat: this.core.compat, ...opts }) : null
  }

  setKeyPair (keyPair) {
    this.keyPair = keyPair
    this.writable = this._isWritable()
  }

  setActive (bool) {
    const active = !!bool
    if (active === this._active || this.closing) return
    this._active = active
    if (!this.opened) return
    this.replicator.updateActivity(this._active ? 1 : -1)
  }

  _passCapabilities (o) {
    if (!this.keyPair) this.keyPair = o.keyPair
    this.crypto = o.crypto
    this.id = o.id
    this.key = o.key
    this.core = o.core
    this.replicator = o.replicator
    this.encryption = o.encryption
    this.writable = this._isWritable()
    this.autoClose = o.autoClose

    if (o.state) this.state = this.draft ? o.state.memoryOverlay() : this.snapshotted ? o.state.snapshot() : o.state.ref()

    if (o.core) this.tracer.setParent(o.core.tracer)

    if (this.snapshotted && this.core && !this._snapshot) this._updateSnapshot()
  }

  async _openFromExisting (from, opts) {
    if (!from.opened) await from.opening

    // includes ourself as well, so the loop below also updates us
    const sessions = this.sessions

    for (const s of sessions) {
      s.sessions = from.sessions
      s._passCapabilities(from)
      s._addSession(s)
    }

    this.storage = from.storage
    this.replicator.findingPeers += this._findingPeers

    ensureEncryption(this, opts)

    // we need to manually fwd the encryption cap as the above removes it potentially
    if (this.encryption && !from.encryption) {
      for (const s of sessions) s.encryption = this.encryption
    }
  }

  async _openSession (key, storage, opts) {
    const isFirst = !opts._opening

    if (!isFirst) {
      await opts._opening
    }
    if (opts.preload) opts = { ...opts, ...(await this._retryPreload(opts.preload)) }

    if (isFirst) {
      await this._openCapabilities(key, storage, opts)

      // check we are the actual root and not a opts.from session
      if (!opts.from) {
        // Only the root session should pass capabilities to other sessions.
        for (let i = 0; i < this.sessions.length; i++) {
          const s = this.sessions[i]
          if (s !== this) s._passCapabilities(this)
        }
      }
    } else {
      ensureEncryption(this, opts)
    }

    if (opts.name) {
      // todo: need to make named sessions safe before ready
      // atm we always copy the state in passCapabilities
      await this.state.unref()
      this.state = await this.core.createSession(opts.name, opts.checkout, opts.refresh)

      if (opts.checkout !== undefined) {
        await this.state.truncate(opts.checkout, this.fork)
      }
    }

    if (opts.manifest && !this.core.header.manifest) {
      await this.core.setManifest(opts.manifest)
    }

    this.writable = this._isWritable()

    if (opts.valueEncoding) {
      this.valueEncoding = c.from(opts.valueEncoding)
    }
    if (opts.encodeBatch) {
      this.encodeBatch = opts.encodeBatch
    }

    // Start continous replication if not in sparse mode.
    if (!this.sparse) this.download({ start: 0, end: -1 })

    // This is a hidden option that's only used by Corestore.
    // It's required so that corestore can load a name from userData before 'ready' is emitted.
    if (opts._preready) await opts._preready(this)

    this.replicator.updateActivity(this._active ? 1 : 0)

    this.opened = true
    this.emit('ready')
  }

  async _retryPreload (preload) {
    while (true) { // TODO: better long term fix is allowing lib/core.js creation from the outside...
      const result = await preload()
      const from = result && result.from
      if (from) {
        if (!from.opened) await from.ready()
        if (from.closing) continue
      }
      return result
    }
  }

  async _openCapabilities (key, storage, opts) {
    if (opts.from) return this._openFromExisting(opts.from, opts)

    const unlocked = !!opts.unlocked
    this.storage = Hypercore.defaultStorage(opts.storage || storage, { unlocked, writable: !unlocked })

    this.core = await Core.open(this.storage, {
      compat: opts.compat,
      force: opts.force,
      sessions: this.sessions,
      createIfMissing: opts.createIfMissing,
      readonly: unlocked,
      discoveryKey: opts.discoveryKey,
      overwrite: opts.overwrite,
      key,
      keyPair: opts.keyPair,
      crypto: this.crypto,
      legacy: opts.legacy,
      manifest: opts.manifest,
      globalCache: opts.globalCache || null, // This is a temp option, not to be relied on unless you know what you are doing (no semver guarantees)
      onupdate: this._oncoreupdate.bind(this),
      onconflict: this._oncoreconflict.bind(this)
    })
    this.tracer.setParent(this.core.tracer)

    this.state = this.core.state

    if (opts.userData) {
      const batch = this.state.storage.createWriteBatch()
      for (const [key, value] of Object.entries(opts.userData)) {
        this.core.setUserData(batch, key, value)
      }
      await batch.flush()
    }

    this.key = this.core.header.key
    this.keyPair = this.core.header.keyPair
    this.id = z32.encode(this.key)

    this.replicator = new Replicator(this.core, this.key, {
      eagerUpgrade: true,
      notDownloadingLinger: opts.notDownloadingLinger,
      allowFork: opts.allowFork !== false,
      inflightRange: opts.inflightRange,
      onpeerupdate: this._onpeerupdate.bind(this),
      onupload: this._onupload.bind(this),
      oninvalid: this._oninvalid.bind(this)
    })

    this.replicator.findingPeers += this._findingPeers

    if (!this.encryption && opts.encryptionKey) {
      this.encryption = new BlockEncryption(opts.encryptionKey, this.key, { compat: this.core.compat, isBlockKey: opts.isBlockKey })
    }
  }

  _getSnapshot () {
    if (this.sparse) {
      return {
        length: this.state.tree.length,
        byteLength: this.state.tree.byteLength,
        fork: this.state.tree.fork,
        compatLength: this.state.tree.length
      }
    }

    return {
      length: this.state.header.hints.contiguousLength,
      byteLength: 0,
      fork: this.state.tree.fork,
      compatLength: this.state.header.hints.contiguousLength
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

  async close ({ error, force = !!error } = {}) {
    if (this.closing) return this.closing

    this.closing = this._close(error || null, force)
    return this.closing
  }

  async _forceClose (err) {
    const sessions = [...this.sessions]

    const closing = []
    for (const session of sessions) {
      if (session === this) continue
      closing.push(sessions.close(err))
    }

    return Promise.all(closing)
  }

  async _close (error, force) {
    if (this.opened === false) await this.opening

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
      this.replicator.clearRequests(this.activeRequests, error)
      this.replicator.updateActivity(this._active ? -1 : 0)
    }

    this._findingPeers = 0

    if (force) {
      await this._forceClose(error)
    } else if (this.sessions.length || this.state.active > 1) {
      // check if there is still an active session
      await this.state.unref()

      // if this is the last session and we are auto closing, trigger that first to enforce error handling
      if (this.sessions.length === 1 && this.core.state.active === 1 && this.autoClose) await this.sessions[0].close({ error })
      // emit "fake" close as this is a session

      this.emit('close', false)
      return
    }

    if (this.replicator !== null) {
      this.replicator.destroy()
    }

    await this.state.unref() // close after replicator
    await this.core.close()

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
    const useSession = !!opts.session

    this._attachToMuxer(protocol, useSession)

    return protocolStream
  }

  _isAttached (stream) {
    return stream.userData && this.replicator && this.replicator.attached(stream.userData)
  }

  _attachToMuxer (mux, useSession) {
    if (this.opened) {
      this._attachToMuxerOpened(mux, useSession)
    } else {
      this.opening.then(this._attachToMuxerOpened.bind(this, mux, useSession), mux.destroy.bind(mux))
    }

    return mux
  }

  _attachToMuxerOpened (mux, useSession) {
    // If the user wants to, we can make this replication run in a session
    // that way the core wont close "under them" during replication
    this.replicator.attachTo(mux, useSession)
  }

  get discoveryKey () {
    return this.replicator === null ? null : this.replicator.discoveryKey
  }

  get manifest () {
    return this.core === null ? null : this.core.header.manifest
  }

  get length () {
    if (this._snapshot) return this._snapshot.length
    if (this.core === null) return 0
    if (!this.sparse) return this.contiguousLength
    return this.state.tree.length
  }

  get flushedLength () {
    return this.state === this.core.state ? this.core.tree.length : this.state.treeLength
  }

  /**
   * Deprecated. Use `const { byteLength } = await core.info()`.
   */
  get byteLength () {
    if (this._snapshot) return this._snapshot.byteLength
    if (this.core === null) return 0
    if (!this.sparse) return this.contiguousByteLength
    return this.state.tree.byteLength - (this.state.tree.length * this.padding)
  }

  get contiguousLength () {
    return this.core === null ? 0 : Math.min(this.core.tree.length, this.core.header.hints.contiguousLength)
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

  get globalCache () {
    return this.core && this.core.globalCache
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

  _oninvalid (err, req, res, from) {
    for (let i = 0; i < this.sessions.length; i++) {
      this.sessions[i].emit('verification-error', err, req, res, from)
    }
  }

  async _oncoreconflict (proof, from) {
    await this.replicator.onconflict(from)

    for (const s of this.sessions) s.emit('conflict', proof.upgrade.length, proof.fork, proof)

    const err = new Error('Two conflicting signatures exist for length ' + proof.upgrade.length)
    await this._closeAllSessions(err)
  }

  async _closeAllSessions (err) {
    // this.sessions modifies itself when a session closes
    // This way we ensure we indeed iterate over all sessions
    const sessions = [...this.sessions]

    const all = []
    for (const s of sessions) all.push(s.close({ error: err, force: false })) // force false or else infinite recursion
    await Promise.allSettled(all)
  }

  _oncoreupdate ({ status, bitfield, value, from }) {
    if (status !== 0) {
      const truncatedNonSparse = (status & 0b1000) !== 0
      const appendedNonSparse = (status & 0b0100) !== 0
      const truncated = (status & 0b0010) !== 0
      const appended = (status & 0b0001) !== 0

      if (truncated) {
        this.replicator.ontruncate(bitfield.start, bitfield.length)
      }

      if ((status & 0b10011) !== 0) {
        this.replicator.onupgrade()
      }

      if (status & 0b10000) {
        for (let i = 0; i < this.sessions.length; i++) {
          const s = this.sessions[i]

          if (s.encryption && s.encryption.compat !== this.core.compat) {
            s.encryption = new BlockEncryption(s.encryption.key, this.key, { compat: this.core.compat, isBlockKey: s.encryption.isBlockKey })
          }
        }

        for (let i = 0; i < this.sessions.length; i++) {
          this.sessions[i].emit('manifest')
        }
      }

      for (let i = 0; i < this.sessions.length; i++) {
        const s = this.sessions[i]

        if (truncated) {
          // If snapshotted, make sure to update our compat so we can fail gets
          if (s._snapshot && bitfield.start < s._snapshot.compatLength) s._snapshot.compatLength = bitfield.start
        }

        if (s.sparse ? truncated : truncatedNonSparse) {
          s.emit('truncate', bitfield.start, this.core.tree.fork)
        }

        // For sparse sessions, immediately emit appends. If non-sparse, emit if contig length has updated
        if (s.sparse ? appended : appendedNonSparse) {
          s.emit('append')
        }
      }

      const contig = this.core.header.hints.contiguousLength

      // When the contig length catches up, broadcast the non-sparse length to peers
      if (appendedNonSparse && contig === this.core.tree.length) {
        for (const peer of this.peers) {
          if (peer.broadcastedNonSparse) continue

          peer.broadcastRange(0, contig)
          peer.broadcastedNonSparse = true
        }
      }
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

    const remoteWait = this._shouldWait(opts, this.replicator.findingPeers > 0)

    let upgraded = false

    if (await this.replicator.applyPendingReorg()) {
      upgraded = true
    }

    if (!upgraded && remoteWait) {
      const activeRequests = (opts && opts.activeRequests) || this.activeRequests
      const req = this.replicator.addUpgrade(activeRequests)

      upgraded = await req.promise
    }

    if (!upgraded) return false
    if (this.snapshotted) return this._updateSnapshot()
    return true
  }

  batch ({ checkout = -1, autoClose = true, session = true, restore = false, clear = false } = {}) {
    return new Batch(session ? this.session() : this, checkout, autoClose, restore, clear)
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
    const req = this.replicator.addSeek(activeRequests, s)

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

    this.tracer.trace('get', { index })

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

    const block = await readBlock(this.state.createReadBatch(), index)

    if (block !== null) return block

    if (this.closing !== null) throw SESSION_CLOSED()

    // snapshot should check if core has block
    if (this._snapshot !== null) {
      checkSnapshot(this._snapshot, index)
      const coreBlock = await readBlock(this.core.state.createReadBatch(), index)

      checkSnapshot(this._snapshot, index)
      if (coreBlock !== null) return coreBlock
    }

    // lets check the bitfield to see if we got it during the above async calls
    // this is the last resort before replication, so always safe.
    if (this.core.state.bitfield.get(index)) {
      return readBlock(this.state.createReadBatch(), index)
    }

    if (!this._shouldWait(opts, this.wait)) return null

    if (opts && opts.onwait) opts.onwait(index, this)
    if (this.onwait) this.onwait(index, this)

    const activeRequests = (opts && opts.activeRequests) || this.activeRequests

    const req = this.replicator.addBlock(activeRequests, index)
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

    this.tracer.trace('download', { range })

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
    if (this.state === this.core.state) this.replicator.updateAll()
  }

  async append (blocks, opts = {}) {
    if (this.opened === false) await this.opening

    const isDefault = this.state === this.core.state

    const { keyPair = this.keyPair, signature = null } = opts
    const writable = !this._readonly && !!(signature || (keyPair && keyPair.secretKey))

    if (isDefault && writable === false) throw SESSION_NOT_WRITABLE()

    blocks = Array.isArray(blocks) ? blocks : [blocks]
    this.tracer.trace('append', { blocks })

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
    return this.crypto.tree(roots)
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

function ensureEncryption (core, opts) {
  if (!opts.encryptionKey) return
  // Only override the block encryption if it's either not already set or if
  // the caller provided a different key.
  if (core.encryption && b4a.equals(core.encryption.key, opts.encryptionKey) && core.encryption.compat === core.core.compat) return
  core.encryption = new BlockEncryption(opts.encryptionKey, core.key, { compat: core.core ? core.core.compat : true, isBlockKey: opts.isBlockKey })
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
