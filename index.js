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
const flat = require('flat-tree')
const assert = require('nanoassert')

const { SMALL_WANTS } = require('./lib/feature-flags')
const { UPDATE_COMPAT } = require('./lib/wants')
const MarkBitfield = require('./lib/mark-bitfield')

const inspect = require('./lib/inspect')
const Core = require('./lib/core')
const Info = require('./lib/info')
const Download = require('./lib/download')
const DefaultEncryption = require('./lib/default-encryption')
const caps = require('./lib/caps')
const Replicator = require('./lib/replicator')
const { manifestHash, createManifest } = require('./lib/verifier')
const { ReadStream, WriteStream, ByteStream } = require('./lib/streams')
const { MerkleTree } = require('./lib/merkle-tree')
const { proof, verify } = require('./lib/fully-remote-proof')
const {
  ASSERTION,
  BAD_ARGUMENT,
  SESSION_CLOSED,
  SESSION_MOVED,
  SESSION_NOT_WRITABLE,
  SNAPSHOT_NOT_AVAILABLE,
  DECODING_ERROR,
  REQUEST_CANCELLED
} = require('hypercore-errors')

// Hypercore actually does not have any notion of max/min block sizes
// but we enforce 15mb to ensure smooth replication (each block is transmitted atomically)
const MAX_SUGGESTED_BLOCK_SIZE = 15 * 1024 * 1024

/**
 * Options for creating or opening a Hypercore instance.
 * @typedef {Object} HypercoreOptions
 * @property {Buffer} [key] - The public key of the core (32 bytes). Omit to create a new writable core.
 * @property {{publicKey: Buffer, secretKey: Buffer}} [keyPair] - Ed25519 key pair for signing appended blocks.
 * @property {Buffer} [encryptionKey] - A 32-byte key to enable block encryption.
 * @property {object} [encryption] - Custom encryption provider satisfying the HypercoreEncryption interface.
 * @property {string|object} [valueEncoding] - Encoding for block values (e.g. `'utf8'`, `'json'`, a compact-encoding codec).
 * @property {boolean} [writable=true] - Set `false` to open the session read-only.
 * @property {boolean} [sparse=true] - Download blocks on demand instead of eagerly.
 * @property {boolean} [weak=false] - Do not keep the underlying core alive when this session is the last one.
 * @property {boolean} [snapshot=false] - Snapshot the core length at open time; blocks beyond that length are invisible.
 * @property {number} [timeout=0] - Default timeout in ms for `get()` / `seek()` (0 = no timeout).
 * @property {boolean} [wait=true] - Wait for blocks to download when `get()` / `seek()` are called.
 * @property {function} [onwait] - Called whenever `get()` triggers a network wait: `onwait(index, core)`.
 * @property {function} [onseq] - Called on each `get()` call with the block index: `onseq(index, core)`.
 * @property {boolean} [compat=false] - Enable legacy (v9) manifest compatibility mode.
 * @property {boolean} [exclusive=false] - Acquire an exclusive write lock on the core.
 */

/**
 * Options for Hypercore.key() static method.
 * @typedef {Object} KeyOptions
 * @property {boolean} [compat=false] - If `true`, returns the first signer's raw public key instead of the manifest hash.
 * @property {number} [version] - Manifest version number (used when building the manifest from a raw key).
 * @property {Buffer} [namespace] - Namespace buffer to include in the manifest when building from a raw key.
 */

/**
 * Options for Hypercore.createProtocolStream().
 * @typedef {Object} ProtocolStreamOptions
 * @property {object} [stream] - An existing raw stream to wrap; avoids creating a new NoiseSecretStream.
 * @property {function} [ondiscoverykey] - Called with a discovery key Buffer when a remote announces a new core.
 * @property {boolean} [keepAlive=true] - Send keep-alive pings every 5 s to prevent idle disconnection.
 */

/**
 * Options for Hypercore.defaultStorage().
 * @typedef {Object} DefaultStorageOptions
 * @property {boolean} [sparse=true] - Use sparse file storage (holes instead of zero-filled regions).
 */

/**
 * Options for core.session() / core.snapshot().
 * @typedef {Object} SessionOptions
 * @property {string|object} [valueEncoding] - Override the value encoding for this session only.
 * @property {boolean} [writable] - Override the writable flag for this session.
 * @property {boolean} [snapshot=false] - Lock the visible length to the current core length.
 * @property {boolean} [sparse=true] - Download blocks on demand.
 * @property {number} [timeout] - Per-session get/seek timeout in ms.
 * @property {boolean} [wait] - Whether to wait for remote blocks by default.
 * @property {boolean} [weak] - Do not keep the core alive when this session is the last one.
 * @property {boolean} [exclusive=false] - Acquire an exclusive write lock on the core.
 * @property {object} [atom] - A storage atom to stage writes against.
 * @property {string} [name] - Named session; writes are staged under this name.
 * @property {number} [checkout] - Roll back a named/atom session to this length after opening.
 */

/**
 * Options for core.setEncryptionKey().
 * @typedef {Object} SetEncryptionKeyOptions
 * @property {boolean} [block=false] - Treat the supplied key as a raw block-level key rather than deriving one.
 */

/**
 * Options for core.get().
 * @typedef {Object} GetOptions
 * @property {boolean} [wait=true] - Wait for the block to be downloaded from a peer if not available locally.
 * @property {number} [timeout=0] - Max ms to wait for replication (0 = use core default).
 * @property {string|object} [valueEncoding] - Decode the block with this encoding instead of the core's default.
 * @property {boolean} [decrypt=true] - Decrypt the block when encryption is enabled.
 * @property {boolean} [raw=false] - Return the raw Buffer without decoding or decrypting.
 * @property {function} [onwait] - Called if this specific get triggers a network wait: `onwait(index, core)`.
 */

/**
 * Options for core.seek().
 * @typedef {Object} SeekOptions
 * @property {boolean} [wait=true] - Wait for the necessary block(s) to download if not local.
 * @property {number} [timeout=0] - Max ms to wait (0 = use core default).
 */

/**
 * Options for core.clear().
 * @typedef {Object} ClearOptions
 * @property {boolean} [diff=false] - Return a `{ blocks: number }` object counting cleared blocks instead of `null`.
 */

/**
 * Options for core.update().
 * @typedef {Object} UpdateOptions
 * @property {boolean} [wait=true] - Wait for remote peers to send a new signed length proof.
 * @property {boolean} [force=false] - Force an update even when the core is writable.
 */

/**
 * Options for core.truncate().
 * @typedef {Object} TruncateOptions
 * @property {number} [fork] - The fork ID to assign after truncation (defaults to `state.fork + 1`).
 * @property {{publicKey: Buffer, secretKey: Buffer}} [keyPair] - Key pair to sign the truncation (defaults to `core.keyPair`).
 * @property {Buffer} [signature] - Pre-computed signature for the truncation.
 */

/**
 * Options for core.append().
 * @typedef {Object} AppendOptions
 * @property {{publicKey: Buffer, secretKey: Buffer}} [keyPair] - Key pair to sign the batch (defaults to `core.keyPair`).
 * @property {Buffer} [signature] - Pre-computed signature.
 * @property {number} [maxLength] - Refuse to append if the resulting length would exceed this value.
 */

/**
 * Options for core.info().
 * @typedef {Object} InfoOptions
 * @property {boolean} [storage=false] - Include per-file storage byte counts in the result.
 */

/**
 * Options for core.createReadStream().
 * @typedef {Object} ReadStreamOptions
 * @property {number} [start=0] - Index of the first block to read.
 * @property {number} [end] - Index of the block to stop at (exclusive).
 * @property {boolean} [live=false] - Keep streaming new blocks as they are appended.
 * @property {boolean} [snapshot=true] - Snap the end to the current length at open time (ignored when `live` is true).
 * @property {boolean} [wait=true] - Wait for blocks to download.
 * @property {number} [timeout=0] - Per-block timeout in ms (0 = use core default).
 */

/**
 * Options for core.createByteStream().
 * @typedef {Object} ByteStreamOptions
 * @property {number} [byteOffset=0] - Start reading from this byte position.
 * @property {number} [byteLength] - Number of bytes to read (-1 = until end of core).
 * @property {number} [prefetch=32] - Number of blocks to prefetch ahead.
 */

/**
 * Download range descriptor for core.download().
 * @typedef {Object} DownloadRange
 * @property {number} [start=0] - First block index to download.
 * @property {number} [end] - Last block index to download (exclusive; defaults to `core.length`).
 * @property {boolean} [linear=false] - Download blocks in sequential order.
 */

/**
 * Options for core.proof().
 * @typedef {Object} ProofOptions
 * @property {object} [block] - Request a block proof: `{ index: number }`.
 * @property {object} [hash] - Request a hash proof: `{ index: number }`.
 * @property {object} [seek] - Request a seek proof: `{ bytes: number }`.
 * @property {object} [upgrade] - Request an upgrade proof: `{ start: number, length: number }`.
 * @property {object} [manifest] - Include a manifest proof.
 */

/**
 * Options for core.commit().
 * @typedef {Object} CommitOptions
 * @property {{publicKey: Buffer, secretKey: Buffer}} [keyPair] - Key pair used to sign the committed blocks.
 */

/**
 * Options for core.sweep().
 * @typedef {Object} SweepOptions
 * @property {number} [batchSize=1000] - Number of clear operations to run in parallel per sweep iteration.
 */

/**
 * Options for core.close().
 * @typedef {Object} CloseOptions
 * @property {Error} [error] - Error to reject pending replication requests with.
 */

class Hypercore extends EventEmitter {
  /**
   * Make a new Hypercore instance.
   * @param {string|object} storage - should be set to a directory where you want to store the data and core metadata.
   * @param {Buffer|string} [key] - can be set to a Hypercore key which is a hash of Hypercore's internal auth manifest, describing how to validate the Hypercore.
   * @param {HypercoreOptions} [opts]
   * @example
   * const core = new Hypercore('./directory') // store data in ./directory
   */
  constructor(storage, key, opts) {
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

    /**
     * Object containing buffers of the core's public and secret key
     * @type {{publicKey: Buffer, secretKey: Buffer|null}|null}
     */
    this.keyPair = opts.keyPair || null
    /**
     * Can we read from this core? After closing the core this will be false.
     * @type {boolean}
     */
    this.readable = true
    /**
     * Can we append to or truncate this core?
     * @type {boolean}
     */
    this.writable = false
    this.exclusive = false
    this.opened = false
    this.closed = false
    this.weak = !!opts.weak
    this.snapshotted = !!opts.snapshot
    this.onseq = opts.onseq || null
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

    this.waits = 0

    // Mark & Sweep GC
    this._marking = false
    this._marks = null

    this._sessionIndex = -1
    this._stateIndex = -1 // maintained by session state
    this._monitorIndex = -1 // maintained by replication state

    this.opening = this._open(storage, key, opts)
    this.opening.catch(safetyCatch)

    this.on('newListener', maybeAddMonitor)
  }

  [Symbol.for('nodejs.util.inspect.custom')](depth, opts) {
    return inspect(this, depth, opts)
  }

  /**
   * The constant for max size (15MB) for blocks appended to Hypercore. This max
   * ensures blocks are replicated smoothly.
   * @type {number}
   */
  static MAX_SUGGESTED_BLOCK_SIZE = MAX_SUGGESTED_BLOCK_SIZE

  static DefaultEncryption = DefaultEncryption

  static SMALL_WANTS = SMALL_WANTS

  static enable(flag) {
    const enableCompat = (flag & SMALL_WANTS) === 0
    UPDATE_COMPAT(enableCompat)
  }

  static setRecoveryPeers(peers) {
    Core.setRecoveryPeers(peers)
  }

  /**
   * `options` include:
   * @param {Buffer|object} manifest - A 32-byte public key Buffer, or a full manifest object with a `signers` array.
   * @param {KeyOptions} [options]
   * @returns {Buffer} the key for a given manifest.
   * @example
   * {
   *   compat: false,  // Whether the manifest has a single signer whose public key is the key
   *   version,        // Manifest version if the manifest argument is the public key of a single signer
   *   namespace       // The signer namespace if the manifest argument is the public key of a single signer
   * }
   */
  static key(manifest, { compat, version, namespace } = {}) {
    if (b4a.isBuffer(manifest)) {
      manifest = { version, signers: [{ publicKey: manifest, namespace }] }
    }
    return compat ? manifest.signers[0].publicKey : manifestHash(createManifest(manifest))
  }

  /**
   * Derive the discovery key from a Hypercore public key. The discovery key
   * can safely be shared to announce the core without exposing the read key.
   * @param {Buffer} key - The 32-byte Hypercore public key.
   * @returns {Buffer} the discovery key for the provided `key`.
   * @example
   * const dKey = Hypercore.discoveryKey(core.key)
   */
  static discoveryKey(key) {
    return crypto.discoveryKey(key)
  }

  /**
   * Derive a block-level encryption key from the Hypercore public key and a
   * master encryption key using BLAKE2b.
   * @param {Buffer} key - The 32-byte Hypercore public key.
   * @param {Buffer} encryptionKey - The 32-byte master encryption key.
   * @returns {Buffer} a block encryption key derived from the `key` and `encryptionKey`.
   * @example
   * const blockKey = Hypercore.blockEncryptionKey(core.key, encryptionKey)
   */
  static blockEncryptionKey(key, encryptionKey) {
    return DefaultEncryption.blockEncryptionKey(key, encryptionKey)
  }

  /**
   * Extract the Protomux instance attached to a Hypercore protocol stream.
   * @param {object} stream - A Hypercore protocol stream (as returned by `core.replicate()` or `Hypercore.createProtocolStream()`).
   * @returns {object} a protomux instance from the provided `stream` Hypercore protocol stream.
   * @example
   * const stream = core.replicate(true)
   * const mux = Hypercore.getProtocolMuxer(stream)
   */
  static getProtocolMuxer(stream) {
    return stream.noiseStream.userData
  }

  /**
   * Create the raw internal Core object directly, bypassing the Hypercore
   * session layer. Useful for low-level tooling that needs direct storage
   * access.
   * @param {string|object} storage - Path to a storage directory, or a CoreStorage instance.
   * @param {HypercoreOptions} opts - Options forwarded to the Core constructor.
   * @returns {object} the internal core using the `storage` and `opts` without creating a full Hypercore instance.
   * @example
   * const core = Hypercore.createCore('./storage', { key: myKey })
   */
  static createCore(storage, opts) {
    return new Core(Hypercore.defaultStorage(storage), { autoClose: false, ...opts })
  }

  /**
   * Create an encrypted noise stream with a protomux instance attached used for
   * Hypercore's replication protocol.
   * @param {boolean|object} isInitiator - can be a framed stream, a protomux or a boolean for whether the stream should be the initiator in the noise handshake.
   * @param {ProtocolStreamOptions} [opts]
   * @returns {object} The outer raw stream with a `.noiseStream.userData` Protomux attached.
   * @example
   * {
   *   ondiscoverykey: () => {}, // A handler for when a discovery key is set over the stream for corestore management
   * }
   */
  static createProtocolStream(isInitiator, opts = {}) {
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
    if (!noiseStream) throw BAD_ARGUMENT('Invalid stream', this.discoveryKey)

    if (!noiseStream.userData) {
      const protocol = Protomux.from(noiseStream)

      if (opts.keepAlive !== false && noiseStream.keepAlive === 0) {
        noiseStream.setKeepAlive(5000)
      }
      noiseStream.userData = protocol
    }

    if (opts.ondiscoverykey) {
      const ondiscoverykey = createDiscoveryKeyHandler(opts.ondiscoverykey)
      noiseStream.userData.pair({ protocol: 'hypercore/alpha' }, ondiscoverykey)
    }

    return outerStream
  }

  /**
   * Wrap a path or existing CoreStorage object into a CoreStorage instance.
   * Called internally by the constructor; exposed for advanced use.
   * @param {string|object} storage - Directory path string or an existing CoreStorage instance (returned as-is).
   * @param {DefaultStorageOptions} [opts] - Extra options forwarded to the CoreStorage constructor.
   * @returns {object} a default hypercore storage.
   * @example
   * const storage = Hypercore.defaultStorage('./my-core')
   */
  static defaultStorage(storage, opts = {}) {
    if (CoreStorage.isCoreStorage(storage)) return storage

    const directory = storage
    return new CoreStorage(directory, opts)
  }

  static clearRequests(session, err) {
    Replicator.clearRequests(session, err)
  }

  static destroyRequests(session, err) {
    Replicator.clearRequests(session, err)
    session.push(null) // mark as dead
  }

  static async treeHashFromStorage(session, length = session.length) {
    const roots = await MerkleTree.getRoots(session.state, length)
    return crypto.tree(roots)
  }

  /**
   * Same as [`core.session(options)`](#const-session--coresessionoptions), but
   * backed by a storage snapshot so will not truncate nor append.
   * @param {SessionOptions} [opts] - Options forwarded to `core.session()` with `snapshot: true` set automatically.
   * @returns {Hypercore} A new read-only Hypercore session locked to the current length.
   * @example
   * const snap = core.snapshot()
   * console.log(snap.length) // frozen at the moment snapshot() was called
   */
  snapshot(opts) {
    return this.session({ ...opts, snapshot: true })
  }

  compact() {
    return this.core.compact()
  }

  /**
   * Creates a new Hypercore instance that shares the same underlying core.
   * @param {SessionOptions} [opts] - Options are inherited from the parent instance, unless they are re-set.
   * @returns {Hypercore} A new Hypercore session sharing the same underlying storage.
   * @throws {SESSION_CLOSED} if called on a core that is already closing.
   * @example
   * {
   *   weak: false // Creates the session as a "weak ref" which closes when all non-weak sessions are closed
   *   exclusive: false, // Create a session with exclusive access to the core. Creating an exclusive session on a core with other exclusive sessions, will wait for the session with access to close before the next exclusive session is `ready`
   *   checkout: undefined, // A index to checkout the core at. Checkout sessions must be an atom or a named session
   *   atom: undefined, // A storage atom for making atomic batch changes across hypercores
   *   name: null, // Name the session creating a persisted branch of the core. Still beta so may break in the future
   * }
   */
  session(opts = {}) {
    if (this.closing) {
      // This makes the closing logic a lot easier. If this turns out to be a problem
      // in practice, open an issue and we'll try to make a solution for it.
      throw SESSION_CLOSED('Cannot make sessions on a closing core', this.discoveryKey)
    }
    if (opts.checkout !== undefined && !opts.name && !opts.atom) {
      throw ASSERTION('Checkouts are only supported on atoms or named sessions', this.discoveryKey)
    }

    const wait = opts.wait === false ? false : this.wait
    const writable = opts.writable === undefined ? !this._readonly : opts.writable === true
    const onwait = opts.onwait === undefined ? this.onwait : opts.onwait
    const onseq = opts.onseq === undefined ? this.onseq : opts.onseq
    const timeout = opts.timeout === undefined ? this.timeout : opts.timeout
    const weak = opts.weak === undefined ? this.weak : opts.weak
    const marking = this._marking
    const marks = this._marks
    const Clz = opts.class || Hypercore
    const s = new Clz(null, this.key, {
      ...opts,
      wait,
      onwait,
      onseq,
      timeout,
      writable,
      weak,
      parent: this
    })
    s._marking = marking
    s._marks = marks

    return s
  }

  /**
   * Set the encryption key.
   * @param {Buffer} key - The 32-byte encryption key to use for block encryption/decryption.
   * @param {SetEncryptionKeyOptions} [opts]
   * @returns {Promise<void>} Resolves once the encryption provider has been installed.
   * @example
   * {
   *   block: false, // Whether the key is for block encryption
   * }
   */
  async setEncryptionKey(key, opts) {
    if (!this.opened) await this.opening
    const encryption = this._getEncryptionProvider({ key, block: !!(opts && opts.block) })
    return this.setEncryption(encryption)
  }

  /**
   * Set the encryption, which should satisfy the
   * [HypercoreEncryption](https://github.com/holepunchto/hypercore-encryption)
   * interface.
   * @param {object|null} encryption - An encryption provider with `padding`, `encrypt`, and `decrypt` methods, or `null` to disable encryption.
   * @returns {Promise<void>} Resolves once the encryption provider has been installed.
   * @throws {ASSERTION} if the provider does not satisfy the `HypercoreEncryption` interface.
   * @example
   * await core.setEncryption(new DefaultEncryption(encryptionKey, core.key))
   */
  async setEncryption(encryption) {
    if (!this.opened) await this.opening

    if (encryption === null) {
      this.encryption = encryption
      return
    }

    if (!isEncryptionProvider(encryption)) {
      throw ASSERTION('Provider does not satisfy HypercoreEncryption interface', this.discoveryKey)
    }

    this.encryption = encryption
  }

  /**
   * Set the group `topic` that the hypercore belongs to. Useful for grouping
   * hypercores together that need to update a larger data structure (eg.
   * `autobee`) that is comprised of them. See `corestore`'s
   * `store.notifyGroup(topic)` for more details.
   * @param {Buffer} topic - is a 32 byte buffer.
   */
  async setGroup(topic) {
    if (!this.opened) await this.opening
    return this.core.setGroup(topic)
  }

  /**
   * Update the core's `keyPair`. Advanced as the `keyPair` is used throughout
   * Hypercore, e.g. verifying blocks, identifying the core, etc.
   * @param {{publicKey: Buffer, secretKey: Buffer}} keyPair - The new Ed25519 key pair to use for signing.
   * @returns {void}
   * @example
   * core.setKeyPair({ publicKey: pubKey, secretKey: secKey })
   */
  setKeyPair(keyPair) {
    this.keyPair = keyPair
  }

  /**
   * Set the core to be active or not. A core is considered 'active' if it should
   * linger to download blocks from peers.
   * @param {boolean} bool - Pass `true` to mark the core as active, `false` to deactivate.
   * @returns {void}
   * @example
   * core.setActive(false) // stop lingering for peer downloads
   */
  setActive(bool) {
    const active = !!bool
    if (active === this._active || this.closing) return
    this._active = active
    if (!this.opened) return
    this.core.replicator.updateActivity(this._active ? 1 : -1)
  }

  async _open(storage, key, opts) {
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

    // Setup automatic recovery if in repair mode
    if (this.core._repairMode) {
      const recoverTreeNodeFromPeersBound = this.recoverTreeNodeFromPeers.bind(this)
      this.once('repaired', () => {
        this.off('peer-add', recoverTreeNodeFromPeersBound)
      })
      this.on('peer-add', recoverTreeNodeFromPeersBound)
    }

    this.emit('ready')

    // if we are a weak session the core might have closed...
    if (this.core.closing) this.close().catch(safetyCatch)
  }

  _removeSession() {
    if (this._sessionIndex === -1) return
    const head = this.sessions.pop()
    if (head !== this) this.sessions[(head._sessionIndex = this._sessionIndex)] = head
    this._sessionIndex = -1
    if (this.ongc !== null) this.ongc(this)
  }

  async _openSession(opts) {
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

    // one session sets for pushOnly for all
    if (opts.pushOnly === true) {
      this.core.replicator.setPushOnly(true)
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
      if (!opts.name && !opts.atom) {
        throw ASSERTION('Checkouts must be named or atomized', this.discoveryKey)
      }
      if (checkout > this.state.length) {
        throw ASSERTION(
          `Invalid checkout ${checkout} for ${opts.name}, length is ${this.state.length}`,
          this.discoveryKey
        )
      }
      if (this.state.prologue && checkout < this.state.prologue.length) {
        throw ASSERTION(
          `Invalid checkout ${checkout} for ${opts.name}, prologue length is ${this.state.prologue.length}`,
          this.discoveryKey
        )
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

  get replicator() {
    return this.core === null ? null : this.core.replicator
  }

  _getSnapshot() {
    return {
      length: this.state.length,
      byteLength: this.state.byteLength,
      fork: this.state.fork
    }
  }

  _updateSnapshot() {
    const prev = this._snapshot
    const next = (this._snapshot = this._getSnapshot())

    if (!prev) return true
    return prev.length !== next.length || prev.fork !== next.fork
  }

  _isWritable() {
    if (this._readonly) return false
    if (this.state && !this.state.isDefault()) return true
    return !!(this.keyPair && this.keyPair.secretKey)
  }

  /**
   * Fully close this core. Passing an error via `{ error }` is optional and all
   * pending replicator requests will be rejected with the error.
   * @param {CloseOptions} [options] - Optional close options.
   * @returns {Promise<void>} Resolves once the session (and underlying core if no other sessions remain) is fully closed.
   * @example
   * await core.close()
   */
  close({ error } = {}) {
    if (this.closing) return this.closing

    this.closing = this._close(error || null)
    return this.closing
  }

  clearRequests(activeRequests, error) {
    if (!activeRequests.length) return
    if (this.core) this.core.replicator.clearRequests(activeRequests, error)
  }

  async _close(error) {
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

  /**
   * Attempt to apply blocks from the session to the `core`. `core` must be a
   * default core, aka a non-named session.
   * @param {Hypercore} session - The named or atom session whose staged blocks should be committed.
   * @param {CommitOptions} [opts]
   * @returns {Promise<{length: number, byteLength: number}|null>} `null` if committing failed.
   * @throws {INVALID_OPERATION} if no database batch was passed, or the tree changed during the batch.
   * @example
   * {
   *   length: session.length, // the core's length after committing the blocks
   *   treeLength: core.length, // The expected length of the core's merkle tree prior to commit
   *   keyPair: core.keyPair, // The keypair to use when committing
   *   signature: undefined, // The signature for the blocks being committed
   * }
   */
  async commit(session, opts) {
    await this.ready()
    await session.ready()

    return this.state.commit(session.state, { keyPair: this.keyPair, ...opts })
  }

  /**
   * Create a replication stream. You should pipe this to another Hypercore
   * instance.
   * @param {boolean|object} isInitiator - `true`/`false` for the noise handshake role, or an existing stream / Protomux to attach to.
   * @param {ProtocolStreamOptions} [opts] - are same as [`Hypercore.createProtocolStream()`](#const-stream--hypercorecreateprotocolstreamisinitiator-opts--).
   * @returns {object} The replication stream (a raw duplex stream with a Protomux attached).
   * @example
   * // assuming we have two cores, localCore + remoteCore, sharing the same key
   * // on a server
   * const net = require('net')
   * const server = net.createServer(function (socket) {
   *   socket.pipe(remoteCore.replicate(false)).pipe(socket)
   * })
   *
   * // on a client
   * const socket = net.connect(...)
   * socket.pipe(localCore.replicate(true)).pipe(socket)
   */
  replicate(isInitiator, opts = {}) {
    // Only limitation here is that ondiscoverykey doesn't work atm when passing a muxer directly,
    // because it doesn't really make a lot of sense.
    if (Protomux.isProtomux(isInitiator)) return this._attachToMuxer(isInitiator)

    // if same stream is passed twice, ignore the 2nd one before we make sessions etc
    if (isStream(isInitiator) && this._isAttached(isInitiator)) return isInitiator

    const protocolStream = Hypercore.createProtocolStream(isInitiator, opts)
    const noiseStream = protocolStream.noiseStream
    const protocol = noiseStream.userData

    this._attachToMuxer(protocol)

    return protocolStream
  }

  _isAttached(stream) {
    return (
      stream.userData &&
      this.core &&
      this.core.replicator &&
      this.core.replicator.attached(stream.userData)
    )
  }

  _attachToMuxer(mux) {
    if (this.opened) {
      this.core.replicator.attachTo(mux)
    } else {
      this.opening.then(() => this.core.replicator.attachTo(mux), mux.destroy.bind(mux))
    }

    return mux
  }

  /**
   * String containing the id (z-base-32 of the public key) identifying this
   * core.
   * @returns {string|null}
   */
  get id() {
    return this.core === null ? null : this.core.id
  }

  /**
   * Buffer containing the public key identifying this core.
   * @returns {Buffer|null}
   */
  get key() {
    return this.core === null ? null : this.core.key
  }

  /**
   * Buffer containing a key derived from the core's public key. In contrast to
   * `core.key` this key does not allow you to verify the data but can be used to
   * announce or look for peers that are sharing the same core, without leaking
   * the core key.
   * @returns {Buffer|null}
   */
  get discoveryKey() {
    return this.core === null ? null : this.core.discoveryKey
  }

  get manifest() {
    return this.core === null ? null : this.core.manifest
  }

  /**
   * How many blocks of data are available on this core.
   * @returns {number}
   */
  get length() {
    if (this._snapshot) return this._snapshot.length
    return this.opened === false ? 0 : this.state.length
  }

  /**
   * How many blocks of data are available on this core that have been signed by
   * a quorum. This is equal to `core.length` for Hypercores with a single
   * signer.
   * @returns {number}
   */
  get signedLength() {
    return this.opened === false ? 0 : this.state.signedLength()
  }

  /**
   * Deprecated. Use `const { byteLength } = await core.info()`.
   */
  get byteLength() {
    if (this.opened === false) return 0
    if (this._snapshot) return this._snapshot.byteLength
    return this.state.byteLength - this.state.length * this.padding
  }

  /**
   * How many blocks are contiguously available starting from the first block of
   * this core on any known remote. This is only updated when a remote thinks it
   * is fully contiguous such that they have all known blocks.
   * @returns {number}
   */
  get remoteContiguousLength() {
    if (this.opened === false) return 0
    return Math.min(this.core.state.length, this.core.header.hints.remoteContiguousLength)
  }

  /**
   * How many blocks are contiguously available starting from the first block of
   * this core.
   * @returns {number}
   */
  get contiguousLength() {
    if (this.opened === false) return 0
    return Math.min(this.core.state.length, this.core.header.hints.contiguousLength)
  }

  get contiguousByteLength() {
    return 0
  }

  /**
   * What is the current fork id of this core?
   * @returns {number}
   */
  get fork() {
    if (this.opened === false) return 0
    return this.state.fork
  }

  /**
   * How much padding is applied to each block of this core? Will be `0` unless
   * block encryption is enabled.
   * @returns {number}
   */
  get padding() {
    if (this.encryption && this.key && this.manifest) {
      return this.encryption.padding(this.core, this.length)
    }

    return 0
  }

  /**
   * Array of current peers the core is replicating with.
   * @returns {Array<object>}
   */
  get peers() {
    return this.opened === false ? [] : this.core.replicator.peers
  }

  get globalCache() {
    return this.opened === false ? null : this.core.globalCache
  }

  get recovering() {
    return this.opened === false ? 0 : this.core.header.hints.recovering
  }

  /**
   * Wait for the core to fully open.
   * @returns {Promise<void>} Resolves once the core is ready for reading and writing.
   * @example
   * const core = new Hypercore('./storage')
   * await core.ready()
   * console.log(core.key) // now available
   */
  ready() {
    return this.opening
  }

  async recover() {
    if (this.opened === false) await this.opening
    return this.state.waitForRecovery()
  }

  /**
   * Set a key in the User Data key-value store.
   * @param {string} key - The user-data key (a string).
   * @param {Buffer|string} value - The value to store (Buffer or UTF-8 string).
   * @returns {Promise<void>} Resolves once the value has been persisted to storage.
   * @example
   * await core.setUserData('version', Buffer.from('1.0'))
   */
  async setUserData(key, value) {
    if (this.opened === false) await this.opening
    const existing = await this.getUserData(key)
    if (existing && b4a.isBuffer(value) && b4a.equals(existing, value)) return
    await this.state.setUserData(key, value)
  }

  /**
   * Reads the local user-data value stored under `key`, resolving with its
   * `Buffer`/string value or `null` if unset. User data is local-only and not
   * replicated.
   * @param {string} key
   * @returns {Promise<Buffer|null>} the value for a key in the User Data key-value store.
   * @example
   * const value = await core.getUserData('version')
   * console.log(value && value.toString()) // '1.0'
   */
  async getUserData(key) {
    if (this.opened === false) await this.opening
    const batch = this.state.storage.read()
    const p = batch.getUserData(key)
    batch.tryFlush()
    return p
  }

  transferSession(core) {
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

  /**
   * Create a hook that tells Hypercore you are finding peers for this core in
   * the background. Call `done` when your current discovery iteration is done.
   * If you're using Hyperswarm, you'd normally call this after a `swarm.flush()`
   * finishes.
   * @returns {function} A `done()` callback to call when peer discovery is complete.
   * @example
   * const done = core.findingPeers()
   * swarm.flush().then(done, done)
   */
  findingPeers() {
    this._findingPeers++
    if (this.core !== null && !this.closing) this.core.replicator.findingPeers++

    let once = true

    return () => {
      if (this.closing || !once) return
      once = false
      this._findingPeers--
      if (this.core !== null && --this.core.replicator.findingPeers === 0) {
        this.core.replicator.queueUpdateAll()
      }
    }
  }

  /**
   * Get information about this core, such as its total size in bytes.
   * @param {InfoOptions} [opts]
   * @returns {Promise<object>} An `Info` object with `key`, `discoveryKey`, `length`, `contiguousLength`, `byteLength`, `fork`, `padding`, and optional `storage` fields.
   * @example
   * Info {
   *   key: Buffer(...),
   *   discoveryKey: Buffer(...),
   *   length: 18,
   *   contiguousLength: 16,
   *   byteLength: 742,
   *   fork: 0,
   *   padding: 8,
   *   storage: {
   *     oplog: 8192,
   *     tree: 4096,
   *     blocks: 4096,
   *     bitfield: 4096
   *   }
   * }
   */
  async info(opts) {
    if (this.opened === false) await this.opening

    return Info.from(this, opts)
  }

  /**
   * Waits for initial proof of the new core length until all `findingPeers`
   * calls have finished.
   * @param {UpdateOptions} [opts]
   * @returns {Promise<boolean>} `true` if the core was updated to a new length, `false` otherwise.
   * @example
   * const updated = await core.update()
   *
   * console.log('core was updated?', updated, 'length is', core.length)
   */
  async update(opts) {
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
      if (isRequestsDestroyed(activeRequests)) throw REQUEST_CANCELLED()

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

  /**
   * Seek to a byte offset.
   * @param {number} bytes - The byte offset to seek to (zero-based).
   * @param {SeekOptions} [opts] - Options controlling wait and timeout behaviour.
   * @returns {Promise<[number, number]>} `[index, relativeOffset]`, where `index` is the data block the `bytes` is contained in and `relativeOffset` is the relative byte offset in the data block.
   * @throws {SESSION_CLOSED} if the core has been closed.
   * @throws {ASSERTION} if `bytes` is not a valid byte offset.
   * @example
   * await core.append([Buffer.from('abc'), Buffer.from('d'), Buffer.from('efg')])
   *
   * const first = await core.seek(1) // returns [0, 1]
   * const second = await core.seek(3) // returns [1, 0]
   * const third = await core.seek(5) // returns [2, 1]
   */
  async seek(bytes, opts) {
    if (this.opened === false) await this.opening
    if (!isValidIndex(bytes)) throw ASSERTION('seek is invalid', this.discoveryKey)

    const activeRequests = (opts && opts.activeRequests) || this.activeRequests
    if (isRequestsDestroyed(activeRequests)) throw REQUEST_CANCELLED()

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

    if (this.closing !== null) {
      throw SESSION_CLOSED('cannot seek on a closed session', this.discoveryKey)
    }

    if (!this._shouldWait(opts, this.wait)) return null

    if (isRequestsDestroyed(activeRequests)) throw REQUEST_CANCELLED()
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

  /**
   * Check if the core has all blocks between `start` and `end`.
   * @param {number} start - Zero-based index of the first block to check.
   * @param {number} [end] - Exclusive end index (defaults to `start + 1`, i.e. checks a single block).
   * @returns {Promise<boolean>} `true` if every block in `[start, end)` is available locally.
   * @throws {ASSERTION} if the `start`/`end` range is invalid.
   * @example
   * const hasBlock = await core.has(5)
   * const hasRange = await core.has(0, 10)
   */
  async has(start, end = start + 1) {
    if (this.opened === false) await this.opening
    if (!isValidIndex(start) || !isValidIndex(end)) {
      throw ASSERTION('has range is invalid', this.discoveryKey)
    }

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

    return count === end - start
  }

  /**
   * Get a block of data. If the data is not available locally this method will
   * prioritize and wait for the data to be downloaded.
   * @param {number} index - Zero-based index of the block to retrieve.
   * @param {GetOptions} [opts]
   * @returns {Promise<Buffer|null>} The block value (decoded per `valueEncoding`), or `null` if the block is not available and `wait` is `false`.
   * @throws {SESSION_CLOSED} if the core has been closed.
   * @throws {ASSERTION} if `index` is not a valid block index.
   * @example
   * // get block #42
   * const block = await core.get(42)
   *
   * // get block #43, but only wait 5s
   * const blockIfFast = await core.get(43, { timeout: 5000 })
   *
   * // get block #44, but only if we have it locally
   * const blockLocal = await core.get(44, { wait: false })
   */
  async get(index, opts) {
    if (this.opened === false) await this.opening
    if (!isValidIndex(index)) throw ASSERTION('block index is invalid', this.discoveryKey)

    if (this.closing !== null) {
      throw SESSION_CLOSED('cannot get on a closed session', this.discoveryKey)
    }

    const encoding =
      (opts && opts.valueEncoding && c.from(opts.valueEncoding)) || this.valueEncoding

    if (this.onseq !== null) this.onseq(index, this)
    if (this._marking) await this.markBlock(index)

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

  /**
   * Clear stored blocks between `start` and `end`, reclaiming storage when
   * possible.
   * @param {number} start - Zero-based index of the first block to clear.
   * @param {number} [end] - Exclusive end index (defaults to `start + 1`).
   * @param {ClearOptions} [opts]
   * @returns {Promise<{blocks: number}|null>} `{ blocks }` with the count of cleared blocks when `opts.diff` is true, otherwise `null`.
   * @throws {SESSION_CLOSED} if the core has been closed.
   * @throws {ASSERTION} if the `start`/`end` range is invalid.
   * @example
   * await core.clear(4) // clear block 4 from your local cache
   * await core.clear(0, 10) // clear block 0-10 from your local cache
   */
  async clear(start, end = start + 1, opts) {
    if (this.opened === false) await this.opening
    if (this.closing !== null) {
      throw SESSION_CLOSED('cannot clear on a closed session', this.discoveryKey)
    }

    if (typeof end === 'object') {
      opts = end
      end = start + 1
    }

    if (!isValidIndex(start) || !isValidIndex(end)) {
      throw ASSERTION('clear range is invalid', this.discoveryKey)
    }

    const cleared = opts && opts.diff ? { blocks: 0 } : null

    if (start >= end) return cleared
    if (start >= this.length) return cleared

    await this.state.clear(start, end, cleared)

    return cleared
  }

  async purge() {
    await this._closeAllSessions(null)
    await this.core.purge()
  }

  async _get(index, opts) {
    const block = await readBlock(this.state.storage.read(), index)

    if (block !== null) return block

    if (this.closing !== null) {
      throw SESSION_CLOSED('cannot get on a closed session', this.discoveryKey)
    }

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

    this.waits++
    if (opts && opts.onwait) opts.onwait(index, this)
    if (this.onwait) this.onwait(index, this)

    const activeRequests = (opts && opts.activeRequests) || this.activeRequests
    if (isRequestsDestroyed(activeRequests)) throw REQUEST_CANCELLED()

    const force = opts ? opts.force === true : false
    const req = this.core.replicator.addBlock(activeRequests, index, force)
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

  _shouldWait(opts, defaultValue) {
    if (opts) {
      if (opts.wait === false) return false
      if (opts.wait === true) return true
    }
    return defaultValue
  }

  _setupMarks() {
    if (this._marks === null) {
      const storage = this.snapshotted ? this.core.state.storage : this.state.storage
      this._marks = new MarkBitfield(storage)
    }
  }

  /**
   * Manually mark a block or range of blocks to be retained when sweeping.
   * Useful to mark blocks without loading them into memory. `end` is
   * non-inclusive and defaults to `start + 1` so `core.markBlock(index)` only
   * marks the block at `index`.
   * @param {number} start - Zero-based index of the first block to mark.
   * @param {number} [end]
   * @returns {Promise<void>} Resolves once all marks in the range have been written to storage.
   * @example
   * await core.markBlock(5)       // mark block 5
   * await core.markBlock(0, 10)   // mark blocks 0–9
   */
  async markBlock(start, end = start + 1) {
    if (this.opened === false) await this.opening

    this._setupMarks()

    // TODO support as single rocks batch
    const setPromises = []
    for (let i = start; i < end; i++) {
      setPromises.push(this._marks.set(i, true))
    }

    return Promise.all(setPromises)
  }

  /**
   * Manually remove all markings. Automatically called when calling
   * `core.startMarking()`.
   * @returns {Promise<void>} Resolves once all marks have been cleared from storage.
   * @example
   * await core.clearMarkings()
   */
  async clearMarkings() {
    if (this.opened === false) await this.opening

    this._setupMarks()

    await this._marks.clear()
    this._marks = null
  }

  /**
   * This enables marking mode for the "mark & sweep" approach to clear hypercore
   * storage. When called the current markings are cleared.
   * @returns {Promise<void>} Resolves once marking mode is active and previous marks are cleared.
   * @throws {ASSERTION} if the core is already in gc mode, or is a named or atomic session.
   * @example
   * await core.startMarking()
   * await core.get(2)
   * await core.get(4)
   * await core.sweep() // All blocks but blocks 2 & 4 are cleared
   */
  async startMarking() {
    if (this._marking) {
      throw ASSERTION("Hypercore cannot be gc'ed when already in gc mode", this.discoveryKey)
    }
    if (this.state && this.state.name) {
      throw ASSERTION("Hypercore cannot be gc'ed when a named session", this.discoveryKey)
    }
    if (this.state && this.state.storage.atom) {
      throw ASSERTION("Hypercore cannot be gc'ed when an atomic session", this.discoveryKey)
    }
    if (this.opened === false) await this.opening
    await this.clearMarkings()

    this._marking = true
  }

  /**
   * Clear all unmarked blocks from storage.
   * @param {SweepOptions} [options]
   * @returns {Promise<void>} Resolves once all unmarked blocks have been cleared.
   * @example
   * {
   *   batchSize: 1000 // How frequently to flush clears to storage.
   * }
   */
  async sweep({ batchSize = 1000 } = {}) {
    if (this.opened === false) await this.opening

    assert(!this.snapshotted, 'Cannot sweep a snapshot')

    // No marks - load from storage
    this._setupMarks()

    let clearing = []
    let prevIndex = this.length
    for await (const index of this._marks.createMarkStream({ reverse: true })) {
      if (index + 1 === prevIndex) {
        prevIndex = index
        continue
      }
      clearing.push(this.clear(index + 1, prevIndex))
      if (clearing.length >= batchSize) {
        await Promise.all(clearing)
        clearing = []
      }
      prevIndex = index
    }
    // Clear range from the very start if not marked
    if (prevIndex > 0) clearing.push(this.clear(0, prevIndex))
    await Promise.all(clearing)

    this._marking = false
    await this.clearMarkings()
  }

  /**
   * Make a read stream to read a range of data out at once.
   * @param {ReadStreamOptions} [opts]
   * @returns {object} A Readable stream that emits decoded blocks.
   * @example
   * // read the full core
   * const fullStream = core.createReadStream()
   *
   * // read from block 10-14
   * const partialStream = core.createReadStream({ start: 10, end: 15 })
   *
   * // pipe the stream somewhere using the .pipe method on Node.js or consume it as
   * // an async iterator
   *
   * for await (const data of fullStream) {
   *   console.log('data:', data)
   * }
   */
  createReadStream(opts) {
    return new ReadStream(this, opts)
  }

  /**
   * Make a write stream to append chunks as blocks.
   * @returns {object} A Writable stream; each chunk written becomes one block appended to the core.
   * @example
   * const ws = core.createWriteStream()
   *
   * // Listen for stream finishing
   * const done = new Promise((resolve) => ws.on('finish', resolve))
   *
   * for (const data of ['hello', 'world']) ws.write(data)
   * ws.end()
   *
   * await done
   *
   * console.log(await core.get(core.length - 2)) // 'hello'
   * console.log(await core.get(core.length - 1)) // 'world'
   */
  createWriteStream() {
    return new WriteStream(this)
  }

  /**
   * Make a byte stream to read a range of bytes.
   * @param {ByteStreamOptions} [opts]
   * @returns {object} A Readable stream that emits raw Buffer chunks spanning the requested byte range.
   * @example
   * // Read the full core
   * const fullStream = core.createByteStream()
   *
   * // Read from byte 3, and from there read 50 bytes
   * const partialStream = core.createByteStream({ byteOffset: 3, byteLength: 50 })
   *
   * // Consume it as an async iterator
   * for await (const data of fullStream) {
   *   console.log('data:', data)
   * }
   *
   * // Or pipe it somewhere like any stream:
   * partialStream.pipe(process.stdout)
   */
  createByteStream(opts) {
    return new ByteStream(this, opts)
  }

  /**
   * Download a range of data.
   * @param {DownloadRange} [range] - The block range to download. Omit to download the entire core.
   * @returns {object} A Download handle with a `.done()` promise and a `.destroy()` method to cancel.
   * @example
   * const download = core.download({ start: 0, end: 10 })
   * await download.done()
   */
  download(range) {
    return new Download(this, range)
  }

  // TODO: get rid of this / deprecate it?
  undownload(range) {
    range.destroy(null)
  }

  // TODO: get rid of this / deprecate it?
  cancel(request) {
    // Do nothing for now
  }

  /**
   * Truncate the core to a smaller length.
   * @param {number} [newLength] - The target length to truncate to (must be ≤ current `core.length`).
   * @param {TruncateOptions} [opts]
   * @returns {Promise<void>} Resolves once the truncation has been signed and written to storage.
   * @throws {SESSION_CLOSED} if the core has been closed.
   * @throws {SESSION_NOT_WRITABLE} if the core is not writable.
   * @throws {INVALID_OPERATION} if the truncation would break the manifest prologue.
   * @example
   * {
   *   fork: core.fork + 1, // The new fork id after truncating
   *   keyPair: core.keyPair, // Key pair used for signing the truncation
   *   signature: null, // Set signature for truncation
   * }
   */
  async truncate(newLength = 0, opts = {}) {
    if (this.opened === false) await this.opening
    if (this.closing) throw SESSION_CLOSED('Cannot append to a closed session', this.discoveryKey)

    const {
      fork = this.state.fork + 1,
      keyPair = this.keyPair,
      signature = null
    } = typeof opts === 'number' ? { fork: opts } : opts

    const isDefault = this.state === this.core.state
    const writable = !this._readonly && !!(signature || (keyPair && keyPair.secretKey))
    if (isDefault && writable === false && (newLength > 0 || fork !== this.state.fork)) {
      throw SESSION_NOT_WRITABLE('cannot append to a non-writable core', this.discoveryKey)
    }

    await this.state.truncate(newLength, fork, { keyPair, signature })

    // TODO: Should propagate from an event triggered by the oplog
    if (this.state === this.core.state) this.core.replicator.updateAll()
  }

  /**
   * Append a block of data (or an array of blocks) to the core. Returns the new
   * length and byte length of the core.
   * @param {Buffer|Array<Buffer>} blocks - A single block, or an array of blocks, to append.
   * @param {AppendOptions} [opts]
   * @returns {Promise<{length: number, byteLength: number}>} The new `length` and `byteLength` of the core after appending.
   * @throws {SESSION_CLOSED} if the core has been closed.
   * @throws {SESSION_NOT_WRITABLE} if the core is not writable.
   * @throws {INVALID_OPERATION} if the append is inconsistent with the manifest prologue.
   * @throws {BAD_ARGUMENT} if an appended block exceeds the maximum suggested block size.
   * @example
   * // simple call append with a new block of data
   * await core.append(Buffer.from('I am a block of data'))
   *
   * // pass an array to append multiple blocks as a batch
   * await core.append([Buffer.from('batch block 1'), Buffer.from('batch block 2')])
   */
  async append(blocks, opts = {}) {
    if (this.opened === false) await this.opening
    if (this.closing) throw SESSION_CLOSED('Cannot append to a closed session', this.discoveryKey)

    const isDefault = this.state === this.core.state
    const defaultKeyPair = this.state.name === null ? this.keyPair : null

    const { keyPair = defaultKeyPair, signature = null, maxLength, postappend = null } = opts
    const writable =
      !isDefault || !!signature || !!(keyPair && keyPair.secretKey) || opts.writable === true

    if (this._readonly || writable === false) {
      throw SESSION_NOT_WRITABLE('cannot append to a readonly core', this.discoveryKey)
    }

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
        throw BAD_ARGUMENT(
          'Appended block exceeds the maximum suggested block size',
          this.discoveryKey
        )
      }
    }

    return this.state.append(buffers, { keyPair, signature, preappend, postappend, maxLength })
  }

  /**
   * Produce the signable payload for a given tree state. The payload encodes the
   * core's `key`, tree hash (`core.treeHash()`), `length`, and `fork`.
   * @param {number} [length] - The length to sign for (defaults to `core.length`).
   * @param {number} [fork] - The fork ID to include (defaults to `core.fork`).
   * @returns {Promise<Buffer>} a buffer which encodes the core's `key`, tree hash (`core.treeHash()`), `length`, & `fork`.
   * @example
   * const payload = await core.signable()
   * const sig = sodium.crypto_sign_detached(payload, secretKey)
   */
  async signable(length = -1, fork = -1) {
    if (this.opened === false) await this.opening
    if (length === -1) length = this.length
    if (fork === -1) fork = this.fork

    return caps.treeSignable(this.key, await this.treeHash(length), length, fork)
  }

  /**
   * Get the Merkle Tree hash of the core at a given length, defaulting to the
   * current length of the core.
   * @param {number} [length] - Tree length to hash at (defaults to `core.length`).
   * @returns {Promise<Buffer>} A 32-byte BLAKE2b hash of the Merkle tree roots at the given length.
   * @example
   * const hash = await core.treeHash()
   * console.log(hash.toString('hex'))
   */
  async treeHash(length = -1) {
    if (this.opened === false) await this.opening
    if (length === -1) length = this.length
    if (length > 0 && !(await this.has(length - 1))) await this.get(length - 1)

    const roots = await MerkleTree.getRoots(this.state, length)
    return crypto.tree(roots)
  }

  async missingNodes(index) {
    if (this.opened === false) await this.opening
    return await MerkleTree.missingNodes(this.core.state, 2 * index, this.core.state.length)
  }

  /**
   * Generate a proof (a `TreeProof` instance) for the request `opts`.
   * @param {ProofOptions} opts
   * @returns {Promise<object>} A settled `TreeProof` object that can be serialised and sent to a remote peer.
   * @example
   * {
   *   block: { index, nodes }, // Block request
   *   hash: { index, nodes }, // Hash Request
   *   seek: { bytes, padding }, // Seek Request
   *   upgrade: { start, length } // Upgrade request
   * }
   */
  async proof(opts) {
    if (this.opened === false) await this.opening
    const rx = this.state.storage.read()
    const proofPromise = MerkleTree.proof(this.state, rx, opts)
    const blockPromise = opts && opts.block ? rx.getBlock(opts.block.index) : null
    rx.tryFlush()
    const [proof, block] = await Promise.all([proofPromise, blockPromise])
    const settled = await proof.settle()
    if (block) settled.block.value = block
    return settled
  }

  async applyProof(proof, from) {
    if (this.opened === false) await this.opening
    return this.core.verify(proof, from)
  }

  /**
   * Note that you cannot seek & provide a block / hash request when upgrading.
   * @param {object} proof - A proof object as produced by a remote core's `core.proof()`.
   * @returns {Promise<object>} the merkle tree batch from the proof.
   * @example
   * const batch = await core.verifyFullyRemote(remoteProof)
   */
  async verifyFullyRemote(proof) {
    if (this.opened === false) await this.opening
    const batch = await MerkleTree.verifyFullyRemote(this.state, proof)
    await this.core._verifyBatchUpgrade(batch, proof.manifest)
    return batch
  }

  generateRemoteProofForTreeNode(treeNodeIndex) {
    const blockProofIndex = flat.rightSpan(treeNodeIndex) / 2
    return proof(this, {
      index: blockProofIndex,
      // + 1 to length so the block is included
      upgrade: { start: 0, length: blockProofIndex + 1 }
    })
  }

  async recoverFromRemoteProof(remoteProof) {
    this.core.replicator.setPushOnly(true)
    this.core._repairMode = true

    await this.core.state.mutex.lock()

    try {
      const p = await verify(this.core.db, remoteProof)
      if (!p) return false

      const tx = this.core.storage.write()
      for (const node of p.proof.upgrade.nodes) {
        tx.putTreeNode(node)
      }
      await tx.flush()

      const succeed = p.proof.upgrade.nodes.length !== 0
      if (succeed) {
        this.core.replicator.setPushOnly(false)
      }
      return succeed
    } finally {
      this.core.state.mutex.unlock()
    }
  }

  recoverTreeNodeFromPeers() {
    this.core.replicator.setPushOnly(true)

    for (const peer of this.core.replicator.peers) {
      const req = {
        id: 0,
        fork: this.fork,
        upgrade: {
          start: 0,
          length: this.length
        }
      }
      peer.wireRequest.send(req)
    }
  }

  /**
   * Register a custom protocol extension. This is a legacy implementation and is
   * no longer recommended. Creating a
   * [`Protomux`](https://github.com/holepunchto/protomux) protocol is
   * recommended instead.
   * @param {string} name - The unique name that identifies this extension across peers.
   * @param {object} [handlers]
   * @returns {object} The extension object with `send(message, peer)`, `broadcast(message)`, and `destroy()` methods.
   * @example
   * {
   *   encoding: 'json' | 'utf-8' | 'binary', // Compact encoding to use for messages. Defaults to buffer
   *   onmessage: (message, peer) => { ... } // Callback for when a message for the extension is received
   * }
   */
  registerExtension(name, handlers = {}) {
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
      send(message, peer) {
        const buffer = c.encode(this.encoding, message)
        peer.extension(name, buffer)
      },
      broadcast(message) {
        const buffer = c.encode(this.encoding, message)
        for (const peer of this.session.peers) {
          peer.extension(name, buffer)
        }
      },
      destroy() {
        for (const peer of this.session.peers) {
          if (peer.extensions.get(name) === ext) peer.extensions.delete(name)
        }
        this.session.extensions.delete(name)
      },
      _onmessage(state, peer) {
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

  _encode(enc, val) {
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

  _decode(enc, block, index) {
    if (this.encryption) block = block.subarray(this.encryption.padding(this.core, index))
    try {
      if (enc) return c.decode(enc, block)
    } catch (err) {
      throw DECODING_ERROR(err.message, this.discoveryKey)
    }
    return block
  }

  _getEncryptionProvider(e) {
    if (isEncryptionProvider(e)) return e
    if (!e || !e.key) return null
    return new DefaultEncryption(e.key, this.key, { block: e.block, compat: this.core.compat })
  }
}

module.exports = Hypercore

function isStream(s) {
  return typeof s === 'object' && s && typeof s.pipe === 'function'
}

async function preappend(blocks) {
  const offset = this.state.length
  const fork = this.state.encryptionFork

  for (let i = 0; i < blocks.length; i++) {
    await this.encryption.encrypt(offset + i, blocks[i], fork, this.core)
  }
}

function isValidIndex(index) {
  return index === 0 || index > 0
}

function maybeUnslab(block) {
  // Unslab only when it takes up less then half the slab
  return block !== null && 2 * block.byteLength < block.buffer.byteLength ? unslab(block) : block
}

function checkSnapshot(snapshot, index) {
  if (index >= snapshot.state.snapshotCompatLength) {
    throw SNAPSHOT_NOT_AVAILABLE(
      `snapshot at index ${index} not available (max compat length ${snapshot.state.snapshotCompatLength})`,
      snapshot.discoveryKey
    )
  }
}

function readBlock(rx, index) {
  const promise = rx.getBlock(index)
  rx.tryFlush()
  return promise
}

function initOnce(session, storage, key, opts) {
  if (storage === null) storage = opts.storage || null
  if (key === null) key = opts.key || null

  session.core = new Core(Hypercore.defaultStorage(storage), {
    preopen: opts.preopen,
    eagerUpgrade: opts.eagerUpgrade !== false,
    notDownloadingLinger: opts.notDownloadingLinger,
    allowFork: opts.allowFork !== false,
    allowPush: !!opts.allowPush,
    pushOnly: !!opts.pushOnly,
    alwaysLatestBlock: !!opts.allowLatestBlock,
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
    group: opts.group,
    globalCache: opts.globalCache || null // session is a temp option, not to be relied on unless you know what you are doing (no semver guarantees)
  })
}

function maybeAddMonitor(name) {
  if (name === 'append' || name === 'truncate') return
  if (this._monitorIndex >= 0 || this.closing) return

  if (this.core === null) {
    this._monitorIndex = -2
  } else {
    this.core.addMonitor(this)
  }
}

function isSessionMoved(err) {
  return err.code === 'SESSION_MOVED'
}

function getEncryptionOption(opts) {
  // old style, supported for now but will go away
  if (opts.encryptionKey) return { key: opts.encryptionKey, block: !!opts.isBlockKey }
  if (!opts.encryption) return null
  return b4a.isBuffer(opts.encryption) ? { key: opts.encryption } : opts.encryption
}

function isEncryptionProvider(e) {
  return e && isFunction(e.padding) && isFunction(e.encrypt) && isFunction(e.decrypt)
}

function isFunction(fn) {
  return !!fn && typeof fn === 'function'
}

function createDiscoveryKeyHandler(fn) {
  return ondiscoverykey
  function ondiscoverykey(id) {
    if (!id || id.byteLength !== 32) throw BAD_ARGUMENT('Invalid discovery key')
    return fn(id)
  }
}

function isRequestsDestroyed(activeRequests) {
  // TODO: move this to an object instead so we can store a property in next major
  return activeRequests.length > 0 && activeRequests[0] === null
}
