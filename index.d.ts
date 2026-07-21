// Type declarations for the holepunchto/hypercore public API.

/**
 * `options`
 */
export interface HypercoreOptions {
  /** create a new Hypercore key pair if none was present in storage */
  createIfMissing?: any
  /** overwrite any old Hypercore that might already exist */
  overwrite?: any
  /** Advanced option. Will force overwrite even if the header's key & the passed key don't match */
  force?: any
  /** defaults to binary */
  valueEncoding?: any
  /** optionally apply an encoding to complete batches */
  encodeBatch?: any
  /** optionally pass the public key and secret key as a key pair */
  keyPair?: any
  /** the block encryption key */
  encryption?: any
  /** hook that is called if gets are waiting for download */
  onwait?: any
  /** wait at max some milliseconds (0 means no timeout) */
  timeout?: any
  /** disable appends and truncates */
  writable?: any
  /** Advanced option. Set to [minInflight, maxInflight] to change the min and max inflight blocks per peer when downloading. */
  inflightRange?: any
  /** A callback called when the session is garbage collected */
  ongc?: any
  /** A callback called when core.get(index) is called. */
  onseq?: any
  /** How many milliseconds to wait after downloading finishes keeping the connection open. Defaults to a random number between 20-40s */
  notDownloadingLinger?: any
  /** Enables updating core when it forks */
  allowFork?: any
  /** An object to assign to the local User Data storage */
  userData?: any
  /** Advanced option. Set the manifest when creating the hypercore. See Manifest section for more info */
  manifest?: any
  /** Advanced option. A promise that returns constructor options overrides before the core is opened */
  preload?: any
  /** An alternative to passing storage as a dedicated argument */
  storage?: any
  /** An alternative to passing key as a dedicated argument */
  key?: any
}

/**
 * `options`
 */
export interface HypercoreKeyOptions {
  /** Whether the manifest has a single signer whose public key is the key */
  compat?: any
  /** Manifest version if the manifest argument is the public key of a single signer */
  version?: any
  /** The signer namespace if the manifest argument is the public key of a single signer */
  namespace?: any
}

/**
 * `opts`
 */
export interface HypercoreCreateProtocolStreamOptions {
  /** A handler for when a discovery key is set over the stream for corestore management */
  ondiscoverykey?: any
}

/**
 * Options are inherited from the parent instance, unless they are re-set.
 */
export interface HypercoreSessionOptions {
  /** Creates the session as a "weak ref" which closes when all non-weak sessions are closed */
  weak?: any
  /** Create a session with exclusive access to the core. Creating an exclusive session on a core with other exclusive sessions, will wait for the session with access to close before the next exclusive session is `ready` */
  exclusive?: any
  /** A index to checkout the core at. Checkout sessions must be an atom or a named session */
  checkout?: any
  /** A storage atom for making atomic batch changes across hypercores */
  atom?: any
  /** Name the session creating a persisted branch of the core. Still beta so may break in the future */
  name?: any
}

/**
 * `opts`
 */
export interface HypercoreSetEncryptionKeyOptions {
  /** Whether the key is for block encryption */
  block?: any
}

/**
 * `opts`
 */
export interface HypercoreCommitOptions {
  /** the core's length after committing the blocks */
  length?: any
  /** The expected length of the core's merkle tree prior to commit */
  treeLength?: any
  /** The keypair to use when committing */
  keyPair?: any
  /** The signature for the blocks being committed */
  signature?: any
}

/**
 * `options`
 */
export interface HypercoreInfoOptions {
  /** get storage estimates in bytes, disabled by default */
  storage?: any
}

/**
 * `options`
 */
export interface HypercoreUpdateOptions {
  wait?: any
  /** Advanced option. Pass requests for replicating blocks */
  activeRequests?: any
  /** Force an update even if core is writable. */
  force?: any
}

export interface HypercoreSeekOptions {
  /** wait for data to be downloaded */
  wait?: any
  /** wait at max some milliseconds (0 means no timeout) */
  timeout?: any
  /** Advanced option. Pass requests for replicating blocks */
  activeRequests?: any
}

/**
 * `options`
 */
export interface HypercoreGetOptions {
  /** wait for block to be downloaded */
  wait?: any
  /** hook that is called if the get is waiting for download */
  onwait?: any
  /** wait at max some milliseconds (0 means no timeout) */
  timeout?: any
  /** Advanced option. Pass BlockRequest for replicating the block */
  activeRequests?: any
  /** defaults to the core's valueEncoding */
  valueEncoding?: any
  /** automatically decrypts the block if encrypted */
  decrypt?: any
  /** Return block without decoding */
  raw?: any
}

/**
 * `options`
 */
export interface HypercoreClearOptions {
  /** Returned `cleared` bytes object is null unless you enable this */
  diff?: any
}

/**
 * `opts`
 */
export interface HypercoreSweepOptions {
  /** How frequently to flush clears to storage. */
  batchSize?: any
}

/**
 * `options`
 */
export interface HypercoreCreateReadStreamOptions {
  start?: any
  end?: any
  /** Whether to wait for updates from peers */
  wait?: any
  /** How long to wait for updates from peers */
  timeout?: any
  /** Wait for next block keeping stream open / live */
  live?: any
  /** auto set end to core.length on open or update it on every read */
  snapshot?: any
}

/**
 * `options`
 */
export interface HypercoreCreateByteStreamOptions {
  /** Offset where to start from */
  byteOffset?: any
  /** How many bytes to read */
  byteLength?: any
  /** How many bytes to download at a time */
  prefetch?: any
}

export interface HypercoreDownloadOptions {
  start?: any
  end?: any
  blocks?: any
  /** download range linearly and not randomly */
  linear?: any
  /** Advanced option. Pass requests for replicating blocks */
  activeRequests?: any
}

/**
 * `options`
 */
export interface HypercoreTruncateOptions {
  /** The new fork id after truncating */
  fork?: any
  /** Key pair used for signing the truncation */
  keyPair?: any
  /** Set signature for truncation */
  signature?: any
}

/**
 * `options`
 */
export interface HypercoreAppendOptions {
  /** Enabled ignores writable check. Does not override whether core is writable. */
  writable?: any
  /** The maximum resulting length of the core after appending */
  maxLength?: any
  /** KeyPair used to sign the block(s) */
  keyPair?: any
  /** Set signature for block(s) */
  signature?: any
}

/**
 * `opts`
 */
export interface HypercoreProofOptions {
  /** Block request */
  block?: any
  /** Hash Request */
  hash?: any
  /** Seek Request */
  seek?: any
  /** Upgrade request */
  upgrade?: any
}

/**
 * `handlers`
 */
export interface HypercoreRegisterExtensionOptions {
  /** Compact encoding to use for messages. Defaults to buffer */
  encoding?: any
  /** Callback for when a message for the extension is received */
  onmessage?: any
}

export class Hypercore {
  /**
   * Make a new Hypercore instance.
   * @param storage - `storage` should be set to a directory where you want to store the data and core metadata.
   * @param key - `key` can be set to a Hypercore key which is a hash of Hypercore's internal auth manifest, describing how to validate the Hypercore.
   * @param opts - `options`
   */
  constructor(storage: any, key?: any, opts?: HypercoreOptions)

  /**
   * The constant for max size (15MB) for blocks appended to Hypercore. This max ensures blocks are replicated smoothly.
   */
  static MAX_SUGGESTED_BLOCK_SIZE: any

  static DefaultEncryption: any

  static SMALL_WANTS: any

  static enable(flag: any): any

  static setRecoveryPeers(peers: any): any

  /**
   * `options` include:
   * @param options - `options`
   * @returns Returns the key for a given manifest.
   */
  static key(manifest: any, options?: HypercoreKeyOptions): any

  /**
   * @returns Returns the discovery key for the provided `key`.
   */
  static discoveryKey(key: any): any

  /**
   * @returns Returns a block encryption key derived from the `key` and `encryptionKey`.
   */
  static blockEncryptionKey(key: any, encryptionKey: any): any

  /**
   * @returns Returns a protomux instance from the provided `stream` Hypercore protocol stream.
   */
  static getProtocolMuxer(stream: any): any

  /**
   * @returns Returns the internal core using the `storage` and `opts` without creating a full Hypercore instance.
   */
  static createCore(storage: any, opts: any): any

  /**
   * Create an encrypted noise stream with a protomux instance attached used for Hypercore's replication protocol.
   * @param isInitiator - `isInitiator` can be a framed stream, a protomux or a boolean for whether the stream should be the initiator in the noise handshake.
   * @param opts - `opts`
   */
  static createProtocolStream(isInitiator: any, opts?: HypercoreCreateProtocolStreamOptions): any

  /**
   * @returns Returns a default hypercore storage.
   */
  static defaultStorage(storage: any, opts?: any): any

  static clearRequests(session: any, err: any): any

  static destroyRequests(session: any, err: any): any

  static treeHashFromStorage(session: any, length?: any): Promise<void>

  /**
   * Same as [`core.session(options)`](#const-session--coresessionoptions), but backed by a storage snapshot so will not truncate nor append.
   */
  snapshot(opts?: any): any

  compact(): any

  /**
   * Creates a new Hypercore instance that shares the same underlying core.
   * @param opts - Options are inherited from the parent instance, unless they are re-set.
   */
  session(opts?: HypercoreSessionOptions): any

  /**
   * Set the encryption key.
   * @param opts - `opts`
   */
  setEncryptionKey(key: any, opts?: HypercoreSetEncryptionKeyOptions): Promise<void>

  /**
   * Set the encryption, which should satisfy the [HypercoreEncryption](https://github.com/holepunchto/hypercore-encryption) interface.
   */
  setEncryption(encryption: any): Promise<void>

  /**
   * Set the group `topic` that the hypercore belongs to. Useful for grouping hypercores together that need to update a larger data structure (eg. `autobee`) that is comprised of them. See `corestore`'s `store.notifyGroup(topic)` for more details.
   * @param topic - `topic` is a 32 byte buffer.
   */
  setGroup(topic: any): Promise<void>

  /**
   * Update the core's `keyPair`. Advanced as the `keyPair` is used throughout Hypercore, e.g. verifying blocks, identifying the core, etc.
   */
  setKeyPair(keyPair: any): any

  /**
   * Set the core to be active or not. A core is considered 'active' if it should linger to download blocks from peers.
   */
  setActive(bool: any): any

  readonly replicator: any

  /**
   * Fully close this core. Passing an error via `{ error }` is optional and all pending replicator requests will be rejected with the error.
   */
  close(options?: any): any

  clearRequests(activeRequests: any, error: any): any

  /**
   * Attempt to apply blocks from the session to the `core`. `core` must be a default core, aka a non-named session.
   * @param opts - `opts`
   * @returns Returns `null` if committing failed.
   */
  commit(session: any, opts?: HypercoreCommitOptions): Promise<void>

  /**
   * Create a replication stream. You should pipe this to another Hypercore instance.
   * @param opts - `opts` are same as [`Hypercore.createProtocolStream()`](#const-stream--hypercorecreateprotocolstreamisinitiator-opts--).
   */
  replicate(isInitiator: any, opts?: any): any

  /**
   * String containing the id (z-base-32 of the public key) identifying this core.
   */
  readonly id: any

  /**
   * Buffer containing the public key identifying this core.
   */
  readonly key: any

  /**
   * Buffer containing a key derived from the core's public key. In contrast to `core.key` this key does not allow you to verify the data but can be used to announce or look for peers that are sharing the same core, without leaking the core key.
   */
  readonly discoveryKey: any

  readonly manifest: any

  /**
   * How many blocks of data are available on this core.
   */
  readonly length: any

  /**
   * How many blocks of data are available on this core that have been signed by a quorum. This is equal to `core.length` for Hypercores with a single signer.
   */
  readonly signedLength: any

  /**
   * Deprecated. Use `const { byteLength } = await core.info()`.
   */
  readonly byteLength: any

  /**
   * How many blocks are contiguously available starting from the first block of this core on any known remote. This is only updated when a remote thinks it is fully contiguous such that they have all known blocks.
   */
  readonly remoteContiguousLength: any

  /**
   * How many blocks are contiguously available starting from the first block of this core.
   */
  readonly contiguousLength: any

  readonly contiguousByteLength: any

  /**
   * What is the current fork id of this core?
   */
  readonly fork: any

  /**
   * How much padding is applied to each block of this core? Will be `0` unless block encryption is enabled.
   */
  readonly padding: any

  /**
   * Array of current peers the core is replicating with.
   */
  readonly peers: any

  readonly globalCache: any

  readonly recovering: any

  /**
   * Wait for the core to fully open.
   */
  ready(): any

  recover(): Promise<void>

  /**
   * Set a key in the User Data key-value store.
   * @param key - `key` is a string and
   */
  setUserData(key: any, value: any): Promise<void>

  /**
   * `key` is a string.
   * @param key - `key` is a string.
   * @returns Return the value for a key in the User Data key-value store.
   */
  getUserData(key: any): Promise<void>

  transferSession(core: any): any

  /**
   * Create a hook that tells Hypercore you are finding peers for this core in the background. Call `done` when your current discovery iteration is done. If you're using Hyperswarm, you'd normally call this after a `swarm.flush()` finishes.
   */
  findingPeers(): any

  /**
   * Get information about this core, such as its total size in bytes.
   * @param opts - `options`
   */
  info(opts?: HypercoreInfoOptions): Promise<void>

  /**
   * Waits for initial proof of the new core length until all `findingPeers` calls have finished.
   * @param opts - `options`
   */
  update(opts?: HypercoreUpdateOptions): Promise<void>

  /**
   * Seek to a byte offset.
   * @returns Returns `[index, relativeOffset]`, where `index` is the data block the `byteOffset` is contained in and `relativeOffset` is the relative byte offset in the data block.
   */
  seek(bytes: any, opts?: HypercoreSeekOptions): Promise<void>

  /**
   * Check if the core has all blocks between `start` and `end`.
   */
  has(start: any, end?: any): Promise<void>

  /**
   * Get a block of data. If the data is not available locally this method will prioritize and wait for the data to be downloaded.
   * @param opts - `options`
   */
  get(index: any, opts?: HypercoreGetOptions): Promise<void>

  /**
   * Clear stored blocks between `start` and `end`, reclaiming storage when possible.
   * @param opts - `options`
   */
  clear(start: any, end?: any, opts?: HypercoreClearOptions): Promise<void>

  purge(): Promise<void>

  /**
   * Manually mark a block or range of blocks to be retained when sweeping. Useful to mark blocks without loading them into memory. `end` is non-inclusive and defaults to `start + 1` so `core.markBlock(index)` only marks the block at `index`.
   * @param end - `end` is non-inclusive and defaults to
   */
  markBlock(start: any, end?: any): Promise<void>

  /**
   * Manually remove all markings. Automatically called when calling `core.startMarking()`.
   */
  clearMarkings(): Promise<void>

  /**
   * This enables marking mode for the "mark & sweep" approach to clear hypercore storage. When called the current markings are cleared.
   */
  startMarking(): Promise<void>

  /**
   * Clear all unmarked blocks from storage.
   * @param options - `opts`
   */
  sweep(options?: HypercoreSweepOptions): Promise<void>

  /**
   * Make a read stream to read a range of data out at once.
   * @param opts - `options`
   */
  createReadStream(opts?: HypercoreCreateReadStreamOptions): any

  /**
   * Make a write stream to append chunks as blocks.
   */
  createWriteStream(): any

  /**
   * Make a byte stream to read a range of bytes.
   * @param opts - `options`
   */
  createByteStream(opts?: HypercoreCreateByteStreamOptions): any

  /**
   * Download a range of data.
   */
  download(range?: HypercoreDownloadOptions): any

  undownload(range: any): any

  cancel(request: any): any

  /**
   * Truncate the core to a smaller length.
   * @param opts - `options`
   */
  truncate(newLength?: any, opts?: HypercoreTruncateOptions): Promise<void>

  /**
   * Append a block of data (or an array of blocks) to the core. Returns the new length and byte length of the core.
   * @param opts - `options`
   */
  append(blocks: any, opts?: HypercoreAppendOptions): Promise<void>

  /**
   * @returns Return a buffer which encodes the core's `key`, tree hash (`core.treeHash()`), `length`, & `fork`.
   */
  signable(length?: any, fork?: any): Promise<void>

  /**
   * Get the Merkle Tree hash of the core at a given length, defaulting to the current length of the core.
   */
  treeHash(length?: any): Promise<void>

  missingNodes(index: any): Promise<void>

  /**
   * Generate a proof (a `TreeProof` instance) for the request `opts`.
   * @param opts - `opts`
   */
  proof(opts: HypercoreProofOptions): Promise<void>

  applyProof(proof: any, from: any): Promise<void>

  /**
   * Note that you cannot seek & provide a block / hash request when upgrading.
   * @returns Return the merkle tree batch from the proof.
   */
  verifyFullyRemote(proof: any): Promise<void>

  generateRemoteProofForTreeNode(treeNodeIndex: any): any

  recoverFromRemoteProof(remoteProof: any): Promise<void>

  recoverTreeNodeFromPeers(): any

  /**
   * Register a custom protocol extension. This is a legacy implementation and is no longer recommended. Creating a [`Protomux`](https://github.com/holepunchto/protomux) protocol is recommended instead.
   * @param handlers - `handlers`
   */
  registerExtension(name: any, handlers?: HypercoreRegisterExtensionOptions): any

  emit(event: any, arg1?: any): any

  core: any

  state: any

  encryption: any

  extensions: any

  valueEncoding: any

  encodeBatch: any

  activeRequests: any

  sessions: any

  ongc: any

  /**
   * Object containing buffers of the core's public and secret key
   */
  keyPair: any

  /**
   * Can we read from this core? After closing the core this will be false.
   */
  readable: any

  /**
   * Can we append to or truncate this core?
   */
  writable: any

  exclusive: any

  opened: any

  closed: any

  weak: any

  snapshotted: any

  onseq: any

  onwait: any

  wait: any

  timeout: any

  preload: any

  closing: any

  opening: any

  waits: any

  /**
   * Sends the `message` to a specific `peer`.
   */
  send(message: any, peer: any): any

  /**
   * Sends the `message` to all peers.
   */
  broadcast(message: any): any

  /**
   * Unregister and remove extension from the hypercore.
   */
  destroy(): any

  /**
   * Emitted when the core has been fully closed.
   */
  on(event: 'close', listener: () => void): this
  /**
   * Emitted after the core has initially opened all its internal state.
   */
  on(event: 'ready', listener: () => void): this
  on(event: 'migrate', listener: (key: any) => void): this
  /**
   * Emitted when the core has been appended to (i.e. has a new length / byteLength), either locally or remotely.
   */
  on(event: 'append', listener: (...args: any[]) => void): this
  /**
   * Emitted when the core has been truncated, either locally or remotely.
   */
  on(event: 'truncate', listener: (...args: any[]) => void): this
  /**
   * Emitted when a new connection has been established with a peer.
   */
  on(event: 'peer-add', listener: (...args: any[]) => void): this
  /**
   * Emitted when a peer's connection has been closed.
   */
  on(event: 'peer-remove', listener: (...args: any[]) => void): this
  /**
   * Emitted when a block is uploaded to a peer.
   */
  on(event: 'upload', listener: (...args: any[]) => void): this
  /**
   * Emitted when a block is downloaded from a peer.
   */
  on(event: 'download', listener: (...args: any[]) => void): this
  /**
   * Emitted when the max known contiguous `length` from a remote, ie `core.remoteContiguousLength`, is updated. Note this is not emitted when core is truncated.
   */
  on(event: 'remote-contiguous-length', listener: (...args: any[]) => void): this
}

export class ReadStream {
  constructor(core: any, opts?: any)

  push(data: any): any

  destroy(err?: any): any

  readonly destroyed: any

  emit(event: any, arg1?: any): any

  core: any

  start: any

  end: any

  snapshot: any

  live: any

  wait: any

  timeout: any

  on(event: 'data', listener: () => void): this
  on(event: 'readable', listener: () => void): this
  on(event: 'end', listener: () => void): this
  on(event: 'close', listener: () => void): this
  on(event: 'error', listener: () => void): this
}

export class WriteStream {
  constructor(core: any)

  write(data: any): any

  end(): any

  destroy(err?: any): any

  readonly destroyed: any

  emit(event: any, arg1?: any): any

  core: any

  on(event: 'drain', listener: () => void): this
  on(event: 'finish', listener: () => void): this
  on(event: 'close', listener: () => void): this
  on(event: 'error', listener: () => void): this
}

export class ByteStream {
  constructor(core: any, opts?: any)

  push(data: any): any

  destroy(err?: any): any

  readonly destroyed: any

  emit(event: any, arg1?: any): any

  on(event: 'data', listener: () => void): this
  on(event: 'readable', listener: () => void): this
  on(event: 'end', listener: () => void): this
  on(event: 'close', listener: () => void): this
  on(event: 'error', listener: () => void): this
}

export default Hypercore
