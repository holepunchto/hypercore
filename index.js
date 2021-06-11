const { EventEmitter } = require('events')
const raf = require('random-access-file')
const isOptions = require('is-options')
const codecs = require('codecs')
const crypto = require('hypercore-crypto')
const MerkleTree = require('./lib/merkle-tree')
const BlockStore = require('./lib/block-store')
const Bitfield = require('./lib/bitfield')
const Replicator = require('./lib/replicator')
const Info = require('./lib/info')
const Extensions = require('./lib/extensions')
const mutexify = require('mutexify/promise')
const fsctl = requireMaybe('fsctl') || { lock: noop, sparse: noop }
const NoiseSecretStream = require('noise-secret-stream')

const promises = Symbol.for('hypercore.promises')
const inspect = Symbol.for('nodejs.util.inspect.custom')

module.exports = class Hypercore extends EventEmitter {
  constructor (storage, key, opts) {
    super()

    if (isOptions(key)) {
      opts = key
      key = null
    }
    if (!opts) opts = {}

    this[promises] = true
    this.options = opts

    this.crypto = crypto
    this.storage = defaultStorage(storage)
    this.lock = mutexify()

    this.tree = null
    this.blocks = null
    this.bitfield = null
    this.info = null
    this.replicator = null
    this.extensions = opts.extensions || new Extensions(this)

    this.sign = opts.sign || null
    if (this.sign === null && opts.keyPair && opts.keyPair.secretKey) {
      this.sign = defaultSign(this.crypto, key, opts.keyPair.secretKey)
    }

    this.valueEncoding = opts.valueEncoding ? codecs(opts.valueEncoding) : null
    this.key = key || null
    this.discoveryKey = null
    this.readable = true
    this.opened = false
    this.closed = false
    this.sessions = opts._sessions || [this]

    this.opening = opts._opening || this.ready()
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
    const s = new Clz(this.storage, this.key, {
      ...opts,
      sign: opts.sign || this.sign,
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
    this.opened = o.opened
    this.lock = o.lock
    this.key = o.key
    this.discoveryKey = o.discoveryKey
    this.info = o.info
    this.replicator = o.replicator
    this.tree = o.tree
    this.blocks = o.blocks
    this.bitfield = o.bitfield
  }

  async close () {
    await this.opening

    const i = this.sessions.indexOf(this)
    if (i === -1) return

    this.sessions.splice(i, 1)
    this.readable = false
    this.closed = true

    if (this.sessions.length) {
      // wait a tick
      await Promise.resolve()
      // emit "fake" close as this is a session
      this.emit('close', false)
      return
    }

    await Promise.all([
      this.bitfield.close(),
      this.info.close(),
      this.tree.close(),
      this.blocks.close()
    ])

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
      this.replicator.joinProtocol(protocol)
    } else {
      this.opening.then(() => this.replicator.joinProtocol(protocol), protocol.destroy.bind(protocol))
    }

    return outerStream
  }

  get writable () {
    return this.readable && this.sign !== null
  }

  get length () {
    return this.tree === null ? 0 : this.tree.length
  }

  get byteLength () {
    return this.tree === null ? 0 : this.tree.byteLength
  }

  get fork () {
    return this.tree === null ? 0 : this.tree.fork
  }

  get peers () {
    return this.replicator === null ? [] : this.replicator.peers
  }

  async ready () {
    if (this.opening) return this.opening

    // We need to set this pre any async ticks so that range objects can be returned
    this.replicator = new Replicator(this)

    if (this.options.preload) {
      this.options = { ...this.options, ...(await this.options.preload()) }
    }

    this.info = await Info.open(this.storage('info'), {
      crypto: this.crypto,
      publicKey: this.key,
      ...this.options.keyPair
    })
    this.key = this.info.publicKey
    const fork = this.info.fork
    const secretKey = this.info.secretKey

    this.tree = await MerkleTree.open(this.storage('tree'), { crypto: this.crypto, fork })
    this.blocks = new BlockStore(this.storage('data'), this.tree)
    this.bitfield = await Bitfield.open(this.storage('bitfield'))

    // TODO: If both a secretKey and a sign option are provided, sign takes precedence.
    // In the future we can try to determine if they're equivalent, and error otherwise.
    if (secretKey && this.sign === null) {
      this.sign = defaultSign(this.crypto, this.key, secretKey)
    }

    this.discoveryKey = this.crypto.discoveryKey(this.key)

    this.replicator.checkRanges()
    this.opened = true

    for (let i = 0; i < this.sessions.length; i++) {
      const s = this.sessions[i]
      if (s !== this) s._initSession(this)
      s.emit('ready')
    }
  }

  async update () {
    if (this.opened === false) await this.opening
    // TODO: add an option where a writer can bootstrap it's state from the network also
    if (this.sign !== null) return false
    return this.replicator.requestUpgrade()
  }

  async seek (bytes) {
    if (this.opened === false) await this.opening

    const s = this.tree.seek(bytes)

    return (await s.update()) || this.replicator.requestSeek(s)
  }

  async has (index) {
    if (this.opened === false) await this.opening

    return this.bitfield.get(index)
  }

  async get (index, opts) {
    if (this.opened === false) await this.opening
    const encoding = (opts && opts.valueEncoding) || this.valueEncoding

    if (this.bitfield.get(index)) return decode(encoding, await this.blocks.get(index))
    if (opts && opts.onwait) opts.onwait(index)

    return decode(encoding, await this.replicator.requestBlock(index))
  }

  download (range) {
    return this.replicator.requestRange(range.start, range.end, !!range.linear)
  }

  undownload (range) {
    range.destroy(null)
  }

  async truncate (newLength = 0, fork = -1) {
    if (this.opened === false) await this.opening
    if (this.sign === null) throw new Error('Core is not writable')

    const release = await this.lock()
    let oldLength = 0

    try {
      if (fork === -1) fork = this.info.fork + 1

      const batch = await this.tree.truncate(newLength, { fork })

      const signature = await this.sign(batch.signable())

      this.info.fork = fork
      this.info.signature = signature

      oldLength = this.tree.length

      // TODO: same thing as in append
      await this.info.flush()

      for (let i = newLength; i < oldLength; i++) {
        this.bitfield.set(i, false)
      }
      batch.commit()
    } finally {
      release()
    }

    for (let i = 0; i < this.sessions.length; i++) {
      this.sessions[i].emit('truncate')
    }
    this.replicator.broadcastInfo()

    // Same note about background processing as below in append
    await this.tree.flush()
    await this.bitfield.flush()
  }

  async append (blocks) {
    if (this.opened === false) await this.opening
    if (this.sign === null) throw new Error('Core is not writable')
    blocks = Array.isArray(blocks) ? blocks : [blocks]

    const release = await this.lock()
    let oldLength = 0
    let newLength = 0

    try {
      const batch = this.tree.batch()
      const buffers = new Array(blocks.length)

      for (let i = 0; i < blocks.length; i++) {
        const blk = blocks[i]

        const buf = Buffer.isBuffer(blk)
          ? blk
          : this.valueEncoding
            ? this.valueEncoding.encode(blk)
            : Buffer.from(blk)

        buffers[i] = buf
        batch.append(buf)
      }

      // write the blocks, if this fails, we'll just overwrite them later
      await this.blocks.putBatch(this.tree.length, buffers)

      const signature = await this.sign(batch.signable())

      // TODO: needs to written first, then updated
      this.info.signature = signature

      oldLength = this.tree.length
      newLength = oldLength + buffers.length

      // TODO: atomically persist that we wanna write these blocks now
      // to the info file, so we can recover if the post-commit stuff fails
      await this.info.flush()

      for (let i = oldLength; i < newLength; i++) {
        this.bitfield.set(i, true)
      }
      batch.commit()
    } finally {
      release()
    }

    for (let i = 0; i < this.sessions.length; i++) {
      this.sessions[i].emit('append')
    }

    // TODO: all these broadcasts should be one
    this.replicator.broadcastInfo()
    for (let i = oldLength; i < newLength; i++) {
      this.replicator.broadcastBlock(i)
    }

    // technically we could run these in the background for more perf
    // as soon as we persist more stuff to the info as the writes here
    // are recoverable from the above blocks

    await this.tree.flush()
    await this.bitfield.flush()

    return oldLength
  }

  registerExtension (name, handlers) {
    return this.extensions.register(name, handlers)
  }

  // called by the extensions
  onextensionupdate () {
    if (this.replicator !== null) this.replicator.broadcastOptions()
  }

  // called by the replicator
  ondownload (block, upgraded, peer) {
    if (block) {
      for (let i = 0; i < this.sessions.length; i++) {
        this.sessions[i].emit('download', block.index, block.value, peer)
      }
    }

    if (upgraded) {
      for (let i = 0; i < this.sessions.length; i++) {
        this.sessions[i].emit('append')
      }
    }
  }

  // called by the replicator
  onreorg () {
    for (let i = 0; i < this.sessions.length; i++) {
      this.sessions[i].emit('reorg', this.info.fork)
    }
  }

  onpeeradd (peer) {
    this.extensions.update(peer)

    for (let i = 0; i < this.sessions.length; i++) {
      this.sessions[i].emit('peer-add', peer)
    }
  }

  onpeerremove (peer) {
    for (let i = 0; i < this.sessions.length; i++) {
      this.sessions[i].emit('peer-remove', peer)
    }
  }
}

function noop () {}

function defaultStorage (storage) {
  if (typeof storage !== 'string') return storage
  const directory = storage
  return function createFile (name) {
    const lock = name === 'info' ? fsctl.lock : null
    const sparse = name !== 'info' ? fsctl.sparse : null
    return raf(name, { directory, lock, sparse })
  }
}

function defaultSign (crypto, publicKey, secretKey) {
  if (!crypto.validateKeyPair({ publicKey, secretKey })) throw new Error('Invalid key pair')
  return signable => crypto.sign(signable, secretKey)
}

function decode (enc, buf) {
  return enc ? enc.decode(buf) : buf
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
