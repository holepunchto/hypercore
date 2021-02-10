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
const Writer = require('./lib/writer')
const Extension = require('./lib/extension')
const lock = requireMaybe('fd-lock')

const promises = Symbol.for('hypercore.promises')
const inspect = Symbol.for('nodejs.util.inspect.custom')

module.exports = class Omega extends EventEmitter {
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
    this.tree = null
    this.blocks = null
    this.bitfield = null
    this.info = null
    this.writer = null
    this.replicator = null
    this.extensions = opts.extensions || Extension.createLocal(this)

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

  static createProtocolStream () {
    return Replicator.createStream()
  }

  session () {
    const s = new Omega(this.key, this.storage, {
      valueEncoding: this.valueEncoding,
      secretKey: this._externalSecretKey,
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
    this.key = o.key
    this.discoveryKey = o.discoveryKey
    this.writer = o.writer
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
    let stream = isStream(isInitiator)
      ? isInitiator
      : opts.stream

    if (!stream) stream = Replicator.createStream()

    if (this.opened) {
      this.replicator.joinStream(stream)
    } else {
      const join = this.replicator.joinStream.bind(this.replicator, stream)
      this.opening.then(join, stream.destroy.bind(stream))
    }

    return stream
  }

  get writable () {
    return this.readable && this.writer !== null
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

    if (this.options.preload) {
      this.options = { ...this.options, ...(await this.options.preload()) }
    }

    this.info = await Info.open(this.storage('info'), {
      crypto: this.crypto,
      publicKey: this.key,
      ...this.options.keyPair
    })
    this.key = this.info.publicKey
    const secretKey = this.info.secretKey
    const fork = this.info.fork

    this.replicator = new Replicator(this)
    this.tree = await MerkleTree.open(this.storage('tree'), { crypto: this.crypto, fork })
    this.blocks = new BlockStore(this.storage('data'), this.tree)
    this.bitfield = await Bitfield.open(this.storage('bitfield'))
    if (this.info.secretKey) this.writer = new Writer(secretKey, this)

    this.discoveryKey = this.crypto.discoveryKey(this.key)
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
    if (this.writer !== null) return false
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

  async truncate (len = 0, fork = -1) {
    if (this.opened === false) await this.opening
    return this.writer.truncate(len, fork)
  }

  async append (datas) {
    if (this.opened === false) await this.opening
    return this.writer.append(Array.isArray(datas) ? datas : [datas])
  }

  registerExtension (name, handlers) {
    return this.extensions.add(name, handlers)
  }

  // called by the extensions
  onextensionupdate () {
    if (this.replicator !== null) this.replicator.broadcastOptions()
  }

  // called by the writer
  ontruncate () {
    for (let i = 0; i < this.sessions.length; i++) {
      this.sessions[i].emit('truncate')
    }
  }

  // called by the writer
  onappend () {
    for (let i = 0; i < this.sessions.length; i++) {
      this.sessions[i].emit('append')
    }
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
  if (typeof storage === 'string') {
    const directory = storage
    return name => raf(name, { directory, lock: name === 'info' ? lock : null })
  }
  return storage
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
