const { EventEmitter } = require('events')
const raf = require('random-access-file')
const crypto = require('hypercore-crypto')
const MerkleTree = require('./lib/merkle-tree')
const BlockStore = require('./lib/block-store')
const Bitfield = require('./lib/bitfield')
const Replicator = require('./lib/replicator')
const Info = require('./lib/info')
const Extension = require('./lib/extension')

const promises = Symbol.for('hypercore.promises')
const inspect = Symbol.for('nodejs.util.inspect.custom')

module.exports = class Omega extends EventEmitter {
  constructor (storage, key) {
    super()

    this[promises] = true
    this.crypto = crypto
    this.storage = defaultStorage(storage)
    this.tree = null
    this.blocks = null
    this.bitfield = null
    this.info = null
    this.replicator = new Replicator(this)
    this.extensions = Extension.createLocal(this)

    this.key = key || null
    this.discoveryKey = null
    this.opened = false
    this.fork = 0

    this.opening = this.ready()
    this.opening.catch(noop)

    this.replicator.on('peer-add', peer => this.emit('peer-add', peer))
    this.replicator.on('peer-remove', peer => this.emit('peer-remove', peer))
  }

  [inspect] (depth, opts) {
    let indent = ''
    if (typeof opts.indentationLvl === 'number') {
      while (indent.length < opts.indentationLvl) indent += ' '
    }

    return 'Omega(\n' +
      indent + '  key: ' + opts.stylize((this.key && this.key.toString('hex')), 'string') + '\n' +
      indent + '  discoveryKey: ' + opts.stylize((this.discoveryKey && this.discoveryKey.toString('hex')), 'string') + '\n' +
      indent + '  opened: ' + opts.stylize(this.opened, 'boolean') + '\n' +
      indent + '  length: ' + opts.stylize(this.length, 'number') + '\n' +
      indent + '  byteLength: ' + opts.stylize(this.byteLength, 'number') + '\n' +
      indent + ')'
  }

  replicate () {
    return this.replicator.createStream()
  }

  get length () {
    return this.tree === null ? 0 : this.tree.length
  }

  get byteLength () {
    return this.tree === null ? 0 : this.tree.byteLength
  }

  get peers () {
    return this.replicator.peers
  }

  async proof (request) {
    if (this.opened === false) await this.opening

    const signature = this.info.signature
    const p = await this.tree.proof(request, signature)

    if (request.block) {
      p.block.value = request.block.value ? await this.blocks.get(request.block.index) : null
    }

    return p
  }

  verifySignature (message, signature) {
    return this.crypto.verify(message, signature, this.key)
  }

  async verify (response, peer) {
    if (this.opened === false) await this.opening

    const len = this.tree.length
    let downloaded = false

    const b = await this.tree.verify(response)

    if (b.upgraded && !this.verifySignature(b.signable(), response.upgrade.signature, this.key)) {
      throw new Error('Remote signature does not match')
    }

    b.commit()
    this.info.fork = this.fork = this.tree.fork

    const { block } = response
    if (block && block.value && !this.bitfield.get(block.index)) {
      downloaded = true
      await this.blocks.put(block.index, block.value)
    }

    await this.tree.flush()

    if (block && block.value) {
      this.bitfield.set(block.index, true)
      await this.bitfield.flush()
    }

    if (downloaded) {
      this.emit('download', block.index, block.value, peer)
    }

    if (this.tree.length !== len) {
      this.emit('append')
    }

    if (b.upgraded) {
      this.replicator.broadcastInfo()
    }

    if (downloaded) {
      this.replicator.broadcastBlock(block.index)
    }
  }

  async ready () {
    if (this.opening) return this.opening

    this.info = await Info.open(this.storage('info'))

    // TODO: move to info.keygen or something?
    if (!this.info.publicKey) {
      if (this.key) {
        this.info.publicKey = this.key
      } else {
        const keys = this.crypto.keyPair()
        this.info.publicKey = this.key = keys.publicKey
        this.info.secretKey = keys.secretKey
      }
      await this.info.flush()
    } else {
      this.key = this.info.publicKey
    }

    if (this.key && this.info.publicKey) {
      if (!this.key.equals(this.info.publicKey)) {
        throw new Error('Another hypercore is stored here')
      }
    }

    this.tree = await MerkleTree.open(this.storage('tree'), { crypto: this.crypto })
    this.blocks = new BlockStore(this.storage('data'), this.tree)
    this.bitfield = await Bitfield.open(this.storage('bitfield'))

    this.fork = this.info.fork // TODO: get rid of this alias, unneeded
    this.discoveryKey = this.crypto.discoveryKey(this.key)
    this.opened = true
  }

  async update () {
    if (this.opened === false) await this.opening
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

    if (this.bitfield.get(index)) return this.blocks.get(index)
    if (opts && opts.onwait) opts.onwait(index)

    return this.replicator.requestBlock(index)
  }

  download (range) {
    return this.replicator.requestRange(range.start, range.end, !!range.linear)
  }

  undownload (range) {
    range.destroy(null)
  }

  async verifyFork (reorg) {
    if (this.opened === false) await this.opening

    const { fork, signature } = reorg
    const oldLength = this.length

    reorg.commit()

    const newLength = reorg.ancestors
    // TODO: we have to broadcast this truncation length also
    // so the other side can truncate their bitfields

    this.fork = this.info.fork = fork
    this.info.signature = signature

    for (let i = newLength; i < oldLength; i++) {
      this.bitfield.set(i, false)
    }

    await this.tree.flush()
    await this.info.flush()
    await this.bitfield.flush()

    this.replicator.broadcastInfo()
    this.emit('fork', this.info.fork)
  }

  async truncate (len = 0, fork = -1) {
    if (this.opened === false) await this.opening

    if (fork === -1) fork = this.info.fork + 1

    const b = await this.tree.truncate(len, { fork })

    const signature = this.info.secretKey
      ? await this.crypto.sign(b.signable(), this.info.secretKey)
      : null

    this.fork = this.info.fork = fork
    this.info.signature = signature

    const length = this.length
    b.commit()

    for (let i = len; i < length; i++) {
      this.bitfield.set(i, false)
    }

    await this.tree.flush()
    await this.info.flush()
    await this.bitfield.flush()

    this.replicator.broadcastInfo()
  }

  async append (datas) {
    if (this.opened === false) await this.opening

    if (!Array.isArray(datas)) datas = [datas]
    if (!datas.length) return

    const b = this.tree.batch()
    const all = []

    for (const data of datas) {
      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data)
      b.append(buf)
      all.push(buf)
    }

    await this.blocks.putBatch(this.tree.length, all)
    const signature = await this.crypto.sign(b.signable(), this.info.secretKey)

    b.commit()

    this.info.signature = signature

    await this.tree.flush()

    for (let i = this.tree.length - datas.length; i < this.tree.length; i++) {
      this.bitfield.set(i, true)
    }

    await this.bitfield.flush()
    await this.info.flush()

    // TODO: all these broadcasts should be one
    this.replicator.broadcastInfo()

    // TODO: should just be one broadcast
    for (let i = this.tree.length - datas.length; i < this.tree.length; i++) {
      this.replicator.broadcastBlock(i)
    }

    this.emit('append')
  }

  registerExtension (name, handlers) {
    const ext = this.extensions.add(name, handlers)
    this.replicator.broadcastOptions()
    return ext
  }
}

function noop () {}

function defaultStorage (storage) {
  if (typeof storage === 'string') return name => raf(name, { directory: storage })
  return storage
}
