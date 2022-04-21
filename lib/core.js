const hypercoreCrypto = require('hypercore-crypto')
const b4a = require('b4a')
const Oplog = require('./oplog')
const Mutex = require('./mutex')
const MerkleTree = require('./merkle-tree')
const BlockStore = require('./block-store')
const Bitfield = require('./bitfield')
const { BAD_ARGUMENT, STORAGE_EMPTY, STORAGE_CONFLICT, INVALID_SIGNATURE } = require('./errors')
const m = require('./messages')

module.exports = class Core {
  constructor (header, crypto, oplog, tree, blocks, bitfield, auth, legacy, onupdate) {
    this.onupdate = onupdate
    this.header = header
    this.crypto = crypto
    this.oplog = oplog
    this.tree = tree
    this.blocks = blocks
    this.bitfield = bitfield
    this.defaultAuth = auth
    this.truncating = 0

    this._maxOplogSize = 65536
    this._autoFlush = 1
    this._verifies = null
    this._verifiesFlushed = null
    this._mutex = new Mutex()
    this._legacy = legacy
  }

  static async open (storage, opts = {}) {
    const oplogFile = storage('oplog')
    const treeFile = storage('tree')
    const bitfieldFile = storage('bitfield')
    const dataFile = storage('data')

    try {
      return await this.resume(oplogFile, treeFile, bitfieldFile, dataFile, opts)
    } catch (err) {
      return new Promise((resolve, reject) => {
        let missing = 4

        oplogFile.close(done)
        treeFile.close(done)
        bitfieldFile.close(done)
        dataFile.close(done)

        function done () {
          if (--missing === 0) reject(err)
        }
      })
    }
  }

  static createAuth (crypto, { publicKey, secretKey }, opts = {}) {
    if (secretKey && !crypto.validateKeyPair({ publicKey, secretKey })) {
      throw BAD_ARGUMENT('Invalid key pair')
    }

    const sign = opts.sign
      ? opts.sign
      : secretKey
        ? (signable) => crypto.sign(signable, secretKey)
        : undefined

    return {
      sign,
      verify (signable, signature) {
        return crypto.verify(signable, signature, publicKey)
      }
    }
  }

  static async resume (oplogFile, treeFile, bitfieldFile, dataFile, opts) {
    let overwrite = opts.overwrite === true

    const force = opts.force === true
    const createIfMissing = opts.createIfMissing !== false
    const crypto = opts.crypto || hypercoreCrypto

    const oplog = new Oplog(oplogFile, {
      headerEncoding: m.oplog.header,
      entryEncoding: m.oplog.entry
    })

    let { header, entries } = await oplog.open()

    if (force && opts.keyPair && header && header.signer && !b4a.equals(header.signer.publicKey, opts.keyPair.publicKey)) {
      overwrite = true
    }

    if (!header || overwrite) {
      if (!createIfMissing) {
        throw STORAGE_EMPTY('No Hypercore is stored here')
      }

      header = {
        types: { tree: 'blake2b', bitfield: 'raw', signer: 'ed25519' },
        userData: [],
        tree: {
          fork: 0,
          length: 0,
          rootHash: null,
          signature: null
        },
        signer: opts.keyPair || crypto.keyPair(),
        hints: {
          reorgs: []
        }
      }

      await oplog.flush(header)
    }

    if (opts.keyPair && !b4a.equals(header.signer.publicKey, opts.keyPair.publicKey)) {
      throw STORAGE_CONFLICT('Another Hypercore is stored here')
    }

    const tree = await MerkleTree.open(treeFile, { crypto, ...header.tree })
    const bitfield = await Bitfield.open(bitfieldFile)
    const blocks = new BlockStore(dataFile, tree)

    if (overwrite) {
      await tree.clear()
      await blocks.clear()
      await bitfield.clear()
      entries = []
    } else if (bitfield.resumed && header.tree.length === 0) {
      // If this was an old bitfield, reset it since it loads based on disk size atm (TODO: change that)
      await bitfield.clear()
    }

    const auth = opts.auth || this.createAuth(crypto, header.signer)

    for (const e of entries) {
      if (e.userData) {
        updateUserData(header.userData, e.userData.key, e.userData.value)
      }

      if (e.treeNodes) {
        for (const node of e.treeNodes) {
          tree.addNode(node)
        }
      }

      if (e.bitfield) {
        bitfield.setRange(e.bitfield.start, e.bitfield.length, !e.bitfield.drop)
      }

      if (e.treeUpgrade) {
        const batch = await tree.truncate(e.treeUpgrade.length, e.treeUpgrade.fork)
        batch.ancestors = e.treeUpgrade.ancestors
        batch.signature = e.treeUpgrade.signature
        addReorgHint(header.hints.reorgs, tree, batch)
        batch.commit()

        header.tree.length = tree.length
        header.tree.fork = tree.fork
        header.tree.rootHash = tree.hash()
        header.tree.signature = tree.signature
      }
    }

    return new this(header, crypto, oplog, tree, blocks, bitfield, auth, !!opts.legacy, opts.onupdate || noop)
  }

  _shouldFlush () {
    // TODO: make something more fancy for auto flush mode (like fibonacci etc)
    if (--this._autoFlush <= 0 || this.oplog.byteLength >= this._maxOplogSize) {
      this._autoFlush = 4
      return true
    }

    return false
  }

  async _flushOplog () {
    // TODO: the apis using this, actually do not need to wait for the bitfields, tree etc to flush
    // as their mutations are already stored in the oplog. We could potentially just run this in the
    // background. Might be easier to impl that where it is called instead and keep this one simple.
    await this.bitfield.flush()
    await this.tree.flush()
    await this.oplog.flush(this.header)
  }

  _appendBlocks (values) {
    return this.blocks.putBatch(this.tree.length, values, this.tree.byteLength)
  }

  async _writeBlock (batch, index, value) {
    const byteOffset = await batch.byteOffset(index * 2)
    await this.blocks.put(index, value, byteOffset)
  }

  async userData (key, value) {
    // TODO: each oplog append can set user data, so we should have a way
    // to just hitch a ride on one of the other ongoing appends?
    await this._mutex.lock()

    try {
      let empty = true

      for (const u of this.header.userData) {
        if (u.key !== key) continue
        if (value && b4a.equals(u.value, value)) return
        empty = false
        break
      }

      if (empty && !value) return

      const entry = {
        userData: { key, value },
        treeNodes: null,
        treeUpgrade: null,
        bitfield: null
      }

      await this.oplog.append([entry], false)

      updateUserData(this.header.userData, key, value)

      if (this._shouldFlush()) await this._flushOplog()
    } finally {
      this._mutex.unlock()
    }
  }

  async truncate (length, fork, auth = this.defaultAuth) {
    this.truncating++
    await this._mutex.lock()

    try {
      const batch = await this.tree.truncate(length, fork)
      batch.signature = await auth.sign(batch.signable())
      await this._truncate(batch, null)
    } finally {
      this.truncating--
      this._mutex.unlock()
    }
  }

  async append (values, auth = this.defaultAuth, hooks = {}) {
    await this._mutex.lock()

    try {
      if (hooks.preappend) await hooks.preappend(values)

      if (!values.length) return this.tree.length

      const batch = this.tree.batch()
      for (const val of values) batch.append(val)

      const hash = batch.hash()
      batch.signature = await auth.sign(this._legacy ? batch.signableLegacy(hash) : batch.signable(hash))

      const entry = {
        userData: null,
        treeNodes: batch.nodes,
        treeUpgrade: batch,
        bitfield: {
          drop: false,
          start: batch.ancestors,
          length: values.length
        }
      }

      await this._appendBlocks(values)
      await this.oplog.append([entry], false)

      this.bitfield.setRange(batch.ancestors, batch.length - batch.ancestors, true)
      batch.commit()

      this.header.tree.length = batch.length
      this.header.tree.rootHash = hash
      this.header.tree.signature = batch.signature
      this.onupdate(0b01, entry.bitfield, null, null)

      if (this._shouldFlush()) await this._flushOplog()

      return batch.ancestors
    } finally {
      this._mutex.unlock()
    }
  }

  _signed (batch, hash, auth = this.defaultAuth) {
    const signable = this._legacy ? batch.signableLegacy(hash) : batch.signable(hash)
    return auth.verify(signable, batch.signature)
  }

  async _verifyExclusive ({ batch, bitfield, value, from }) {
    // TODO: move this to tree.js
    const hash = batch.hash()
    if (!batch.signature || !this._signed(batch, hash)) {
      throw INVALID_SIGNATURE('Proof contains an invalid signature')
    }

    await this._mutex.lock()

    try {
      if (!batch.commitable()) return false

      const entry = {
        userData: null,
        treeNodes: batch.nodes,
        treeUpgrade: batch,
        bitfield
      }

      if (bitfield) await this._writeBlock(batch, bitfield.start, value)

      await this.oplog.append([entry], false)

      if (bitfield) this.bitfield.set(bitfield.start, true)
      batch.commit()

      this.header.tree.fork = batch.fork
      this.header.tree.length = batch.length
      this.header.tree.rootHash = batch.rootHash
      this.header.tree.signature = batch.signature
      this.onupdate(0b01, bitfield, value, from)

      if (this._shouldFlush()) await this._flushOplog()
    } finally {
      this._mutex.unlock()
    }

    return true
  }

  async _verifyShared () {
    if (!this._verifies.length) return false

    await this._mutex.lock()

    const verifies = this._verifies
    this._verifies = null
    this._verified = null

    try {
      const entries = []

      for (const { batch, bitfield, value } of verifies) {
        if (!batch.commitable()) continue

        if (bitfield) {
          await this._writeBlock(batch, bitfield.start, value)
        }

        entries.push({
          userData: null,
          treeNodes: batch.nodes,
          treeUpgrade: null,
          bitfield
        })
      }

      await this.oplog.append(entries, false)

      for (let i = 0; i < verifies.length; i++) {
        const { batch, bitfield, value, from } = verifies[i]

        if (!batch.commitable()) {
          verifies[i] = null // signal that we cannot commit this one
          continue
        }

        if (bitfield) this.bitfield.set(bitfield.start, true)
        batch.commit()
        this.onupdate(0, bitfield, value, from)
      }

      if (this._shouldFlush()) await this._flushOplog()
    } finally {
      this._mutex.unlock()
    }

    return verifies[0] !== null
  }

  async verify (proof, from) {
    // We cannot apply "other forks" atm.
    // We should probably still try and they are likely super similar for non upgrades
    // but this is easy atm (and the above layer will just retry)

    if (proof.fork !== this.tree.fork) return false

    const batch = await this.tree.verify(proof)
    if (!batch.commitable()) return false

    const value = (proof.block && proof.block.value) || null
    const op = {
      batch,
      bitfield: value && { drop: false, start: proof.block.index, length: 1 },
      value: value,
      from
    }

    if (batch.upgraded) return this._verifyExclusive(op)

    if (this._verifies !== null) {
      const verifies = this._verifies
      const i = verifies.push(op)
      await this._verified
      return verifies[i] !== null
    }

    this._verifies = [op]
    this._verified = this._verifyShared()
    return this._verified
  }

  async reorg (batch, from) {
    if (!batch.commitable()) return false

    this.truncating++
    await this._mutex.lock()

    try {
      if (!batch.commitable()) return false
      await this._truncate(batch, from)
    } finally {
      this.truncating--
      this._mutex.unlock()
    }

    return true
  }

  async _truncate (batch, from) {
    const entry = {
      userData: null,
      treeNodes: batch.nodes,
      treeUpgrade: batch,
      bitfield: {
        drop: true,
        start: batch.ancestors,
        length: this.tree.length - batch.ancestors
      }
    }

    await this.oplog.append([entry], false)

    this.bitfield.setRange(batch.ancestors, this.tree.length - batch.ancestors, false)
    addReorgHint(this.header.hints.reorgs, this.tree, batch)
    batch.commit()

    const appended = batch.length > batch.ancestors

    this.header.tree.fork = batch.fork
    this.header.tree.length = batch.length
    this.header.tree.rootHash = batch.hash()
    this.header.tree.signature = batch.signature
    this.onupdate(appended ? 0b11 : 0b10, entry.bitfield, null, from)

    // TODO: there is a bug in the merkle tree atm where it cannot handle unflushed
    // truncates if we append or download anything after the truncation point later on
    // This is because tree.get checks the truncated flag. We should fix this so we can do
    // the later flush here as well
    // if (this._shouldFlush()) await this._flushOplog()
    await this._flushOplog()
  }

  async close () {
    await this._mutex.destroy()
    await Promise.allSettled([
      this.oplog.close(),
      this.bitfield.close(),
      this.tree.close(),
      this.blocks.close()
    ])
  }
}

function addReorgHint (list, tree, batch) {
  if (tree.length === 0 || tree.fork === batch.fork) return

  while (list.length >= 4) list.shift() // 4 here is arbitrary, just want it to be small (hints only)
  while (list.length > 0) {
    if (list[list.length - 1].ancestors > batch.ancestors) list.pop()
    else break
  }

  list.push({ from: tree.fork, to: batch.fork, ancestors: batch.ancestors })
}

function updateUserData (list, key, value) {
  for (let i = 0; i < list.length; i++) {
    if (list[i].key === key) {
      if (value) list[i].value = value
      else list.splice(i, 1)
      return
    }
  }
  if (value) list.push({ key, value })
}

function noop () {}
