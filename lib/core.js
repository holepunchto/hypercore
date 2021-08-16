const hypercoreCrypto = require('hypercore-crypto')
const Oplog = require('./oplog')
const Mutex = require('./mutex')
const MerkleTree = require('./merkle-tree')
const BlockStore = require('./block-store')
const Bitfield = require('./bitfield')
const { oplogHeader, oplogEntry } = require('./messages')

module.exports = class Core {
  constructor (header, crypto, oplog, tree, blocks, bitfield, sign, onupdate) {
    this.onupdate = onupdate
    this.header = header
    this.crypto = crypto
    this.oplog = oplog
    this.tree = tree
    this.blocks = blocks
    this.bitfield = bitfield
    this.sign = sign

    this._maxOplogSize = 65536
    this._autoFlush = 1
    this._verifies = null
    this._verifiesFlushed = null
    this._mutex = new Mutex()
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

  static async resume (oplogFile, treeFile, bitfieldFile, dataFile, opts) {
    const overwrite = opts.overwrite === true
    const createIfMissing = opts.createIfMissing !== false
    const crypto = opts.crypto || hypercoreCrypto

    const oplog = new Oplog(oplogFile, {
      headerEncoding: oplogHeader,
      entryEncoding: oplogEntry
    })

    let { header, entries } = await oplog.open()

    if (!header || overwrite === true) {
      if (!createIfMissing) {
        throw new Error('No hypercore is stored here')
      }

      const keyPair = opts.keyPair || { type: 'Ed25519', ...crypto.keyPair() }

      header = {
        userData: [],
        keyPair,
        tree: {
          type: 'BLAKE2b',
          fork: 0,
          length: 0,
          signature: null
        },
        reorgs: [],
        bitfield: {
          type: 'raw'
        }
      }

      await oplog.flush(header)
    }

    if (opts.keyPair && !header.keyPair.publicKey.equals(opts.keyPair.publicKey)) {
      throw new Error('Another hypercore is stored here')
    }

    const tree = await MerkleTree.open(treeFile, { crypto, ...header.tree })
    const bitfield = await Bitfield.open(bitfieldFile)
    const blocks = new BlockStore(dataFile, tree)

    if (overwrite) {
      await tree.clear()
      await blocks.clear()
      await bitfield.clear()
    }

    const sign = opts.sign || (header.keyPair.secretKey ? signable => crypto.sign(signable, header.keyPair.secretKey) : null)

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
        for (let i = 0; i < e.bitfield.length; i++) {
          const idx = e.bitfield.start + i
          bitfield.set(idx, !e.bitfield.drop)
        }
      }

      if (e.treeUpgrade) {
        const batch = await tree.truncate(e.treeUpgrade.length, e.treeUpgrade.fork)
        batch.ancestors = e.treeUpgrade.ancestors
        batch.signature = e.treeUpgrade.signature
        addReorgMaybe(header.reorgs, tree, batch)
        batch.commit()

        header.tree.length = tree.length
        header.tree.fork = tree.fork
        header.tree.signature = tree.signature
      }
    }

    return new this(header, crypto, oplog, tree, blocks, bitfield, sign, opts.onupdate || noop)
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

  async truncate (length, fork) {
    await this._mutex.lock()

    try {
      const batch = await this.tree.truncate(length, fork)
      batch.signature = await this.sign(batch.signable())
      await this._truncate(batch, null)
    } finally {
      this._mutex.unlock()
    }
  }

  async append (values) {
    await this._mutex.lock()

    try {
      if (!values.length) return this.tree.length

      const batch = this.tree.batch()
      for (const val of values) batch.append(val)
      batch.signature = await this.sign(batch.signable())

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

      for (let i = batch.ancestors; i < batch.length; i++) this.bitfield.set(i, true)
      batch.commit()

      this.header.tree.length = batch.length
      this.header.tree.signature = batch.signature
      this.onupdate(0b01, entry.bitfield, null, null)

      if (this._shouldFlush()) await this._flushOplog()

      return batch.ancestors
    } finally {
      this._mutex.unlock()
    }
  }

  async _verifyExclusive ({ batch, bitfield, value, from }) {
    if (!batch.signature || !this.crypto.verify(batch.signable(), batch.signature, this.header.keyPair.publicKey)) {
      throw new Error('Remote signature does not match')
    }

    await this._mutex.lock()

    try {
      if (!batch.commitable()) return

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
      this.header.tree.signature = batch.signature
      this.onupdate(0b01, bitfield, value, from)

      if (this._shouldFlush()) await this._flushOplog()
    } finally {
      this._mutex.unlock()
    }
  }

  async _verifyShared () {
    await this._mutex.lock()

    try {
      const entries = []

      for (const { batch, bitfield, value } of this._verifies) {
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

      for (const { batch, bitfield, value, from } of this._verifies) {
        if (!batch.commitable()) continue
        if (bitfield) this.bitfield.set(bitfield.start, true)
        batch.commit()
        this.onupdate(0, bitfield, value, from)
      }

      if (this._shouldFlush()) await this._flushOplog()
    } finally {
      this._verifies = null
      this._verified = null
      this._mutex.unlock()
    }
  }

  async verify (proof, from) {
    const batch = await this.tree.verify(proof)

    if (!batch.commitable()) return

    const value = (proof.block && proof.block.value) || null
    const op = {
      batch,
      bitfield: value && { drop: false, start: proof.block.index, length: 1 },
      value: value,
      from
    }

    if (batch.upgraded) return this._verifyExclusive(op)

    if (this._verifies !== null) {
      this._verifies.push(op)
      return this._verified
    }

    this._verifies = [op]
    this._verified = this._verifyShared()

    return this._verified
  }

  async reorg (batch, from) {
    if (!batch.commitable()) return false

    await this._mutex.lock()

    try {
      if (!batch.commitable()) return false
      await this._truncate(batch, from)
      return true
    } finally {
      this._mutex.unlock()
    }
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

    for (let i = batch.ancestors; i < this.tree.length; i++) this.bitfield.set(i, false)
    addReorgMaybe(this.header.reorgs, this.tree, batch)
    batch.commit()

    const appended = batch.length > batch.ancestors

    this.header.tree.fork = batch.fork
    this.header.tree.length = batch.length
    this.header.tree.signature = batch.signature
    this.onupdate(appended ? 0b11 : 0b10, entry.bitfield, null, from)

    // TODO: there is a bug in the merkle tree atm where it cannot handle unflushed
    // truncates if we append or download anything after the truncation point later on
    // This is because tree.get checks the truncated flag. We should fix this so we can do
    // the later flush here as well
    // if (this._shouldFlush()) await this._flushOplog()
    await this._flushOplog()
  }
}

function addReorgMaybe (list, tree, batch) {
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
