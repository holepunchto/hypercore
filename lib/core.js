const hypercoreCrypto = require('hypercore-crypto')
const b4a = require('b4a')
const assert = require('nanoassert')
const unslab = require('unslab')
const Mutex = require('./mutex')
const MerkleTree = require('./merkle-tree')
const BlockStore = require('./block-store')
const BitInterlude = require('./bit-interlude')
const Bitfield = require('./bitfield')
const RemoteBitfield = require('./remote-bitfield')
// const Info = require('./info')
const { BAD_ARGUMENT, STORAGE_EMPTY, STORAGE_CONFLICT, INVALID_OPERATION, INVALID_SIGNATURE, INVALID_CHECKSUM } = require('hypercore-errors')
const Verifier = require('./verifier')
const audit = require('./audit')
const { createTracer } = require('hypertrace')

const HEAD = Symbol.for('head')
const CORE = Symbol.for('core')
const CONTIG = Symbol.for('contig')
const TREE = Symbol.for('tree')
const BITFIELD = Symbol.for('bitfield')
const USER_DATA = Symbol.for('user-data')

class Update {
  constructor (batch, bitfield, header, state) {
    this.batch = batch
    this.bitfield = new BitInterlude(bitfield)

    this.state = state

    this.contiguousLength = header.hints.contiguousLength

    this.tree = null

    this.updates = []
    this._coreUpdates = []
  }

  async flushBitfield () {
    const update = await this.bitfield.flush(this.batch)
    if (update) this.updates.push({ type: BITFIELD, update })
  }

  flushTreeBatch (batch) {
    const update = batch.commit(this.batch)
    this.updates.push({ type: TREE, update })

    if (batch.upgraded) {
      this.tree = {
        fork: batch.fork,
        length: batch.length,
        rootHash: batch.hash(),
        signature: batch.signature
      }
    }
  }

  setUserData (key, value) {
    this.updates.push({ type: USER_DATA, update: { key, value } })
    this.batch.setUserData(key, value)
  }

  coreUpdate (update) {
    let { bitfield, status, value, from } = update

    if (bitfield) {
      const contig = updateContigBatch(this.contiguousLength, bitfield, this.bitfield)

      status |= contig.status

      if (contig.length > this.contiguousLength || (bitfield.drop && contig.length < this.contiguousLength)) {
        this.contiguousLength = contig.length
        this._coreUpdates.push({ type: CONTIG, update: contig.length })
      }
    }

    this._coreUpdates.push({ type: CORE, update: { status, bitfield, value, from } })
  }

  async flush () {
    await this.flushBitfield()

    if (this.tree) {
      this.batch.setCoreHead(this.tree)
      this.updates.push({ type: HEAD, update: this.tree })
    }

    // bitfield flushed before core updates
    for (const upd of this._coreUpdates) {
      this.updates.push(upd)
    }

    await this.batch.flush()

    return this.updates
  }
}

module.exports = class Core {
  constructor (storage, header, compat, crypto, tree, blocks, bitfield, verifier, sessions, legacy, globalCache, onupdate, onconflict) {
    this.storage = storage
    this.tracer = createTracer(this)
    this.onupdate = onupdate
    this.onconflict = onconflict
    this.preupdate = null
    this.header = header
    this.compat = compat
    this.crypto = crypto
    this.tree = tree
    this.blocks = blocks
    this.bitfield = bitfield
    this.verifier = verifier
    this.truncating = 0
    this.updating = false
    this.closed = false
    this.skipBitfield = null
    this.active = sessions.length
    this.sessions = sessions
    this.globalCache = globalCache

    this.state = { storage, tree, bitfield, mutex: new Mutex(), blocks: this.blocks }

    this._manifestFlushed = !!header.manifest
    this._maxOplogSize = 65536
    this._autoFlush = 1
    this._onflush = null
    this._flushing = null
    this._activeBatch = null
    this._bitfield = null
    this._verifies = null
    this._verifiesFlushed = null
    this._legacy = legacy
  }

  async createNamedSession (name, treeLength = this.tree.length) {
    const storage = await this.storage.registerBatch(name, treeLength)
    // const head = await storage.getCoreHead()
    const bitfield = await Bitfield.from(this.bitfield)
    const mutex = new Mutex()

    const tree = await MerkleTree.open(storage, {
      crypto: this.crypto,
      prologue: this.tree.prologue,
      length: treeLength
    })

    return {
      storage,
      tree,
      mutex,
      blocks: this.blocks,
      treeLength,
      bitfield
    }
  }

  static async open (db, opts = {}) {
    const discoveryKey = opts.discoveryKey || (opts.key && hypercoreCrypto.discoveryKey(opts.key))
    const storage = db.get(discoveryKey)
    return await this.resume(storage, opts)
  }

  static async resume (storage, opts) {
    let overwrite = opts.overwrite === true

    const force = opts.force === true
    const createIfMissing = opts.createIfMissing !== false
    const crypto = opts.crypto || hypercoreCrypto
    // kill this flag soon
    const legacy = !!opts.legacy

    // default to true for now if no manifest is provided
    let compat = opts.compat === true || (opts.compat !== false && !opts.manifest)

    let header = parseHeader(await storage.open())

    if (force && opts.key && header && !b4a.equals(header.key, opts.key)) {
      overwrite = true
    }

    if (!header || overwrite) {
      if (!createIfMissing) {
        throw STORAGE_EMPTY('No Hypercore is stored here')
      }

      if (compat) {
        if (opts.key && opts.keyPair && !b4a.equals(opts.key, opts.keyPair.publicKey)) {
          throw BAD_ARGUMENT('Key must match publicKey when in compat mode')
        }
      }

      const keyPair = opts.keyPair || (opts.key ? null : crypto.keyPair())

      const defaultManifest = !opts.manifest && (!!opts.compat || !opts.key || !!(keyPair && b4a.equals(opts.key, keyPair.publicKey)))
      const manifest = defaultManifest ? Verifier.defaultSignerManifest(opts.key || keyPair.publicKey) : Verifier.createManifest(opts.manifest)

      header = {
        key: opts.key || (compat ? manifest.signers[0].publicKey : Verifier.manifestHash(manifest)),
        manifest,
        external: null,
        keyPair,
        userData: [],
        tree: {
          fork: 0,
          length: 0,
          rootHash: null,
          signature: null
        },
        hints: {
          reorgs: [],
          contiguousLength: 0
        }
      }

      const discoveryKey = opts.discoveryKey || hypercoreCrypto.discoveryKey(header.key)

      await storage.create({
        key: header.key,
        manifest: manifest ? Verifier.encodeManifest(manifest) : null,
        keyPair,
        discoveryKey
      })
    }

    // unslab the long lived buffers to avoid keeping the slab alive
    header.key = unslab(header.key)

    if (header.tree) {
      header.tree.rootHash = unslab(header.tree.rootHash)
      header.tree.signature = unslab(header.tree.signature)
    }

    if (opts.manifest) {
      // if we provide a manifest and no key, verify that the stored key is the same
      if (!opts.key && !Verifier.isValidManifest(header.key, Verifier.createManifest(opts.manifest))) {
        throw STORAGE_CONFLICT('Manifest does not hash to provided key')
      }

      if (!header.manifest) header.manifest = opts.manifest
    }

    if (opts.key && !b4a.equals(header.key, opts.key)) {
      throw STORAGE_CONFLICT('Another Hypercore is stored here')
    }

    // if we signalled compat, but already now this core isn't disable it
    if (compat && header.manifest && !Verifier.isCompat(header.key, header.manifest)) {
      compat = false
    } else if (!compat && header.manifest && Verifier.isCompat(header.key, header.manifest)) {
      compat = true
    }

    const prologue = header.manifest ? header.manifest.prologue : null

    const tree = await MerkleTree.open(storage, { crypto, prologue, ...header.tree })
    const bitfield = await Bitfield.open(storage)
    const blocks = new BlockStore(storage)

    if (overwrite) {
      const writer = storage.createWriteBatch()
      tree.clear(writer)
      blocks.clear(writer)
      bitfield.clear(writer)
      await writer.flush()
    }

    // compat from earlier version that do not store contig length
    // if (header.hints.contiguousLength === 0) {
    //   while (bitfield.get(header.hints.contiguousLength)) header.hints.contiguousLength++
    // }

    // to unslab
    if (header.manifest) header.manifest = Verifier.createManifest(header.manifest)

    const verifier = header.manifest ? new Verifier(header.key, header.manifest, { crypto, legacy }) : null

    return new this(storage, header, compat, crypto, tree, blocks, bitfield, verifier, opts.sessions || [], legacy, opts.globalCache || null, opts.onupdate || noop, opts.onconflict || noop)
  }

  async audit (state = this.state) {
    await state.mutex.lock()

    try {
      const update = this._createUpdate(state)
      const corrections = await audit(this, update)
      if (corrections.blocks || corrections.tree) {
        await this._flushUpdate(update)
      }

      return corrections
    } finally {
      this._clearActiveBatch(state)
      await state.mutex.unlock()
    }
  }

  async setManifest (manifest) {
    await this.state.mutex.lock()

    try {
      if (manifest && this.header.manifest === null) {
        if (!Verifier.isValidManifest(this.header.key, manifest)) throw INVALID_CHECKSUM('Manifest hash does not match')

        const update = this._createUpdate(this.state)
        this._setManifest(update, Verifier.createManifest(manifest), null)

        await this._flushUpdate(update)
      }
    } finally {
      this._clearActiveBatch(this.state)
      this.state.mutex.unlock()
    }
  }

  _setManifest (update, manifest, keyPair) {
    if (!manifest && b4a.equals(keyPair.publicKey, this.header.key)) manifest = Verifier.defaultSignerManifest(this.header.key)
    if (!manifest) return

    const verifier = new Verifier(this.header.key, manifest, { crypto: this.crypto, legacy: this._legacy })

    if (verifier.prologue) this.tree.setPrologue(verifier.prologue)

    this.header.manifest = manifest

    update.batch.setCoreAuth({ key: this.header.key, manifest: Verifier.encodeManifest(manifest) })

    this.compat = verifier.compat
    this.verifier = verifier
    this._manifestFlushed = false

    update.coreUpdate({ status: 0b10000, bitfield: null, value: null, from: null })
  }

  async copyPrologue (src, { additional = [] } = {}) {
    await this.state.mutex.lock()

    try {
      await src.mutex.lock()
    } catch (err) {
      this.state.mutex.unlock()
      throw err
    }

    try {
      const update = this._createUpdate(this.state)

      const prologue = this.header.manifest && this.header.manifest.prologue
      if (!prologue) throw INVALID_OPERATION('No prologue present')

      const srcLength = prologue.length - additional.length
      const srcBatch = srcLength !== src.tree.length ? await src.tree.truncate(srcLength) : src.tree.batch()
      const srcNodes = srcBatch.roots.slice(0)

      if (additional.length) {
        for (const blk of additional) srcBatch.append(blk)
      }

      if (!b4a.equals(srcBatch.hash(), prologue.hash)) throw INVALID_OPERATION('Source tree is conflicting')

      // all hashes are correct, lets copy

      let bitfield = null

      const batch = this.tree.batch()

      if (additional.length) {
        this.blocks.putBatch(update.batch, srcLength, additional)

        for (const node of srcBatch.nodes) srcNodes.push(node)

        bitfield = {
          drop: false,
          start: srcLength,
          length: additional.length
        }
      }

      batch.nodes = srcNodes

      if (this.header.tree.length < srcBatch.length) {
        batch.upgraded = true
        batch.length = srcBatch.length
        batch.byteLength = srcBatch.byteLength
        batch.roots = srcBatch.roots

        this.onupdate({ status: 0b0001, bitfield: null, value: null, from: null })
      }

      if (bitfield) {
        update.bitfield.setRange(bitfield.start, bitfield.start + bitfield.length, true)
      }

      // no more additional blocks now and we should be consistent on disk
      // copy over all existing segments...

      let segmentEnd = 0

      while (segmentEnd < srcLength) {
        const segmentStart = maximumSegmentStart(segmentEnd, src.bitfield, this.bitfield)
        if (segmentStart >= srcLength || segmentStart < 0) break

        // max segment is 65536 to avoid running out of memory
        segmentEnd = Math.min(segmentStart + 65536, srcLength, minimumSegmentEnd(segmentStart, src.bitfield, this.bitfield))

        const treeNodes = await src.tree.getNeededNodes(srcLength, segmentStart, segmentEnd)
        const bitfield = {
          drop: false,
          start: segmentStart,
          length: segmentEnd - segmentStart
        }

        const blocks = []

        const reader = src.storage.createReadBatch()
        for (let i = segmentStart; i < segmentEnd; i++) {
          blocks.push(src.blocks.get(reader, i))
        }
        reader.tryFlush()

        const segment = await Promise.all(blocks)

        this.blocks.putBatch(update.batch, segmentStart, segment)

        batch.addNodesUnsafe(treeNodes)

        update.bitfield.setRange(bitfield.start, segmentEnd, true)
        update.coreUpdate({ status: 0, bitfield, value: null, from: null })
      }

      update.flushTreeBatch(batch)

      for await (const { key, value } of src.storage.createUserDataStream()) {
        this.userData(update.batch, key, value)
      }

      await this._flushUpdate(update)
    } finally {
      this._clearActiveBatch(this.state)
      src.mutex.unlock()
      this.state.mutex.unlock()
    }
  }

  // async flush () {
  //   await this.state.mutex.lock()
  //   try {
  //     this._manifestFlushed = true
  //     this._autoFlush = 4
  //     await this._flushBitfield(writer)
  //   } finally {
  //     this.state.mutex.unlock()
  //   }
  // }

  get isFlushing () {
    return !!(this._flushing || this.state._activeBatch)
  }

  _clearActiveBatch (state, err) {
    if (!state._activeBatch) return

    if (this._onflush) this._onflush(err)

    this._onflush = null
    this._flushing = null

    state._activeStack = null
    state._activeBatch = null
  }

  _createUpdate (state) {
    assert(!state._activeBatch)

    state._activeBatch = state.storage.createWriteBatch()
    return new Update(state._activeBatch, state.bitfield, this.header, state, this.header.key[0] === 0x44)
  }

  async _flushUpdate (u) {
    const flushing = this._flushUpdateBatch(u)

    try {
      if (!this._flushing) this._flushing = flushing

      await flushing
    } finally {
      this._clearActiveBatch(u.state)
    }
  }

  flushed () {
    if (!this.state._activeBatch) return

    if (this._flushing) return this._flushing

    this._flushing = new Promise(resolve => {
      this._onflush = resolve
    })

    return this._flushing
  }

  _writeBlock (writer, index, value) {
    this.blocks.put(writer, index, value)
  }

  userData (update, key, value) {
    return update.setUserData(key, value)
  }

  async setUserData (state, key, value) {
    await state.mutex.lock()

    try {
      const update = this._createUpdate(state)
      this.userData(update, key, value)
      return await this._flushUpdate(update)
    } finally {
      this._clearActiveBatch(state)
      state.mutex.unlock()
    }
  }

  async truncate (state, length, fork, { signature, keyPair = this.header.keyPair } = {}) {
    if (this.tree.prologue && length < this.tree.prologue.length) {
      throw INVALID_OPERATION('Truncation breaks prologue')
    }

    const isDefault = state.storage === this.storage

    this.truncating++
    await state.mutex.lock()

    // upsert compat manifest
    if (this.verifier === null && keyPair) this._setManifest(null, keyPair)

    try {
      const batch = await state.tree.truncate(length, fork)
      if (isDefault && length > 0) batch.signature = signature || this.verifier.sign(batch, keyPair)

      const update = this._createUpdate(state)
      await this._truncate(update, batch, null)
      await this._flushUpdate(update)
    } finally {
      this.truncating--
      this._clearActiveBatch(state)
      state.mutex.unlock()
    }
  }

  async clear (state, start, end, cleared) {
    await state.mutex.lock()

    try {
      const bitfield = {
        start,
        length: end - start,
        drop: true
      }

      const update = this._createUpdate(state)

      update.bitfield.setRange(start, end, false)

      start = state.bitfield.firstSet(start + 1)

      // TODO: verify this:
      // start = state.bitfield.lastSet(start) + 1
      // end = state.bitfield.firstSet(end)

      if (end === -1) end = state.tree.length
      if (start === -1 || start >= state.tree.length) return

      this.blocks.clear(update.batch, start, end - start)

      update.coreUpdate({ status: 0, bitfield, value: null, from: null })

      await this._flushUpdate(update)
    } finally {
      this._clearActiveBatch(state)
      state.mutex.unlock()
    }
  }

  // async purge () {
  //   return new Promise((resolve, reject) => {
  //     let missing = 4
  //     let error = null

  //     this.oplog.storage.unlink(done)
  //     this.tree.storage.unlink(done)
  //     this.bitfield.storage.unlink(done)
  //     this.blocks.storage.unlink(done)

  //     function done (err) {
  //       if (err) error = err
  //       if (--missing) return
  //       if (error) reject(error)
  //       else resolve()
  //     }
  //   })
  // }

  async commit (state, { signature, keyPair = this.header.keyPair, length = state.tree.length, treeLength = state.treeLength } = {}) {
    await this.state.mutex.lock()

    const update = this._createUpdate(this.state)

    try {
      // upsert compat manifest
      if (this.verifier === null && keyPair) this._setManifest(update, null, keyPair)

      if (this.tree.fork !== state.tree.fork) return null

      if (this.tree.length > state.tree.length) return null // TODO: partial commit in the future if possible

      if (this.tree.length > treeLength) {
        for (const root of this.tree.roots) {
          const batchRoot = await state.tree.get(root.index)
          if (batchRoot.size !== root.size || !b4a.equals(batchRoot.hash, root.hash)) {
            return null
          }
        }
      }

      const adding = length - treeLength

      const batch = await this.tree.reconcile(state.tree, length, treeLength)

      if (batch.upgraded) batch.signature = signature || this.verifier.sign(batch, keyPair)

      const treeUpgrade = batch.upgraded ? batch : null

      const bitfield = {
        drop: false,
        start: treeLength,
        length: adding
      }

      const promises = []
      const reader = state.storage.createReadBatch()

      for (let i = 0; i < adding; i++) {
        promises.push(reader.getBlock(treeLength + i))
      }

      reader.tryFlush()

      const values = await Promise.all(promises)

      this.blocks.putBatch(update.batch, treeLength, values)

      update.bitfield.setRange(bitfield.start, bitfield.start + bitfield.length, true)
      update.flushTreeBatch(batch)

      // TODO: do we need this below?
      // we already commit this, and now we signed it, so tell others
      if (treeUpgrade && treeLength > batch.treeLength) {
        bitfield.start = batch.treeLength
        bitfield.length = treeLength - batch.treeLength
      }

      await this._flushUpdate(update)

      state.treeLength = batch.length

      return { length: batch.length, byteLength: batch.byteLength }
    } finally {
      this._clearActiveBatch(this.state)
      this.state.mutex.unlock()
    }
  }

  async append (state, values, { signature, keyPair = this.header.keyPair, preappend } = {}) {
    await state.mutex.lock()

    try {
      const update = this._createUpdate(state)

      // upsert compat manifest
      if (this.verifier === null && keyPair) this._setManifest(update, null, keyPair)

      if (preappend) await preappend(values)

      if (!values.length) {
        await this._flushUpdate(update)
        return { length: state.tree.length, byteLength: state.tree.byteLength }
      }

      const batch = state.tree.batch()
      for (const val of values) batch.append(val)

      // only multisig can have prologue so signature is always present
      if (state.tree.prologue && batch.length < state.tree.prologue.length) {
        throw INVALID_OPERATION('Append is not consistent with prologue')
      }

      const isDefault = state.storage === this.storage
      batch.signature = isDefault ? (signature || this.verifier.sign(batch, keyPair)) : null

      update.flushTreeBatch(batch)
      update.bitfield.setRange(batch.ancestors, batch.length, true)

      this.blocks.putBatch(update.batch, state.tree.length, values)

      const bitfield = {
        drop: false,
        start: batch.ancestors,
        length: values.length
      }

      update.coreUpdate({
        bitfield,
        status: 0b0001,
        value: null,
        from: null
      })

      await this._flushUpdate(update)

      return { length: batch.length, byteLength: batch.byteLength }
    } finally {
      this._clearActiveBatch(state)
      state.mutex.unlock()
    }
  }

  _verifyBatchUpgrade (update, batch, manifest) {
    if (!this.header.manifest) {
      if (!manifest && this.compat) manifest = Verifier.defaultSignerManifest(this.header.key)

      if (!manifest || !(Verifier.isValidManifest(this.header.key, manifest) || (this.compat && Verifier.isCompat(this.header.key, manifest)))) {
        throw INVALID_SIGNATURE('Proof contains an invalid manifest') // TODO: proper error type
      }
    }

    manifest = Verifier.createManifest(manifest) // To unslab

    const verifier = this.verifier || new Verifier(this.header.key, manifest, { crypto: this.crypto, legacy: this._legacy })

    if (!verifier.verify(batch, batch.signature)) {
      throw INVALID_SIGNATURE('Proof contains an invalid signature')
    }

    if (!this.header.manifest && update !== null) this._setManifest(update, manifest, null)
  }

  async _verifyExclusive ({ batch, bitfield, value, manifest, from }) {
    await this.state.mutex.lock()

    const update = this._createUpdate(this.state)

    try {
      this._verifyBatchUpgrade(update, batch, manifest)

      if (!batch.commitable()) return false
      this.updating = true

      if (this.preupdate !== null) await this.preupdate(batch, this.header.key)
      if (bitfield) this._writeBlock(update.batch, bitfield.start, value)

      if (bitfield) {
        update.bitfield.setRange(bitfield.start, bitfield.start + 1, true)
      }

      update.coreUpdate({ status: 0b0001, bitfield, value, from })
      update.flushTreeBatch(batch)

      await this._flushUpdate(update)
    } finally {
      this._clearActiveBatch(this.state)
      this.updating = false
      this.state.mutex.unlock()
    }

    return true
  }

  async _verifyShared () {
    if (!this._verifies.length) return false

    await this.state.mutex.lock()

    const update = this._createUpdate(this.state)

    const verifies = this._verifies
    this._verifies = null
    this._verified = null

    try {
      for (const { batch, bitfield, value } of verifies) {
        if (!batch.commitable()) continue

        if (bitfield) {
          this._writeBlock(update.batch, bitfield.start, value)
        }
      }

      for (let i = 0; i < verifies.length; i++) {
        const { batch, bitfield, value, manifest, from } = verifies[i]

        if (!batch.commitable()) {
          verifies[i] = null // signal that we cannot commit this one
          continue
        }

        if (bitfield) {
          update.bitfield.setRange(bitfield.start, bitfield.start + 1, true)
        }

        // if we got a manifest AND its strictly a non compat one, lets store it
        if (manifest && this.header.manifest === null) {
          if (!Verifier.isValidManifest(this.header.key, manifest)) throw INVALID_CHECKSUM('Manifest hash does not match')
          this._setManifest(update, manifest, null)
        }

        update.coreUpdate({ status: 0, bitfield, value, from })
        update.flushTreeBatch(batch)
      }

      await this._flushUpdate(update)
    } finally {
      this._clearActiveBatch(this.state)
      this.state.mutex.unlock()
    }

    return verifies[0] !== null
  }

  async checkConflict (proof, from) {
    if (this.tree.length < proof.upgrade.length || proof.fork !== this.tree.fork) {
      // out of date this proof - ignore for now
      return false
    }

    const batch = this.tree.verifyFullyRemote(proof)

    await this.state.mutex.lock()

    try {
      const update = this._createUpdate(this.state)
      this._verifyBatchUpgrade(update, batch, proof.manifest)

      await this._flushUpdate(update)
    } catch {
      this._clearActiveBatch(this.state)
      this.state.mutex.unlock()
      return true
    }

    const remoteTreeHash = this.crypto.tree(proof.upgrade.nodes)
    const localTreeHash = this.crypto.tree(await this.tree.getRoots(proof.upgrade.length))

    if (b4a.equals(localTreeHash, remoteTreeHash)) return false

    await this.onconflict(proof)
    return true
  }

  async verifyReorg (proof) {
    const batch = await this.tree.reorg(proof)
    const update = this._createUpdate(this.state)

    this._verifyBatchUpgrade(update, batch, proof.manifest)

    await this._flushUpdate(update)

    return batch
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
      value,
      status: 0,
      manifest: proof.manifest,
      from
    }

    if (batch.upgraded) {
      return this._verifyExclusive(op)
    }

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
    await this.state.mutex.lock()

    try {
      if (!batch.commitable()) return false

      const update = this._createUpdate(this.state)
      await this._truncate(update, batch, from)
      await this._flushUpdate(update)
    } finally {
      this._clearActiveBatch(this.state)
      this.truncating--
      this.state.mutex.unlock()
    }

    return true
  }

  async _truncate (update, batch, from) {
    const bitfield = {
      drop: true,
      start: batch.ancestors,
      length: batch.treeLength - batch.ancestors
    }

    update.bitfield.setRange(batch.ancestors, batch.treeLength, false)

    const status = (batch.length > batch.ancestors) ? 0b0011 : 0b0010

    update.flushTreeBatch(batch)
    update.coreUpdate({ status, bitfield, value: null, from })
  }

  openSkipBitfield () {
    if (this.skipBitfield !== null) return this.skipBitfield
    this.skipBitfield = new RemoteBitfield()
    const buf = this.bitfield.toBuffer(this.tree.length)
    const bitfield = new Uint32Array(buf.buffer, buf.byteOffset, buf.byteLength / 4)
    this.skipBitfield.insert(0, bitfield)
    return this.skipBitfield
  }

  async _flushUpdateBatch (u) {
    await u.flush()

    if (!u.updates.length) return

    const isDefault = u.state === this.state

    for (const { type, update } of u.updates) {
      switch (type) {
        case HEAD: {
          if (isDefault) this.header.tree = update
          break
        }

        case CORE: { // core
          if (isDefault) this.onupdate(update)
          break
        }

        case CONTIG: { // contig
          if (isDefault) this.header.hints.contiguousLength = update
          break
        }

        case TREE: // tree
          if (isDefault && update.truncated) addReorgHint(this.header.hints.reorgs, this.tree, update)
          u.state.tree.onupdate(update)
          break

        case BITFIELD: // bitfield
          u.state.bitfield.onupdate(update)
          if (isDefault && this.skipBitfield !== null) this._updateSkipBitfield(update)
          break

        case USER_DATA: { // user data
          if (!isDefault) continue

          let exists = false
          for (const entry of this.header.userData) {
            if (entry.key !== update.key) continue

            entry.value = update.value
            exists = true
            break
          }

          if (exists) continue

          this.header.userData.push({ key: update.key, value: update.value })
          break
        }
      }
    }
  }

  _updateSkipBitfield ({ ranges, drop }) {
    for (const { start, end } of ranges) {
      this.skipBitfield.setRange(start, end - start, drop === false)
    }
  }

  async close () {
    this.closed = true
    await this.state.mutex.destroy()
    await this.storage.close() // TODO: add option where the storage is NOT closed for corestore
  }
}

function updateContigBatch (start, upd, bitfield) {
  const end = upd.start + upd.length

  let c = start

  if (upd.drop) {
    // If we dropped a block in the current contig range, "downgrade" it
    if (c > upd.start) {
      c = upd.start
    }
  } else {
    if (c <= end && c >= upd.start) {
      c = end
      while (bitfield.get(c)) c++
    }
  }

  if (c === start) {
    return {
      status: 0b0000,
      length: null
    }
  }

  if (c > start) {
    return {
      status: 0b0100,
      length: c
    }
  }

  return {
    status: 0b1000,
    length: c
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

function parseHeader (info) {
  if (!info) return null

  return {
    key: info.auth.key,
    manifest: info.auth.manifest ? Verifier.decodeManifest(info.auth.manifest) : null,
    external: null,
    keyPair: info.keyPair,
    userData: [],
    tree: info.head,
    hints: {
      reorgs: [],
      contiguousLength: 0
    }
  }
}

function noop () {}

function maximumSegmentStart (start, src, dst) {
  while (true) {
    const a = src.firstSet(start)
    const b = dst.firstUnset(start)

    if (a === -1) return -1
    if (b === -1) return a

    // if dst has the segment, restart
    if (a < b) {
      start = b
      continue
    }

    return a
  }
}

function minimumSegmentEnd (start, src, dst) {
  const a = src.firstUnset(start)
  const b = dst.firstSet(start)

  if (a === -1) return -1
  if (b === -1) return a
  return a < b ? a : b
}
