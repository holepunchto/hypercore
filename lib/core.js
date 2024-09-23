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
    // TODO: investigate when tree is not commitable
    if (batch.commitable()) {
      const update = batch.commit(this.batch)
      this.updates.push({ type: TREE, update })
    }

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

  async truncate (batch, from) {
    const bitfield = {
      drop: true,
      start: batch.ancestors,
      length: batch.treeLength - batch.ancestors
    }

    this.bitfield.setRange(batch.ancestors, batch.treeLength, false)
    this.batch.deleteBlockRange(bitfield.start, bitfield.start + bitfield.length)

    const status = (batch.length > batch.ancestors) ? 0b0011 : 0b0010

    this.flushTreeBatch(batch)
    this.coreUpdate({ status, bitfield, value: null, from })
  }
}

class SessionState {
  constructor (core, storage, blocks, tree, bitfield, treeLength, snapshot) {
    this.core = core

    this.storage = storage
    this.storageSnapshot = snapshot || null

    this.mutex = new Mutex()

    this.blocks = blocks
    this.tree = tree
    this.bitfield = bitfield

    this.treeLength = treeLength

    this.active = 0

    this._onflush = null
    this._flushing = null
    this._activeBatch = null

    this.ref()
  }

  get isSnapshot () {
    return this.storageSnapshot !== null
  }

  get isDefault () {
    return this.core.state === this
  }

  async unref () {
    if (--this.active > 0) return Promise.resolve()

    await this.close()
  }

  ref () {
    this.active++
    return this
  }

  async close () {
    if (this.storageSnapshot) this.storageSnapshot.destroy()

    await this.storage.close()
    await this.mutex.destroy(new Error('Closed'))
  }

  snapshot () {
    const snapshot = this.storage.snapshot()

    const s = new SessionState(
      this.core,
      this.storage,
      this.blocks,
      this.tree,
      this.bitfield,
      this.treeLength,
      snapshot
    )

    return s
  }

  createReadBatch () {
    return this.storage.createReadBatch({ snapshot: this.storageSnapshot })
  }

  _clearActiveBatch (err) {
    if (!this._activeBatch) return
    this._activeBatch.destroy()

    if (this._onflush) this._onflush(err)

    this._onflush = null
    this._flushing = null

    this._activeBatch = null
  }

  createUpdate () {
    assert(!this._activeBatch && !this.isSnapshot)

    this._activeBatch = this.storage.createWriteBatch()
    return new Update(this._activeBatch, this.bitfield, this.core.header, this)
  }

  async flushUpdate (u) {
    const flushing = this._flushUpdateBatch(u)

    try {
      if (!this._flushing) this._flushing = flushing

      await flushing
    } finally {
      this._clearActiveBatch(this)
    }
  }

  flushed () {
    if (!this._activeBatch) return

    if (this._flushing) return this._flushing

    this._flushing = new Promise(resolve => {
      this._onflush = resolve
    })

    return this._flushing
  }

  async _flushUpdateBatch (u) {
    await u.flush()

    if (!u.updates.length) return

    for (const { type, update } of u.updates) {
      switch (type) {
        case TREE: // tree
          if (!this.isDefault) this.tree.onupdate(update)
          break

        case BITFIELD: // bitfield
          this.bitfield.onupdate(update)
          break
      }
    }

    if (!this.isDefault) return

    this.core._processUpdates(u.updates)
  }

  async setUserData (key, value) {
    await this.mutex.lock()

    try {
      const update = this.createUpdate()
      update.setUserData(key, value)

      return await this.flushUpdate(update)
    } finally {
      this._clearActiveBatch()
      this.mutex.unlock()
    }
  }

  async truncate (length, fork, { signature, keyPair } = {}) {
    if (this.tree.prologue && length < this.tree.prologue.length) {
      throw INVALID_OPERATION('Truncation breaks prologue')
    }

    if (!keyPair && this.isDefault) keyPair = this.core.header.keyPair

    await this.mutex.lock()

    try {
      const batch = await this.tree.truncate(length, fork)

      if (!signature && keyPair && length > 0) signature = this.core.verifier.sign(batch, keyPair)
      if (signature) batch.signature = signature

      const update = this.createUpdate()

      // upsert compat manifest
      if (this.core.verifier === null && keyPair) this.core._setManifest(update, null, keyPair)

      await update.truncate(batch, null)

      if (batch.length < this.treeLength) this.treeLength = batch.length

      await this.flushUpdate(update)
    } finally {
      this._clearActiveBatch()
      this.mutex.unlock()
    }
  }

  async clear (start, end, cleared) {
    await this.mutex.lock()

    try {
      const bitfield = {
        start,
        length: end - start,
        drop: true
      }

      const update = this.createUpdate()

      update.bitfield.setRange(start, end, false)

      end = this.bitfield.firstSet(end)

      // TODO: verify this:
      // start = state.bitfield.lastSet(start) + 1
      // end = state.bitfield.firstSet(end)

      if (end === -1) end = this.tree.length
      if (start === -1 || start >= this.tree.length) return

      this.blocks.clear(update.batch, start, end - start)
      update.coreUpdate({ status: 0, bitfield, value: null, from: null })

      if (start < this.treeLength) this.treeLength = start

      await this.flushUpdate(update)
    } finally {
      this._clearActiveBatch()
      this.mutex.unlock()
    }
  }

  async append (values, { signature, keyPair, preappend } = {}) {
    if (!keyPair && this.isDefault) keyPair = this.core.header.keyPair

    await this.mutex.lock()

    try {
      const update = this.createUpdate()

      // upsert compat manifest
      if (this.core.verifier === null && keyPair) this.core._setManifest(update, null, keyPair)

      if (preappend) await preappend(values)

      if (!values.length) {
        await this.flushUpdate(update)
        return { length: this.tree.length, byteLength: this.tree.byteLength }
      }

      const batch = this.tree.batch()
      for (const val of values) batch.append(val)

      // only multisig can have prologue so signature is always present
      if (this.tree.prologue && batch.length < this.tree.prologue.length) {
        throw INVALID_OPERATION('Append is not consistent with prologue')
      }

      if (!signature && keyPair) signature = this.core.verifier.sign(batch, keyPair)
      if (signature) batch.signature = signature

      update.flushTreeBatch(batch)
      update.bitfield.setRange(batch.ancestors, batch.length, true)

      this.blocks.putBatch(update.batch, this.tree.length, values)

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

      await this.flushUpdate(update)

      return { length: batch.length, byteLength: batch.byteLength }
    } finally {
      this._clearActiveBatch()
      this.mutex.unlock()
    }
  }

  async upgrade (start, end, batch, values, keyPair) {
    await this.mutex.lock()

    const update = this.createUpdate()

    try {
      // upsert compat manifest
      if (this.core.verifier === null && keyPair) this.core._setManifest(update, null, keyPair)

      this.blocks.putBatch(update.batch, start, values)

      update.bitfield.setRange(start, end, true)
      update.flushTreeBatch(batch)

      await this.flushUpdate(update)
    } finally {
      this._clearActiveBatch()
      this.mutex.unlock()
    }
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
    this.sessions = sessions
    this.globalCache = globalCache

    this.state = new SessionState(this, storage, this.blocks, tree, bitfield, tree.length, null)

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

  async createSession (name, length, overwrite) {
    const treeLength = length === undefined ? this.tree.length : length

    const storage = await this.storage.registerBatch(name, treeLength, overwrite)
    const treeInfo = await storage.getCoreHead()
    const bitfield = await Bitfield.open(storage)

    bitfield.merge(this.bitfield, treeLength)

    const tree = await MerkleTree.open(storage, {
      crypto: this.crypto,
      prologue: this.tree.prologue,
      length: (length === treeLength || !treeInfo) ? treeLength : treeInfo.length
    })

    return new SessionState(this, storage, this.blocks, tree, bitfield, treeLength, null)
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
        keyPair: keyPair ? { publicKey: keyPair.publicKey, secretKey: keyPair.secretKey || null } : null,
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

    if (header.keyPair) {
      header.keyPair.publicKey = unslab(header.keyPair.publicKey)
      header.keyPair.secretKey = unslab(header.keyPair.secretKey)
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

    for await (const { key, value } of storage.createUserDataStream()) {
      header.userData.push({ key, value: unslab(value) })
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
      const update = state.createUpdate()
      const corrections = await audit(this, update)
      if (corrections.blocks || corrections.tree) {
        await state.flushUpdate(update)
      }

      return corrections
    } finally {
      state._clearActiveBatch()
      await state.mutex.unlock()
    }
  }

  async setManifest (manifest) {
    await this.state.mutex.lock()

    try {
      if (manifest && this.header.manifest === null) {
        if (!Verifier.isValidManifest(this.header.key, manifest)) throw INVALID_CHECKSUM('Manifest hash does not match')

        const update = this.state.createUpdate()
        this._setManifest(update, Verifier.createManifest(manifest), null)

        await this.state.flushUpdate(update)
      }
    } finally {
      this.state._clearActiveBatch()
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
      const update = this.state.createUpdate()

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
        this.setUserData(update.batch, key, value)
      }

      await this.state.flushUpdate(update)
    } finally {
      this.state._clearActiveBatch()
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

  flushed () {
    return this.state.flushed()
  }

  async _processUpdates (updates) {
    for (const { type, update } of updates) {
      switch (type) {
        case HEAD: {
          this.header.tree = update
          break
        }

        case CORE: { // core
          this.onupdate(update)
          break
        }

        case CONTIG: { // contig
          this.header.hints.contiguousLength = update
          break
        }

        case TREE: // tree
          if (update.truncated) addReorgHint(this.header.hints.reorgs, this.tree, update)
          this.tree.onupdate(update)
          break

        case BITFIELD: // bitfield
          if (this.skipBitfield !== null) this._updateSkipBitfield(update)
          break

        case USER_DATA: { // user data
          let exists = false
          for (const entry of this.header.userData) {
            if (entry.key !== update.key) continue

            entry.value = update.value
            exists = true
            break
          }

          if (exists) continue

          this.header.userData.push({ key: update.key, value: unslab(update.value) })
          break
        }
      }
    }
  }

  _writeBlock (writer, index, value) {
    this.blocks.put(writer, index, value)
  }

  userData (key, value) {
    const update = this.state.createUpdate()
    this.setUserData(update, key, value)

    return this.state.flushUpdate(update)
  }

  setUserData (update, key, value) {
    return update.setUserData(key, value)
  }

  async commit (state, { signature, keyPair = this.header.keyPair, length = state.tree.length, treeLength = state.treeLength } = {}) {
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

    const promises = []

    const reader = state.storage.createReadBatch()
    for (let i = treeLength; i < length; i++) promises.push(reader.getBlock(i))
    reader.tryFlush()

    const values = await Promise.all(promises)

    const batch = await this.tree.reconcile(state.tree, length, treeLength)
    if (batch.upgraded) batch.signature = signature || this.verifier.sign(batch, keyPair)

    await this.state.upgrade(treeLength, length, batch, values, keyPair)

    state.treeLength = batch.length

    return {
      length: batch.length,
      byteLength: batch.byteLength
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

    const update = this.state.createUpdate()

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

      await this.state.flushUpdate(update)
    } finally {
      this.state._clearActiveBatch()
      this.updating = false
      this.state.mutex.unlock()
    }

    return true
  }

  async _verifyShared () {
    if (!this._verifies.length) return false

    await this.state.mutex.lock()

    const update = this.state.createUpdate()

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

      await this.state.flushUpdate(update)
    } finally {
      this.state._clearActiveBatch()
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
      const update = this.state.createUpdate()
      this._verifyBatchUpgrade(update, batch, proof.manifest)

      await this.state.flushUpdate(update)
    } catch {
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
    this._verifyBatchUpgrade(null, batch, proof.manifest)
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

      const update = this.state.createUpdate()
      await update.truncate(batch, from)

      await this.state.flushUpdate(update)
    } finally {
      this.state._clearActiveBatch()
      this.truncating--
      this.state.mutex.unlock()
    }

    return true
  }

  openSkipBitfield () {
    if (this.skipBitfield !== null) return this.skipBitfield
    this.skipBitfield = new RemoteBitfield()
    const buf = this.bitfield.toBuffer(this.tree.length)
    const bitfield = new Uint32Array(buf.buffer, buf.byteOffset, buf.byteLength / 4)
    this.skipBitfield.insert(0, bitfield)
    return this.skipBitfield
  }

  _updateSkipBitfield ({ ranges, drop }) {
    for (const { start, end } of ranges) {
      this.skipBitfield.setRange(start, end - start, drop === false)
    }
  }

  async close () {
    this.closed = true
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
