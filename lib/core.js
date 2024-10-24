const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const assert = require('nanoassert')
const unslab = require('unslab')
const z32 = require('z32')
const MemoryOverlay = require('./memory-overlay')
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
const copyPrologue = require('./copy-prologue')
const BlockEncryption = require('./block-encryption')

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
  constructor (core, storage, blocks, tree, bitfield, treeLength, name) {
    this.core = core

    this.storage = storage
    this.name = name

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
    return this.storage.snapshotted
  }

  get isDefault () {
    return this.core.state === this
  }

  async unref () {
    if (--this.active > 0) return
    await this.close()
  }

  ref () {
    this.active++
    return this
  }

  async close () {
    await this.storage.close()
    await this.mutex.destroy(new Error('Closed'))
  }

  snapshot () {
    const s = new SessionState(
      this.core,
      this.storage.snapshot(),
      this.blocks,
      this.tree, // todo: should clone also but too many changes atm
      this.bitfield,
      this.treeLength,
      this.name
    )

    return s
  }

  memoryOverlay () {
    const storage = new MemoryOverlay(this.storage)
    const s = new SessionState(
      this.core,
      storage,
      this.blocks,
      this.tree.clone(storage),
      this.bitfield,
      this.treeLength,
      this.name
    )

    return s
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
    assert(!this._activeBatch && !this.storage.snapshotted)

    this._activeBatch = this.overlay ? this.overlay.createWriteBatch() : this.storage.createWriteBatch()
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
}

module.exports = class Core {
  constructor (db, opts = {}) {
    this.db = db
    this.storage = null
    this.replicator = null

    this.id = opts.key ? z32.encode(opts.key) : null
    this.key = opts.key || null
    this.discoveryKey = opts.discoveryKey || (opts.key && crypto.discoveryKey(opts.key)) || null
    this.manifest = null

    this.preupdate = null
    this.header = null
    this.compat = false
    this.tree = null
    this.blocks = null
    this.bitfield = null
    this.verifier = null
    this.truncating = 0
    this.updating = false
    this.closed = false
    this.skipBitfield = null
    this.sessions = []
    this.globalCache = opts.globalCache || null
    this.encryption = null

    this.state = null
    this.opened = false

    this._manifestFlushed = false
    this._onflush = null
    this._flushing = null
    this._activeBatch = null
    this._bitfield = null
    this._verifies = null
    this._verifiesFlushed = null
    this._legacy = !!opts.legacy

    this._closing = null
    this._opening = this._open(opts)
    this._opening.catch(noop)
  }

  ready () {
    return this._opening
  }

  async _open (opts) {
    let storage = await this.db.resume(this.discoveryKey)

    let overwrite = opts.overwrite === true

    const force = opts.force === true
    const createIfMissing = opts.createIfMissing !== false
    // kill this flag soon
    const legacy = !!opts.legacy

    // default to true for now if no manifest is provided
    let compat = opts.compat === true || (opts.compat !== false && !opts.manifest)

    let header = storage ? parseHeader(await getCoreInfo(storage)) : null

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

      const discoveryKey = opts.discoveryKey || crypto.discoveryKey(header.key)

      storage = await this.db.create({
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

    this.storage = storage
    this.header = header
    this.compat = compat
    this.tree = tree
    this.blocks = blocks
    this.bitfield = bitfield
    this.verifier = verifier
    this.state = new SessionState(this, storage, this.blocks, tree, bitfield, tree.length, null)

    if (this.key === null) this.key = this.header.key
    if (this.discoveryKey === null) this.discoveryKey = crypto.discoveryKey(this.key)
    if (this.id === null) this.id = z32.encode(this.key)
    if (this.manifest === null) this.manifest = this.header.manifest

    this._manifestFlushed = !!header.manifest
    this.opened = true
  }

  async _getTreeHeadAt (length) {
    const head = getDefaultTree()

    head.length = length

    if (length === this.header.tree.length) {
      head.fork = this.header.tree.fork
      head.rootHash = this.header.tree.rootHash
      return head
    }

    const roots = await this.tree.getRoots(length)
    const rootHash = crypto.tree(roots)

    head.fork = this.header.tree.fork
    head.rootHash = rootHash

    return head
  }

  async createSession (name, length, overwrite) {
    let storage = null
    let treeInfo = null

    if (!overwrite) {
      storage = await this.storage.openBatch(name)

      if (storage !== null) {
        treeInfo = (await getCoreHead(storage)) || getDefaultTree()
        if (length !== -1 && treeInfo.length !== length) throw STORAGE_CONFLICT('Different batch stored here')
      }
    }

    if (storage === null) {
      treeInfo = await this._getTreeHeadAt(length === -1 ? this.tree.length : length)
      storage = await this.storage.registerBatch(name, treeInfo)
    }

    const bitfield = await Bitfield.open(storage)

    bitfield.merge(this.bitfield, treeInfo.length)

    const tree = await MerkleTree.open(storage, {
      prologue: this.tree.prologue,
      length: treeInfo.length
    })

    const sharedLength = Math.min(this.tree.length, treeInfo.length)

    return new SessionState(this, storage, this.blocks, tree, bitfield, sharedLength, name)
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

    const verifier = new Verifier(this.header.key, manifest, { legacy: this._legacy })

    if (verifier.prologue) this.tree.setPrologue(verifier.prologue)

    this.manifest = this.header.manifest = manifest

    update.batch.setCoreAuth({ key: this.header.key, manifest: Verifier.encodeManifest(manifest) })

    this.compat = verifier.compat
    this.verifier = verifier
    this._manifestFlushed = false

    update.coreUpdate({ status: 0b10000, bitfield: null, value: null, from: null })
  }

  async copyPrologue (src) {
    await this.state.mutex.lock()

    try {
      await src.mutex.lock()
    } catch (err) {
      this.state.mutex.unlock()
      throw err
    }

    try {
      await copyPrologue(src, this)
    } finally {
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
          this._onupdate(update)
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
    await this.state.mutex.lock()

    const update = this.state.createUpdate()

    try {
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

      // upsert compat manifest
      if (this.verifier === null && keyPair) this._setManifest(update, null, keyPair)

      this.state.blocks.putBatch(update.batch, treeLength, values)

      update.bitfield.setRange(treeLength, length, true)
      update.flushTreeBatch(batch)

      const bitfield = { start: treeLength, length: length - treeLength, drop: false }
      const status = batch.upgraded ? 0b0001 : 0

      update.coreUpdate({ status, bitfield, value: null, from: null })

      await this.state.flushUpdate(update)

      state.treeLength = batch.length

      return {
        length: batch.length,
        byteLength: batch.byteLength
      }
    } finally {
      this.state._clearActiveBatch()
      this.updating = false
      this.state.mutex.unlock()
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

    const verifier = this.verifier || new Verifier(this.header.key, manifest, { legacy: this._legacy })

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
      return true
    } finally {
      this.state.mutex.unlock()
    }

    const remoteTreeHash = crypto.tree(proof.upgrade.nodes)
    const localTreeHash = crypto.tree(await this.tree.getRoots(proof.upgrade.length))

    if (b4a.equals(localTreeHash, remoteTreeHash)) return false

    await this._onconflict(proof)
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

  close () {
    if (!this._closing) this._closing = this._close()
    return this._closing
  }

  // session management - should be moved to some session manager next
  _onupdate ({ status, bitfield, value, from }) {
    if (this.sessions.length === 0 || this.replicator === null) return

    if (status !== 0) {
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

          if (s.encryption && s.encryption.compat !== this.compat) {
            s.encryption = this.encryption = new BlockEncryption(s.encryption.key, this.key, { compat: this.compat, isBlockKey: s.encryption.isBlockKey })
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

        if (truncated) {
          s.emit('truncate', bitfield.start, this.tree.fork)
        }

        if (appended) {
          s.emit('append')
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

  async _onconflict (proof, from) {
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

  async _close () {
    if (this.replicator) await this.replicator.destroy()
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

function getDefaultTree () {
  return {
    fork: 0,
    length: 0,
    rootHash: null,
    signature: null
  }
}

function parseHeader (info) {
  if (!info) return null

  return {
    key: info.auth.key,
    manifest: info.auth.manifest ? Verifier.decodeManifest(info.auth.manifest) : null,
    external: null,
    keyPair: info.keyPair,
    userData: [],
    tree: info.head || getDefaultTree(),
    hints: {
      reorgs: [],
      contiguousLength: 0
    }
  }
}

function noop () {}

function getCoreHead (storage) {
  const b = storage.createReadBatch()
  const p = b.getCoreHead()
  b.tryFlush()
  return p
}

async function getCoreInfo (storage) {
  const r = storage.createReadBatch()

  const auth = r.getCoreAuth()
  const localKeyPair = r.getLocalKeyPair()
  const encryptionKey = r.getEncryptionKey()
  const head = r.getCoreHead()

  await r.flush()

  return {
    auth: await auth,
    keyPair: await localKeyPair,
    encryptionKey: await encryptionKey,
    head: await head
  }
}
