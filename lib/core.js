const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const unslab = require('unslab')
const z32 = require('z32')
const Mutex = require('./mutex')
const MerkleTree = require('./merkle-tree')
const BlockStore = require('./block-store')
const BitInterlude = require('./bit-interlude')
const Bitfield = require('./bitfield')
const RemoteBitfield = require('./remote-bitfield')
const { BAD_ARGUMENT, STORAGE_EMPTY, STORAGE_CONFLICT, INVALID_OPERATION, INVALID_SIGNATURE, INVALID_CHECKSUM } = require('hypercore-errors')
const Verifier = require('./verifier')
const audit = require('./audit')
const copyPrologue = require('./copy-prologue')
const SessionState = require('./session-state')
const Replicator = require('./replicator')

module.exports = class Core {
  constructor (db, opts = {}) {
    this.db = db
    this.storage = null
    this.replicator = new Replicator(this, opts)
    this.sessionStates = []
    this.monitors = []
    this.activeSessions = 0

    this.id = opts.key ? z32.encode(opts.key) : null
    this.key = opts.key || null
    this.discoveryKey = opts.discoveryKey || (opts.key && crypto.discoveryKey(opts.key)) || null
    this.manifest = null
    this.opening = null
    this.closing = null
    this.exclusive = null

    this.preupdate = null
    this.header = null
    this.compat = false
    this.tree = null
    this.blocks = null
    this.bitfield = null
    this.verifier = null
    this.truncating = 0
    this.updating = false
    this.skipBitfield = null
    this.globalCache = opts.globalCache || null
    this.autoClose = opts.autoClose !== false
    this.encryption = null
    this.onidle = noop

    this.state = null
    this.opened = false
    this.destroyed = false
    this.closed = false

    this._manifestFlushed = false
    this._onflush = null
    this._flushing = null
    this._activeBatch = null
    this._bitfield = null
    this._verifies = null
    this._verifiesFlushed = null
    this._legacy = !!opts.legacy

    this.opening = this._open(opts)
    this.opening.catch(noop)
  }

  ready () {
    return this.opening
  }

  addMonitor (s) {
    s._monitorIndex = this.monitors.push(s) - 1
  }

  removeMonitor (s) {
    if (s._monitorIndex < 0) return
    const head = this.monitors.pop()
    if (head !== s) this.monitors[(head._monitorIndex = s._monitorIndex)] = head
    s._monitorIndex = -1
  }

  emitManifest () {
    for (let i = this.monitors.length - 1; i >= 0; i--) {
      this.monitors[i].emit('manifest')
    }
  }

  createUserDataStream (opts) {
    const storage = (opts && opts.session) ? opts.session.state.storage : this.storage
    return storage.createUserDataStream(opts)
  }

  allSessions () {
    const sessions = []
    for (const state of this.sessionStates) {
      if (state.sessions.length) sessions.push(...state.sessions)
    }
    return sessions
  }

  hasSession () {
    return this.activeSessions !== 0
  }

  checkIfIdle () {
    if (this.destroyed === true || this.hasSession() === true) return
    if (this.replicator.idle() === false) return
    if (this.state === null || this.state.mutex.idle() === false) return
    this.onidle()
  }

  async lockExclusive () {
    if (this.exclusive === null) this.exclusive = new Mutex()
    await this.exclusive.lock()
  }

  unlockExclusive () {
    if (this.exclusive !== null) this.exclusive.unlock()
  }

  async _open (opts) {
    try {
      await this._tryOpen(opts)
    } catch (err) {
      this.onidle()
      throw err
    }

    this.opened = true
  }

  async _tryOpen (opts) {
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
        discoveryKey,
        userData: opts.userData || []
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

    const tree = await MerkleTree.open(storage, header.tree.length, { crypto, prologue, ...header.tree })
    const bitfield = await Bitfield.open(storage, header.tree.length)
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
    this.state = new SessionState(this, storage, this.blocks, tree, -1, null)

    this.state.ref()

    if (this.key === null) this.key = this.header.key
    if (this.discoveryKey === null) this.discoveryKey = crypto.discoveryKey(this.key)
    if (this.id === null) this.id = z32.encode(this.key)
    if (this.manifest === null) this.manifest = this.header.manifest

    this._manifestFlushed = !!header.manifest
  }

  async audit () {
    await this.state.mutex.lock()

    try {
      const storage = this.state.createWriteBatch()

      // TODO: refactor audit
      const corrections = await audit(this, storage)
      if (corrections.blocks || corrections.tree) {
        await this.state.flushUpdate(storage)
      }

      return corrections
    } finally {
      this.state._unlock()
    }
  }

  async setManifest (manifest) {
    await this.state.mutex.lock()

    try {
      if (manifest && this.header.manifest === null) {
        if (!Verifier.isValidManifest(this.header.key, manifest)) throw INVALID_CHECKSUM('Manifest hash does not match')

        const storage = this.state.createWriteBatch()
        this._setManifest(storage, Verifier.createManifest(manifest), null)

        await this.state.flushWriteBatch(storage)
        this.replicator.onupgrade()
      }
    } finally {
      this.state._unlock()
    }
  }

  _setManifest (storage, manifest, keyPair) {
    if (!manifest && b4a.equals(keyPair.publicKey, this.header.key)) manifest = Verifier.defaultSignerManifest(this.header.key)
    if (!manifest) return

    const verifier = new Verifier(this.header.key, manifest, { legacy: this._legacy })

    if (verifier.prologue) this.tree.setPrologue(verifier.prologue)

    this.manifest = this.header.manifest = manifest

    storage.setCoreAuth({ key: this.header.key, manifest: Verifier.encodeManifest(manifest) })

    this.compat = verifier.compat
    this.verifier = verifier
    this._manifestFlushed = false

    this.replicator.onupgrade()
    this.emitManifest()
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
      this.checkIfIdle()
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

  async commit (state, { signature, keyPair = this.header.keyPair, length = state.tree.length, treeLength = state.flushedLength(), overwrite = false } = {}) {
    let sourceLocked = false

    await this.state.mutex.lock()

    try {
      await state.mutex.lock()
      sourceLocked = true

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

      if (this.verifier === null) {
        throw INVALID_OPERATION('Cannot commit without manifest') // easier to assert than upsert
      }

      if (this.tree.length < length && !signature) {
        signature = this.verifier.sign(state.tree.batch(), keyPair)
      }

      const tree = await this.state._overwrite(state, length, treeLength, signature)

      // gc blocks from source
      if (treeLength < length) {
        const storage = state.createWriteBatch()

        state.blocks.clear(storage, treeLength, length)
        const dependency = state.updateDependency(storage, length)

        await state.flushWriteBatch(storage)

        if (dependency) state.refreshDependencies(dependency)
      }

      if (this.header.hints.contiguousLength === treeLength) {
        // TODO: we need to persist this somehow
        this.header.hints.contiguousLength = length
      }

      // update in memory bitfield
      this._setBitfieldRanges(treeLength, length, true)

      if (this.header.tree.length < tree.length || treeLength < this.header.tree.length) {
        this.header.tree = tree
      }

      const bitfield = { start: treeLength, length: length - treeLength, drop: false }
      this.state.onappend(bitfield)

      return {
        length: this.tree.length,
        byteLength: this.tree.byteLength
      }
    } finally {
      this.updating = false
      this.state.mutex.unlock()

      if (sourceLocked) {
        state.mutex.unlock()
        state._clearActiveBatch()
      }

      this.checkIfIdle()
    }
  }

  _verifyBatchUpgrade (batch, manifest) {
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
  }

  async _verifyExclusive ({ batch, bitfield, value, manifest }) {
    this._verifyBatchUpgrade(batch, manifest)
    if (!batch.commitable()) return false

    if (this.preupdate !== null) await this.preupdate(batch, this.header.key)

    await this.state._verifyBlock(batch, bitfield, value, this.header.manifest ? null : manifest)

    if (!batch.upgraded && bitfield) {
      this.replicator.onhave(bitfield.start, bitfield.length, bitfield.drop)
    }

    return true
  }

  async _verifyShared () {
    if (!this._verifies.length) return false

    await this.state.mutex.lock()

    const storage = this.state.createWriteBatch()

    const verifies = this._verifies
    this._verifies = null
    this._verified = null

    try {
      for (const { batch, bitfield, value } of verifies) {
        if (!batch.commitable()) continue

        if (bitfield) {
          storage.putBlock(bitfield.start, value)
        }
      }

      const bits = new BitInterlude()
      const treeUpdates = []

      for (let i = 0; i < verifies.length; i++) {
        const { batch, bitfield, manifest } = verifies[i]

        if (!batch.commitable()) {
          verifies[i] = null // signal that we cannot commit this one
          continue
        }

        if (bitfield) {
          bits.setRange(bitfield.start, bitfield.start + 1, true)
        }

        // if we got a manifest AND its strictly a non compat one, lets store it
        if (manifest && this.header.manifest === null) {
          if (!Verifier.isValidManifest(this.header.key, manifest)) throw INVALID_CHECKSUM('Manifest hash does not match')
          this._setManifest(storage, manifest, null)
        }

        if (batch.commitable()) treeUpdates.push(batch.commit(storage))
      }

      const ranges = bits.flush(storage, this.bitfield)

      await this.state.flushWriteBatch(storage)

      for (const batch of treeUpdates) {
        this.state.tree.onupdate(batch)
      }

      for (const { start, end, value } of ranges) {
        this._setBitfieldRanges(start, end, value)
      }

      for (let i = 0; i < verifies.length; i++) {
        const bitfield = verifies[i] && verifies[i].bitfield
        if (bitfield) {
          this.replicator.onhave(bitfield.start, bitfield.length, bitfield.drop)
          this.updateContiguousLength(bitfield)
        }
      }
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

    try {
      this._verifyBatchUpgrade(batch, proof.manifest)
    } catch {
      return true
    }

    await this.state.mutex.lock()

    try {
      const storage = this.state.createWriteBatch()
      if (this.header.manifest === null && proof.manifest) {
        this._setManifest(storage, proof.manifest, null)
      }

      await this.state.flushWriteBatch(storage)
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
    this._verifyBatchUpgrade(batch, proof.manifest)
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

  async reorg (batch) {
    if (!batch.commitable()) return false

    this.truncating++

    try {
      await this.state.reorg(batch)
    } finally {
      this.truncating--
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

  _setBitfieldRanges (start, end, value) {
    this.bitfield.setRange(start, end, value)
    if (this.skipBitfield !== null) this.skipBitfield.setRange(start, end, value)
  }

  close () {
    if (!this.closing) this.closing = this._close()
    return this.closing
  }

  updateContiguousLength (bitfield) {
    const contig = updateContigBatch(this.header.hints.contiguousLength, bitfield, this.bitfield)

    if (contig.length !== -1 && contig.length !== this.header.hints.contiguousLength) {
      this.header.hints.contiguousLength = contig.length
    }
  }

  onappend (tree, bitfield) {
    this.header.tree = tree

    if (!bitfield) {
      this.replicator.onupgrade()
      return
    }

    this.replicator.cork()

    this._setBitfieldRanges(bitfield.start, bitfield.start + bitfield.length, true)
    this.updateContiguousLength(bitfield)

    this.replicator.onupgrade()
    this.replicator.onhave(bitfield.start, bitfield.length, bitfield.drop)
    this.replicator.uncork()
  }

  ontruncate (tree, to, from) {
    if (tree) this.header.tree = tree

    this.replicator.cork()

    const length = from - to

    this.replicator.ontruncate(to, length)
    this.replicator.onhave(to, length, true)
    this.replicator.onupgrade()
    this.replicator.uncork()

    for (const sessionState of this.sessionStates) {
      if (to < sessionState.snapshotCompatLength) sessionState.snapshotCompatLength = to
    }

    this._setBitfieldRanges(to, from, false)
    this.updateContiguousLength({ start: to, length, drop: true })
  }

  async _onconflict (proof, from) {
    await this.replicator.onconflict(from)

    for (let i = this.monitors.length - 1; i >= 0; i--) {
      const s = this.monitors[i]
      s.emit('conflict', proof.upgrade.length, proof.fork, proof)
    }

    const err = new Error('Two conflicting signatures exist for length ' + proof.upgrade.length)
    await this.closeAllSessions(err)
  }

  async closeAllSessions (err) {
    // this.sessions modifies itself when a session closes
    // This way we ensure we indeed iterate over all sessions
    const sessions = this.allSessions()

    const all = []
    for (const s of sessions) all.push(s.close({ error: err, force: false })) // force false or else infinite recursion
    await Promise.allSettled(all)
  }

  destroy () {
    if (this.destroyed === true) return
    this.destroyed = true

    if (this.hasSession() === true) throw new Error('Cannot destroy while sessions are open')

    const weakSessions = this.allSessions()

    if (this.replicator) this.replicator.destroy()
    if (this.state) this.state.destroy()

    // close all pending weak sessions...
    for (const s of weakSessions) s.close().catch(noop)
  }

  async _close () {
    if (this.opened === false) await this.opening
    if (this.hasSession() === true) throw new Error('Cannot close while sessions are open')

    if (this.replicator) await this.replicator.close()

    this.destroy()
    if (this.autoClose) await this.storage.root.close()

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
      length: -1
    }
  }

  if (c > start) {
    return {
      length: c
    }
  }

  return {
    length: c
  }
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
