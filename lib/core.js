const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const unslab = require('unslab')
const z32 = require('z32')
const Mutex = require('./mutex')
const { MerkleTree, ReorgBatch } = require('./merkle-tree')
const BitInterlude = require('./bit-interlude')
const Bitfield = require('./bitfield')
const RemoteBitfield = require('./remote-bitfield')
const { BAD_ARGUMENT, STORAGE_EMPTY, STORAGE_CONFLICT, INVALID_SIGNATURE, INVALID_CHECKSUM } = require('hypercore-errors')
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
    this.gc = 0 // corestore uses this to main a gc strike pool

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
    this.bitfield = null
    this.verifier = null
    this.truncating = 0
    this.updating = false
    this.skipBitfield = null
    this.globalCache = opts.globalCache || null
    this.autoClose = opts.autoClose !== false
    this.onidle = noop

    this.state = null
    this.opened = false
    this.destroyed = false
    this.closed = false

    this._manifestFlushed = false
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
    if (s._monitorIndex >= 0) return
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

  createUserDataStream (opts, session = this.state) {
    return session.storage.createUserDataStream(opts)
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
    if (!this.opened || this.destroyed === true || this.hasSession() === true) return
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
    if (opts.preopen) await opts.preopen // just a hook to allow exclusive access here...

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

    if (!header && (opts.discoveryKey && !(opts.key || opts.manifest))) {
      throw STORAGE_EMPTY('No Hypercore is stored here')
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
        frozen: false,
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
        manifest,
        keyPair,
        frozen: false,
        discoveryKey,
        userData: opts.userData || [],
        alias: opts.alias || null
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

    const bitfield = await Bitfield.open(storage, header.tree.length)

    const treeInfo = {
      fork: header.tree.fork,
      length: header.tree.length,
      signature: header.tree.signature,
      roots: header.tree.length ? await MerkleTree.getRootsFromStorage(storage, header.tree.length) : [],
      prologue
    }

    if (overwrite) {
      const tx = storage.write()
      tx.deleteTreeNodeRange(0, -1)
      tx.deleteBlockRange(0, -1)
      bitfield.clear(tx)
      await tx.flush()
    }

    const len = bitfield.findFirst(false, header.hints.contiguousLength)
    if (header.hints.contiguousLength !== len) {
      header.hints.contiguousLength = len
      const tx = storage.write()
      tx.setHints({ contiguousLength: len })
      await tx.flush()
    }

    // to unslab
    if (header.manifest) {
      header.manifest = Verifier.createManifest(header.manifest)
    }

    const verifier = header.manifest ? new Verifier(header.key, header.manifest, { crypto, legacy }) : null

    this.storage = storage
    this.header = header
    this.compat = compat
    this.bitfield = bitfield
    this.verifier = verifier
    this.state = new SessionState(this, null, storage, treeInfo, null)

    if (this.key === null) this.key = this.header.key
    if (this.discoveryKey === null) this.discoveryKey = crypto.discoveryKey(this.key)
    if (this.id === null) this.id = z32.encode(this.key)
    if (this.manifest === null) this.manifest = this.header.manifest

    this._manifestFlushed = !!header.manifest
  }

  async audit (opts) {
    await this.state.mutex.lock()

    try {
      return await audit(this, opts)
    } finally {
      this.state._unlock()
    }
  }

  async setManifest (manifest) {
    await this.state.mutex.lock()

    try {
      if (manifest && this.header.manifest === null) {
        if (!Verifier.isValidManifest(this.header.key, manifest)) throw INVALID_CHECKSUM('Manifest hash does not match')

        const tx = this.state.createWriteBatch()
        this._setManifest(tx, Verifier.createManifest(manifest), null)
        await this.state.flush()
      }
    } finally {
      this.state._unlock()
    }
  }

  _setManifest (tx, manifest, keyPair) {
    if (!manifest && b4a.equals(keyPair.publicKey, this.header.key)) manifest = Verifier.defaultSignerManifest(this.header.key)
    if (!manifest) return

    const verifier = new Verifier(this.header.key, manifest, { legacy: this._legacy })

    if (verifier.prologue) this.state.prologue = Object.assign({}, verifier.prologue)

    this.manifest = this.header.manifest = manifest

    tx.setAuth({
      key: this.header.key,
      discoveryKey: this.discoveryKey,
      manifest,
      keyPair: this.header.keyPair
      // TODO: encryptionKey?
    })

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

  flushed () {
    return this.state.flushed()
  }

  async _validateCommit (state, treeLength) {
    if (this.state.length > state.length) {
      return false // TODO: partial commit and truncation possible in the future
    }

    if (this.state.length > treeLength) {
      for (const root of this.state.roots) {
        const batchRoot = await MerkleTree.get(state, root.index)
        if (batchRoot.size !== root.size || !b4a.equals(batchRoot.hash, root.hash)) {
          return false
        }
      }
    }

    if (this.verifier === null) {
      return false // easier to assert than upsert
    }

    return true
  }

  _verifyBatchUpgrade (batch, manifest) {
    if (!this.header.manifest) {
      // compat, drop at some point
      if (!manifest) manifest = Verifier.defaultSignerManifest(this.header.key)

      if (!manifest || !(Verifier.isValidManifest(this.header.key, manifest) || Verifier.isCompat(this.header.key, manifest))) {
        throw INVALID_SIGNATURE('Proof contains an invalid manifest') // TODO: proper error type
      }
    }

    const verifier = this.verifier || new Verifier(this.header.key, Verifier.createManifest(manifest), { legacy: this._legacy })
    if (!verifier.verify(batch, batch.signature)) {
      throw INVALID_SIGNATURE('Proof contains an invalid signature')
    }

    return manifest
  }

  async _verifyExclusive ({ batch, bitfield, value, manifest }) {
    manifest = this._verifyBatchUpgrade(batch, manifest)

    if (!batch.commitable()) return false

    if (this.preupdate !== null) await this.preupdate(batch, this.header.key)

    if (!(await this.state._verifyBlock(batch, bitfield, value, this.header.manifest ? null : manifest))) {
      return false
    }

    if (!batch.upgraded && bitfield) {
      this.replicator.onhave(bitfield.start, bitfield.length, bitfield.drop)
    }

    return true
  }

  async _verifyShared () {
    if (!this._verifies.length) return false

    await this.state.mutex.lock()

    const tx = this.state.createWriteBatch()

    const verifies = this._verifies
    this._verifies = null
    this._verified = null

    try {
      for (const { batch, bitfield, value } of verifies) {
        if (!batch.commitable()) continue

        if (bitfield) {
          tx.putBlock(bitfield.start, value)
        }
      }

      const bits = new BitInterlude()

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
          this._setManifest(tx, manifest, null)
        }

        if (batch.commitable()) batch.commit(tx)
      }

      const ranges = bits.flush(tx, this.bitfield)

      await this.state.flush()

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
    if (this.state.length < proof.upgrade.length || proof.fork !== this.state.fork) {
      // out of date this proof - ignore for now
      return false
    }

    // sanity check -> no manifest, no way to verify
    if (!this.header.manifest) {
      return false
    }

    const batch = MerkleTree.verifyFullyRemote(this.state, proof)

    try {
      this._verifyBatchUpgrade(batch, proof.manifest)
    } catch {
      return true
    }

    const roots = await MerkleTree.getRootsFromStorage(this.storage, proof.upgrade.length)
    const remoteTreeHash = crypto.tree(proof.upgrade.nodes)
    const localTreeHash = crypto.tree(roots)

    try {
      const rx = this.state.storage.read()
      const treeProofPromise = MerkleTree.proof(this.state, rx, {
        block: null,
        hash: null,
        seek: null,
        upgrade: {
          start: 0,
          length: proof.upgrade.length
        }
      })

      rx.tryFlush()

      const treeProof = await treeProofPromise

      const verifyBatch = MerkleTree.verifyFullyRemote(this.state, await treeProof.settle())
      this._verifyBatchUpgrade(verifyBatch, this.header.manifest)
    } catch {
      return true
    }

    // both proofs are valid, now check if they forked
    if (b4a.equals(localTreeHash, remoteTreeHash)) return false

    await this.state.mutex.lock()

    try {
      const tx = this.state.createWriteBatch()

      this.header.frozen = true

      tx.setAuth({
        key: this.header.key,
        discoveryKey: this.discoveryKey,
        manifest: this.header.manifest,
        keyPair: this.header.keyPair,
        frozen: true
      })

      await this.state.flush()
    } finally {
      this.state.mutex.unlock()
    }

    // tmp log so we can see these
    const id = b4a.toString(this.discoveryKey, 'hex')
    console.log('[hypercore] conflict detected in ' + id + ' (writable=' + !!this.header.keyPair + ',quorum=' + this.header.manifest.quorum + ')')
    await this._onconflict(proof)
    return true
  }

  async verifyReorg (proof) {
    const batch = new ReorgBatch(this.state)
    await MerkleTree.reorg(this.state, proof, batch)
    const manifest = this._verifyBatchUpgrade(batch, proof.manifest)

    if (manifest && !this.header.manifest) {
      await this.state.mutex.lock()
      try {
        if (manifest && this.header.manifest === null) {
          const tx = this.state.createWriteBatch()
          this._setManifest(tx, Verifier.createManifest(manifest), null)
          await this.state.flush()
        }
      } finally {
        this.state._unlock()
      }
    }

    return batch
  }

  async verify (proof, from) {
    // We cannot apply "other forks" atm.
    // We should probably still try and they are likely super similar for non upgrades
    // but this is easy atm (and the above layer will just retry)
    if (proof.fork !== this.state.fork) return false

    const batch = await MerkleTree.verify(this.state, proof)
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
    const buf = this.bitfield.toBuffer(this.state.length)
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

    const { start, length, drop } = bitfield

    this._setBitfieldRanges(start, start + length, true)
    this.updateContiguousLength({ start, length, drop: false })

    this.replicator.onupgrade()
    this.replicator.onhave(start, length, drop)
    this.replicator.uncork()
  }

  ontruncate (tree, { start, length }) {
    if (tree) this.header.tree = tree

    this.replicator.cork()

    this.replicator.ontruncate(start, length)
    this.replicator.onhave(start, length, true)
    this.replicator.onupgrade()
    this.replicator.uncork()

    for (const sessionState of this.sessionStates) {
      if (start < sessionState.snapshotCompatLength) sessionState.snapshotCompatLength = start
    }

    this._setBitfieldRanges(start, start + length, false)
    this.updateContiguousLength({ start, length, drop: true })
  }

  async _onconflict (proof) {
    await this.replicator.onconflict()

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

  async destroy () {
    if (this.destroyed === true) return
    this.destroyed = true

    if (this.hasSession() === true) throw new Error('Cannot destroy while sessions are open')

    const weakSessions = this.allSessions()

    if (this.replicator) this.replicator.destroy()
    if (this.state) await this.state.close()

    // close all pending weak sessions...
    for (const s of weakSessions) s.close().catch(noop)
  }

  async _close () {
    if (this.opened === false) await this.opening
    if (this.hasSession() === true) throw new Error('Cannot close while sessions are open')

    if (this.replicator) await this.replicator.close()

    await this.destroy()
    if (this.autoClose) await this.storage.store.close()

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
    key: info.key,
    manifest: info.manifest,
    external: null,
    keyPair: info.keyPair,
    tree: info.head || getDefaultTree(),
    hints: {
      reorgs: [],
      contiguousLength: info.hints ? info.hints.contiguousLength : 0
    }
  }
}

function noop () {}

async function getCoreInfo (storage) {
  const r = storage.read()

  const auth = r.getAuth()
  const head = r.getHead()
  const hints = r.getHints()

  r.tryFlush()

  return {
    ...await auth,
    head: await head,
    hints: await hints
  }
}
