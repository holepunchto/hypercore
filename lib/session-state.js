const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const assert = require('nanoassert')
const flat = require('flat-tree')
const quickbit = require('quickbit-universal')

const { INVALID_OPERATION, INVALID_SIGNATURE } = require('hypercore-errors')

const Mutex = require('./mutex')
const Bitfield = require('./bitfield')
const { MerkleTree, MerkleTreeBatch } = require('./merkle-tree')

module.exports = class SessionState {
  constructor (core, parent, storage, treeInfo, name) {
    this.core = core
    this.index = this.core.sessionStates.push(this) - 1

    this.storage = storage
    this.name = name
    this.sessions = []

    // small hack to close old storages as late as possible.
    // TODO: add a read lock so we can avoid that
    this.lingers = null

    this.parent = parent
    this.atomized = null
    this.mutex = new Mutex()

    // merkle state
    this.roots = treeInfo.roots.length ? treeInfo.roots : []
    this.fork = treeInfo.fork || 0
    this.length = MerkleTree.span(this.roots) / 2
    this.byteLength = MerkleTree.size(this.roots)
    this.prologue = treeInfo.prologue || null
    this.signature = treeInfo.signature || null

    this.snapshotCompatLength = this.isSnapshot() ? Math.min(this.length, this.core.state.length) : -1
    this.lastTruncation = null

    this.active = 0

    this._activeTx = null
    this._pendingBitfield = null

    this.ref()
  }

  isSnapshot () {
    return this.storage.snapshotted
  }

  isDefault () {
    return this.core.state === this || this.isAtomicDefault()
  }

  isAtomicDefault () {
    return !!this.storage.atom && !!this.parent && this.parent.isDefault()
  }

  createTreeBatch () {
    return new MerkleTreeBatch(this)
  }

  addSession (s) {
    if (s._stateIndex !== -1) return
    s._stateIndex = this.sessions.push(s) - 1
    if (s.weak === false) this.core.activeSessions++
  }

  removeSession (s) {
    if (s._stateIndex === -1) return
    const head = this.sessions.pop()
    if (head !== s) this.sessions[(head._stateIndex = s._stateIndex)] = head
    s._stateIndex = -1
    if (s.weak === false) this.core.activeSessions--
    this.core.checkIfIdle()
  }

  flushedLength () {
    if (this.isDefault() || this.isSnapshot()) return this.length
    const deps = this.storage.dependencies
    if (deps.length) return deps[deps.length - 1].length
    return 0
  }

  signedLength () {
    const l = Math.min(this.flushedLength(), this.core.state.length)
    return this.isSnapshot() && l > this.snapshotCompatLength ? this.snapshotCompatLength : l
  }

  unref () {
    if (--this.active > 0) return
    this.close().catch(noop) // technically async, but only for the last db session
  }

  ref () {
    this.active++
    return this
  }

  hash () {
    return MerkleTree.hash(this)
  }

  setRoots (roots) {
    this.roots = roots
    this.length = MerkleTree.span(roots) / 2
    this.byteLength = MerkleTree.size(roots)
  }

  get encryptionFork () {
    return this.core.header.tree.fork
  }

  async updateSnapshotStorage (storage) {
    if (!this.atomized || !this.atomized.flushing) return this.treeInfo()
    await this.atomized.flushed()

    let rx = storage.read()
    const headPromise = rx.getHead()
    const authPromise = rx.getAuth()
    const depPromise = rx.getDependency()

    rx.tryFlush()
    const [head, auth, dep] = await Promise.all([headPromise, authPromise, depPromise])

    storage.setDependencyHead(dep)

    const fork = head ? head.fork : 0
    const length = head ? head.length : 0

    rx = storage.read()
    const rootPromises = []
    for (const r of flat.fullRoots(length * 2)) {
      rootPromises.push(rx.getTreeNode(r))
    }

    rx.tryFlush()

    const roots = await Promise.all(rootPromises)

    // dbl check if we are hitting an regression from earler
    for (const root of roots) {
      if (root === null) {
        throw new Error('Bad snapshot from atomized session, id = ' + this.core.id + ' length = ' + length + ' fork = ' + fork)
      }
    }

    return {
      fork,
      roots,
      length,
      prologue: auth.manifest && auth.manifest.prologue,
      signature: head && head.signature
    }
  }

  treeInfo () {
    return {
      fork: this.fork,
      roots: this.roots.slice(),
      length: this.length,
      prologue: this.prologue,
      signature: this.signature
    }
  }

  async close () {
    if (this.index === -1) return

    this.active = 0
    this.mutex.destroy(new Error('Closed')).catch(noop)
    if (this.parent && this.parent.atomized) this.parent.atomized = null

    const closing = this.storage.close()

    const head = this.core.sessionStates.pop()
    if (head !== this) this.core.sessionStates[(head.index = this.index)] = head

    this.index = -1
    this.core.checkIfIdle()

    if (this.lingers !== null) {
      for (const storage of this.lingers) await storage.close()
    }

    return closing
  }

  async snapshot () {
    const storage = this.storage.snapshot()
    const treeInfo = await this.updateSnapshotStorage(storage)

    const s = new SessionState(
      this.core,
      null,
      storage,
      treeInfo,
      this.name
    )

    return s
  }

  updateDependency (tx, length) {
    const dependency = updateDependency(this, length, false)
    if (dependency) tx.setDependency(dependency)
    return dependency
  }

  _clearActiveBatch () {
    this._activeTx = null
  }

  createWriteBatch () {
    assert(!this._activeTx && !this.storage.snapshotted)

    this._activeTx = this.storage.write()
    return this._activeTx
  }

  _unlock () {
    this._clearActiveBatch()
    this.mutex.unlock()
    this.core.checkIfIdle()
  }

  async flush () {
    const tx = this._activeTx
    this._activeTx = null

    try {
      if (!(await tx.flush())) return false
    } finally {
      this._clearActiveBatch()
    }

    this.lastTruncation = null
    return true
  }

  _precommit () {
    this.commiting = true
  }

  async _commit () {
    await this.mutex.lock()

    try {
      const bitfield = this._pendingBitfield
      this._pendingBitfield = null
      this.lastTruncation = null
      await this.parent._oncommit(this, bitfield)
    } finally {
      this.commiting = false
      this.mutex.unlock()
    }
  }

  async _oncommit (src, bitfield) {
    await this.mutex.lock()

    try {
      const currLength = this.length

      // load dependency into memory
      const rx = this.storage.read()
      const dependencyPromise = rx.getDependency()

      rx.tryFlush()

      const dependency = await dependencyPromise

      this.fork = src.fork
      this.length = src.length
      this.byteLength = src.byteLength
      this.roots = src.roots.slice()
      this.signature = src.signature

      const tree = {
        fork: this.fork,
        length: this.length,
        rootHash: this.hash(),
        signature: this.signature
      }

      if (dependency) this.storage.setDependencyHead(dependency)

      const b = bitfield

      if (b && b.truncated && b.start < currLength) {
        this.ontruncate(tree, b.start, currLength, true)
        if (!b || b.appends === 0) return
      }

      const append = b ? { start: b.start, length: b.appends, drop: false } : null

      this.onappend(tree, append, true)
    } finally {
      this.mutex.unlock()
      this.core.checkIfIdle()
    }
  }

  async setUserData (key, value) {
    await this.mutex.lock()

    try {
      const tx = this.createWriteBatch()
      tx.putUserData(key, value)

      return await this.flush()
    } finally {
      this._unlock()
    }
  }

  async _verifyBlock (batch, bitfield, value, manifest, from) {
    await this.mutex.lock()

    try {
      if (!batch.commitable()) return false
      const tx = this.createWriteBatch()
      this.updating = true

      if (bitfield) {
        tx.putBlock(bitfield.start, value)
      }

      if (bitfield && this.isDefault()) {
        await storeBitfieldRange(this.storage, tx, bitfield.start, bitfield.start + 1, true)
      }

      if (manifest) this.core._setManifest(tx, manifest, null)

      assert(batch.commitable(), 'Should still be commitable')
      batch.commit(tx)

      const head = {
        fork: batch.fork,
        length: batch.length,
        rootHash: batch.hash(),
        signature: batch.signature
      }

      if (batch.upgraded) tx.setHead(head)

      const flushed = await this.flush()

      if (batch.upgraded) {
        this.roots = batch.roots
        this.length = batch.length
        this.byteLength = batch.byteLength
        this.fork = batch.fork
        this.signature = batch.signature

        this.onappend(head, bitfield, flushed)
      }
    } finally {
      this._clearActiveBatch()
      this.updating = false
      this.mutex.unlock()
    }

    return true
  }

  async truncate (length, fork, { signature, keyPair } = {}) {
    if (!keyPair && this.isDefault()) keyPair = this.core.header.keyPair

    await this.mutex.lock()

    try {
      if (this.prologue && length < this.prologue.length) {
        throw INVALID_OPERATION('Truncation breaks prologue')
      }
      if (length > this.length) {
        throw INVALID_OPERATION('Not a truncation, ' + length + ' must be less or equal to ' + this.length)
      }

      const batch = this.createTreeBatch()
      await MerkleTree.truncate(this, length, batch, fork)

      if (!signature && keyPair && length > 0) signature = this.core.verifier.sign(batch, keyPair)
      if (signature) batch.signature = signature

      const tx = this.createWriteBatch()

      // upsert compat manifest
      if (this.core.verifier === null && keyPair) this.core._setManifest(tx, null, keyPair)

      const { dependency, tree, roots } = await this._truncate(tx, batch)

      for (const sessionState of this.core.sessionStates) {
        if (sessionState.isSnapshot() && sessionState.name === this.name && length < sessionState.snapshotCompatLength) {
          sessionState.snapshotCompatLength = length
        }
      }

      const flushed = await this.flush()

      this.fork = tree.fork
      this.length = tree.length
      this.byteLength = MerkleTree.size(roots)
      this.roots = roots
      this.signature = tree.signature

      if (dependency) this.storage.setDependencyHead(dependency)

      this.ontruncate(tree, tree.length, batch.treeLength, flushed)
    } finally {
      this._unlock()
    }
  }

  async reorg (batch) {
    await this.mutex.lock()

    const storage = this.createWriteBatch()

    try {
      if (!batch.commitable()) return false

      const { dependency, tree } = await this._truncate(storage, batch)

      const flushed = await this.flush()

      this.fork = batch.fork
      this.length = batch.length
      this.byteLength = batch.byteLength
      this.roots = batch.roots
      this.signature = batch.signature

      if (dependency) this.storage.setDependencyHead(dependency)

      this.ontruncate(tree, batch.ancestors, batch.treeLength, flushed)
    } finally {
      this._unlock()
    }
  }

  async _truncate (storage, batch) {
    storage.deleteBlockRange(batch.ancestors, batch.treeLength)

    assert(batch.commitable(), 'Batch must be commitable')

    const tree = {
      fork: batch.fork,
      length: batch.length,
      rootHash: batch.hash(),
      signature: batch.signature
    }

    storage.setHead(tree)
    batch.commit(storage)

    const truncated = batch.length < this.flushedLength()
    const dependency = truncated ? updateDependency(this, batch.length, true) : null

    if (dependency) storage.setDependency(dependency)

    if (this.isDefault()) {
      await storeBitfieldRange(this.storage, storage, batch.ancestors, batch.treeLength, false)
      if (batch.ancestors < this.core.header.hints.contiguousLength) {
        storage.setHints({ contiguousLength: batch.ancestors })
      }
    }

    return { dependency, tree, roots: batch.roots }
  }

  async clear (start, end, cleared) {
    await this.mutex.lock()

    try {
      const tx = this.createWriteBatch()

      if (this.isDefault()) {
        await storeBitfieldRange(this.storage, tx, start, end, false)
        if (start < this.core.header.hints.contiguousLength) {
          tx.setHints({ contiguousLength: start })
        }
      }

      tx.deleteBlockRange(start, end)

      const dependency = start < this.flushedLength() ? updateDependency(this, start, true) : null

      const flushed = await this.flush()

      if (dependency) this.storage.setDependencyHead(dependency)

      // todo: atomic event handle
      if (this.isDefault() && flushed) {
        const length = end - start
        this.core.updateContiguousLength({ start, length, drop: true })
        this.core._setBitfieldRanges(start, end, false)
        this.core.replicator.onhave(start, length, true)
      }
    } finally {
      this._unlock()
    }
  }

  async append (values, { signature, keyPair, preappend } = {}) {
    if (!keyPair && this.isDefault()) keyPair = this.core.header.keyPair

    await this.mutex.lock()

    try {
      const tx = this.createWriteBatch()

      // upsert compat manifest
      if (this.core.verifier === null && keyPair) this.core._setManifest(tx, null, keyPair)

      if (preappend) await preappend(values)

      if (!values.length) {
        await this.flush()
        return { length: this.length, byteLength: this.byteLength }
      }

      const batch = this.createTreeBatch()
      for (const val of values) batch.append(val)

      // only multisig can have prologue so signature is always present
      if (this.prologue && batch.length < this.prologue.length) {
        throw INVALID_OPERATION('Append is not consistent with prologue')
      }

      if (!signature && keyPair) signature = this.core.verifier.sign(batch, keyPair)
      if (signature) batch.signature = signature

      batch.commit(tx)

      const tree = {
        fork: batch.fork,
        length: batch.length,
        rootHash: batch.hash(),
        signature: batch.signature
      }

      tx.setHead(tree)

      if (this.isDefault()) {
        await storeBitfieldRange(this.storage, tx, batch.ancestors, batch.length, true)
        if (this.length === this.core.header.hints.contiguousLength) {
          tx.setHints({ contiguousLength: this.length + values.length })
        }
      }

      for (let i = 0; i < values.length; i++) {
        tx.putBlock(this.length + i, values[i])
      }

      const bitfield = {
        drop: false,
        start: batch.ancestors,
        length: values.length
      }

      const flushed = await this.flush()

      this.fork = batch.fork
      this.roots = batch.roots
      this.length = batch.length
      this.byteLength = batch.byteLength
      this.signature = batch.signature

      this.onappend(tree, bitfield, flushed)

      return { length: this.length, byteLength: this.byteLength }
    } finally {
      this._unlock()
    }
  }

  onappend (tree, bitfield, flushed) {
    if (!flushed) this._updateBitfield(bitfield)
    else if (this.isDefault()) this.core.onappend(tree, bitfield)

    for (let i = this.sessions.length - 1; i >= 0; i--) {
      this.sessions[i].emit('append')
    }
  }

  ontruncate (tree, to, from, flushed) {
    const bitfield = { start: to, length: from - to, drop: true }

    this.lastTruncation = { from, to }

    if (!flushed) this._updateBitfield(bitfield)
    else if (this.isDefault()) this.core.ontruncate(tree, bitfield)

    for (const sessionState of this.core.sessionStates) {
      if (sessionState.isSnapshot() && sessionState.name === this.name && to < sessionState.snapshotCompatLength) {
        sessionState.snapshotCompatLength = to
      }
    }

    for (let i = this.sessions.length - 1; i >= 0; i--) {
      this.sessions[i].emit('truncate', to, tree.fork)
    }
  }

  _updateBitfield (bitfield, flushed) {
    if (!bitfield) return

    const p = this._pendingBitfield
    const b = bitfield

    if (b.drop) {
      // truncation must be from end
      if (p && (b.start + b.length !== p.start + p.appends)) {
        throw INVALID_OPERATION('Atomic truncations must be contiguous')
      }

      // actual truncation
      if (p === null || b.start < p.start) {
        this._pendingBitfield = { truncated: true, start: b.start, appends: 0 }
        return
      }

      // just clearing batch data
      p.appends = b.start - p.start

      // we cleared the current batch
      if (p.appends === 0) this._pendingBitfield = null

      return
    }

    if (p === null) {
      this._pendingBitfield = { truncated: false, start: b.start, appends: b.length }
      return
    }

    if (b.start !== p.start + p.appends) {
      throw INVALID_OPERATION('Atomic operations must be contiguous')
    }

    p.appends += b.length
  }

  async catchup (length) {
    assert(!this.isDefault(), 'Cannot catchup signed state') // TODO: make this check better

    await this.mutex.lock()

    try {
      const origLength = this.length

      let sharedLength = 0
      for (let i = this.storage.dependencies.length - 1; i >= 0; i--) {
        const dep = this.storage.dependencies[i]
        if (dep.dataPointer === this.core.state.storage.core.dataPointer) {
          sharedLength = dep.length
          break
        }
      }

      const tx = this.createWriteBatch()
      const rx = this.core.state.storage.read()
      const rootPromises = []

      for (const root of flat.fullRoots(length * 2)) {
        rootPromises.push(rx.getTreeNode(root))
      }

      rx.tryFlush()

      const roots = await Promise.all(rootPromises)
      const truncating = sharedLength < origLength

      for (const node of roots) {
        if (node === null) throw INVALID_OPERATION('Invalid catchup length, tree nodes not available')
      }

      const fork = truncating ? this.fork + 1 : this.fork

      // overwrite it atm, TODO: keep what we can connect to the tree
      tx.deleteBlockRange(0, -1)
      tx.deleteTreeNodeRange(0, -1)
      tx.deleteBitfieldPageRange(0, -1)

      const tree = {
        fork,
        length,
        rootHash: crypto.tree(roots),
        signature: null
      }

      tx.setHead(tree)

      // prop a better way to do this
      const dep = updateDependency(this, sharedLength, true)
      dep.length = length

      tx.setDependency(dep)

      const flushed = await this.flush()

      this.storage.setDependencyHead(dep)

      this.fork = tree.fork
      this.roots = roots
      this.length = tree.length
      this.byteLength = MerkleTree.size(roots)

      if (truncating) this.ontruncate(tree, sharedLength, origLength, flushed)
      if (sharedLength < length) this.onappend(tree, null, flushed)
    } finally {
      this.mutex.unlock()
    }
  }

  async _overwrite (source, fork, length, treeLength, signature) {
    const blockPromises = []
    const treePromises = []
    const rootPromises = []

    const rx = source.storage.read()

    for (const root of flat.fullRoots(length * 2)) {
      rootPromises.push(rx.getTreeNode(root))
    }

    for (const index of flat.patch(treeLength * 2, length * 2)) {
      treePromises.push(rx.getTreeNode(index))
    }

    for (let i = treeLength; i < length; i++) {
      treePromises.push(rx.getTreeNode(i * 2))
      treePromises.push(rx.getTreeNode(i * 2 + 1))
      blockPromises.push(rx.getBlock(i))
    }

    rx.tryFlush()

    const blocks = await Promise.all(blockPromises)
    const nodes = await Promise.all(treePromises)
    const roots = await Promise.all(rootPromises)

    if (this.core.destroyed) throw new Error('Core destroyed')

    if (signature) {
      const batch = this.createTreeBatch()
      batch.roots = roots
      batch.length = length

      if (!this.core.verifier.verify(batch, signature)) {
        throw INVALID_SIGNATURE('Signature is not valid over committed tree')
      }
    }

    const tx = this.createWriteBatch()

    // truncate existing tree
    if (treeLength < this.length) {
      tx.deleteBlockRange(treeLength, this.length)
    }

    for (const root of roots) tx.putTreeNode(root)

    // no nodes will be copied in shallow mode
    for (const node of nodes) {
      if (node !== null) tx.putTreeNode(node)
    }

    for (let i = 0; i < blocks.length; i++) {
      assert(blocks[i] !== null, 'has block')
      tx.putBlock(i + treeLength, blocks[i])
    }

    const totalLength = Math.max(length, this.length)

    if (totalLength > treeLength) {
      const firstPage = getBitfieldPage(treeLength)
      const lastPage = getBitfieldPage(totalLength - 1)

      const srx = this.storage.read()
      const bitfieldPagePromise = srx.getBitfieldPage(firstPage)
      srx.tryFlush()

      const bitfieldPage = await bitfieldPagePromise

      let index = treeLength

      for (let i = firstPage; i <= lastPage; i++) {
        const page = b4a.alloc(Bitfield.BYTES_PER_PAGE)
        tx.putBitfieldPage(i, page)

        // copy existing bits in
        if (i === firstPage && bitfieldPage) page.set(bitfieldPage)

        if (index < length) {
          index = fillBitfieldPage(page, index, length, i, true)
          if (index < length) continue
        }

        if (index < this.length) {
          index = fillBitfieldPage(page, index, this.length, i, false)
        }
      }
    }

    const tree = {
      fork,
      length,
      rootHash: crypto.tree(roots),
      signature
    }

    const upgraded = treeLength < this.length || this.length < length || tree.fork !== this.fork

    if (upgraded) tx.setHead(tree)

    const flushed = await this.flush()

    this.fork = tree.fork
    this.roots = roots
    this.length = length
    this.byteLength = MerkleTree.size(roots)
    this.signature = signature

    return { tree, flushed }
  }

  async commit (state, { signature, keyPair, length = state.length, treeLength = -1, overwrite = false } = {}) {
    assert(this.isDefault() || (this.parent && this.parent.isDefault()), 'Can only commit into default state')

    let srcLocked = false
    await this.mutex.lock()

    try {
      await state.mutex.lock()
      srcLocked = true

      if (treeLength === -1) treeLength = state.flushedLength()

      if (!(await this.core._validateCommit(state, treeLength))) return null
      if (this.length > length) return null

      if (this.length < length && !signature) {
        if (!keyPair) keyPair = this.core.header.keyPair
        const batch = state.createTreeBatch()
        if (length !== batch.length) await batch.restore(length)
        signature = this.core.verifier.sign(batch, keyPair)
      }

      const { tree, flushed } = await this._overwrite(state, this.fork, length, treeLength, signature)

      // gc blocks from source
      if (treeLength < length) {
        const tx = state.createWriteBatch()

        tx.deleteBlockRange(treeLength, length)
        const dependency = state.updateDependency(tx, length)

        await state.flush(tx)

        if (dependency) state.storage.setDependencyHead(dependency)
      }

      const bitfield = { start: treeLength, length: length - treeLength, drop: false }
      this.onappend(tree, bitfield, flushed)

      return {
        length: this.length,
        byteLength: this.byteLength
      }
    } finally {
      this.updating = false
      this.mutex.unlock()

      if (srcLocked) {
        state.mutex.unlock()
        state._clearActiveBatch()
      }

      this.core.checkIfIdle()
    }
  }

  async _getTreeHeadAt (length) {
    if (length === null) return this.treeInfo()

    const head = getDefaultTree()

    head.length = length

    const roots = await MerkleTree.getRootsFromStorage(this.storage, length)
    const rootHash = crypto.tree(roots)

    head.fork = this.fork
    head.rootHash = rootHash

    if (length === this.length) head.signature = this.signature

    return head
  }

  _moveToCore (core, truncated, appended) {
    const head = this.core.sessionStates.pop()
    if (head !== this) this.core.sessionStates[(head.index = this.index)] = head

    this.core = core
    this.index = this.core.sessionStates.push(this) - 1

    for (let i = this.sessions.length - 1; i >= 0; i--) {
      const s = this.sessions[i]
      const manifest = s.manifest
      s.transferSession(this.core)
      if (!manifest && s.manifest) s.emit('manifest')
      if (truncated) s.emit('truncate', truncated.to, truncated.fork)
      if (appended) s.emit('append')
    }
  }

  async moveTo (core, length) {
    const state = core.state

    await this.mutex.lock()

    try {
      // if (state.storage && (await state.storage.resumeSession(this.name)) !== null) {
      //   throw STORAGE_CONFLICT('Batch has already been created')
      // }

      const treeLength = this.length

      let truncated = null
      let appended = false

      if (!this.isSnapshot()) {
        if (this.lingers === null) this.lingers = []
        this.lingers.push(this.storage)

        const resumed = await state.storage.resumeSession(this.name)

        const truncation = length < this.length ? await truncateAndFlush(this, length) : null
        const treeInfo = truncation ? truncation.tree : resumed ? null : await state._getTreeHeadAt(this.length)

        const fork = truncation ? this.fork + 1 : this.fork

        // todo: validate treeInfo

        let storage = null

        if (resumed) {
          storage = resumed
        } else {
          treeInfo.fork = fork
          storage = await state.storage.createSession(this.name, treeInfo)
        }

        const roots = await MerkleTree.getRootsFromStorage(storage, length)

        this.storage = storage
        this.prologue = state.prologue
        this.fork = fork
        this.length = length
        this.byteLength = MerkleTree.size(roots)
        this.roots = roots

        if (truncation) {
          const { dependency } = truncation

          if (dependency) this.storage.setDependencyHead(dependency)
          truncated = { to: treeLength, fork }
        }

        if (this.length > treeLength) {
          appended = true
        }
      }

      for (let i = this.core.sessionStates.length - 1; i >= 0; i--) {
        const state = this.core.sessionStates[i]
        if (state === this) continue
        if (state.name === this.name) state._moveToCore(core.core)
      }

      this._moveToCore(core.core, truncated, appended)
    } finally {
      this.mutex.unlock()
    }
  }

  async createSession (name, overwrite, atom) {
    let storage = null
    let treeInfo = null

    if (!atom && !overwrite && this.storage) {
      storage = await this.storage.resumeSession(name)

      if (storage !== null) treeInfo = (await getCoreHead(storage)) || getDefaultTree()
    }

    const length = treeInfo ? treeInfo.length : this.length

    if (storage === null) {
      treeInfo = await this._getTreeHeadAt(length)

      if (atom) {
        storage = await this.storage.createAtomicSession(atom, treeInfo)
      } else {
        storage = await this.storage.createSession(name, treeInfo)
      }
    }

    if (this.atomized && atom) {
      throw new Error('Session already atomized')
    }

    const head = {
      fork: this.fork,
      roots: length === this.length ? this.roots.slice() : await MerkleTree.getRootsFromStorage(storage, length),
      length,
      prologue: this.prologue,
      signature: length === this.length ? this.signature : null
    }

    const state = new SessionState(
      this.core,
      atom ? this : null,
      storage,
      head,
      atom ? this.name : name
    )

    if (atom) {
      this.atomized = atom
      atom.onflush(state._commit.bind(state))
    }

    return state
  }
}

function noop () {}

function getBitfieldPage (index) {
  return Math.floor(index / Bitfield.BITS_PER_PAGE)
}

function fillBitfieldPage (page, start, end, pageIndex, value) {
  const offset = pageIndex * Bitfield.BITS_PER_PAGE
  const max = offset + Bitfield.BITS_PER_PAGE

  const index = max < end ? max : end

  const from = start - offset
  const to = index - offset

  quickbit.fill(page, value, from, to)

  return index
}

async function storeBitfieldRange (storage, tx, from, to, value) {
  if (from >= to) return

  const firstPage = getBitfieldPage(from)
  const lastPage = getBitfieldPage(to - 1)

  let index = from

  const rx = storage.read()

  const promises = []
  for (let i = firstPage; i <= lastPage; i++) {
    promises.push(rx.getBitfieldPage(i))
  }

  rx.tryFlush()

  const pages = await Promise.all(promises)
  const cnt = lastPage - firstPage + 1

  for (let i = 0; i < cnt; i++) {
    const pageIndex = i + firstPage
    if (!pages[i]) pages[i] = b4a.alloc(Bitfield.BYTES_PER_PAGE)

    index = fillBitfieldPage(pages[i], index, to, pageIndex, value)
    tx.putBitfieldPage(pageIndex, pages[i])
  }
}

async function truncateAndFlush (s, length) {
  const batch = s.createTreeBatch()
  await MerkleTree.truncate(s, length, batch, s.fork)
  const tx = s.createWriteBatch()

  const info = await s._truncate(tx, batch)
  const flushed = await s.flush()

  return {
    tree: info.tree,
    roots: info.roots,
    dependency: info.dependency,
    flushed
  }
}

function updateDependency (state, length, truncated) {
  const i = state.storage.findDependencyIndex(length, truncated)
  if (i === -1) return null // skip default state and overlays of default

  return {
    dataPointer: state.storage.dependencies[i].dataPointer,
    length
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

function getCoreHead (storage) {
  const b = storage.read()
  const p = b.getHead()
  b.tryFlush()
  return p
}
