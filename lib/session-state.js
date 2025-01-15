const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const assert = require('nanoassert')
const flat = require('flat-tree')
const quickbit = require('quickbit-universal')

const { STORAGE_CONFLICT, INVALID_OPERATION, INVALID_SIGNATURE } = require('hypercore-errors')

const Mutex = require('./mutex')
const Bitfield = require('./bitfield')
const { MerkleTree, MerkleTreeBatch } = require('./merkle-tree')

module.exports = class SessionState {
  constructor (core, parent, storage, blocks, tree, treeInfo, name) {
    this.core = core
    this.index = this.core.sessionStates.push(this) - 1

    this.storage = storage
    this.name = name
    this.sessions = []

    this.parent = parent
    this.mutex = new Mutex()

    this.blocks = blocks
    this.tree = tree

    // merkle state
    this.roots = []
    this.length = 0
    this.fork = treeInfo.fork || 0
    this.prologue = treeInfo.prologue || null
    this.signature = treeInfo.signature || null
    this.parentLength = 0

    const deps = this.storage.dependencies
    this.dependencyLength = deps.length ? deps[deps.length - 1].length : Infinity

    if (treeInfo.roots.length) this.setRoots(treeInfo.roots)
    if (parent) this.parentLength = this.length

    this.snapshotCompatLength = this.isSnapshot() ? this.length : -1

    this.active = 0

    this._onflush = null
    this._flushing = null
    this._activeTx = null
    this._pendingBitfield = null

    this.ref()
  }

  isSnapshot () {
    return this.storage.snapshotted
  }

  isDefault () {
    return this.core.state === this
  }

  createTreeBatch () {
    return new MerkleTreeBatch(this.tree, this)
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
    return this.dependencyLength
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
  }

  get byteLength () {
    return MerkleTree.size(this.roots)
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

    const closing = this.storage.close()

    const head = this.core.sessionStates.pop()
    if (head !== this) this.core.sessionStates[(head.index = this.index)] = head

    this.index = -1
    this.core.checkIfIdle()

    return closing
  }

  snapshot () {
    const s = new SessionState(
      this.core,
      null,
      this.storage.snapshot(),
      this.blocks,
      this.tree.clone(),
      this.treeInfo(),
      this.name
    )

    return s
  }

  updateDependency (storage, length) {
    const dependency = updateDependency(this, length)
    if (dependency) {
      this.dependencyLength = dependency.length
      storage.setDependency(dependency)
    }

    return dependency
  }

  _clearActiveBatch (err) {
    if (!this._activeTx) return
    this._activeTx = null

    if (this._onflush) this._onflush(err)

    this._onflush = null
    this._flushing = null

    this._activeTx = null
  }

  createWriteBatch () {
    assert(!this._activeTx && !this.storage.snapshotted)

    this._activeTx = this.storage.write()
    return this._activeTx
  }

  _unlock (lock) {
    this._clearActiveBatch()
    this.mutex.unlock()
    this.core.checkIfIdle()
  }

  async flush () {
    const tx = this._activeTx
    this._activeTx = null

    const flushing = tx.flush()

    try {
      if (!this._flushing) this._flushing = flushing

      return flushing
    } finally {
      this._clearActiveBatch()
    }
  }

  _commit () {
    const bitfield = this._pendingBitfield
    this._pendingBitfield = null

    return this.parent._oncommit(this, bitfield)
  }

  async _oncommit (src, bitfield) {
    this.fork = src.fork
    this.length = src.length
    this.roots = src.roots.slice()
    this.signature = src.signature

    const tree = {
      fork: this.fork,
      length: this.length,
      rootHash: this.hash(),
      signature: this.signature
    }

    // handle migration
    if (src.core !== this.core) {
      this.prologue = src.prologue
      this.storage = await src.core.state.storage.resumeSession(this.name)
      this.tree = new MerkleTree(this.storage)

      for (let i = this.core.sessionStates.length - 1; i >= 0; i--) {
        const state = this.core.sessionStates[i]
        if (state === this) continue
        if (state.name === this.name) state._moveToCore(src.core)
      }

      this._moveToCore(src.core)
    }

    if (bitfield && bitfield.drop) {
      this.ontruncate(tree, bitfield.start, bitfield.start + bitfield.length, true)
      return
    }

    // checkout sessions should emit truncate
    if (src.parentLength < this.length) {
      this.ontruncate(tree, src.parentLength, this.length, true)
    }

    this.onappend(tree, bitfield, true)
  }

  flushed () {
    if (!this._activeTx) return

    if (this._flushing) return this._flushing

    this._flushing = new Promise(resolve => {
      this._onflush = resolve
    })

    return this._flushing
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
      const tx = this.createWriteBatch()
      this.updating = true

      if (bitfield) this.blocks.put(tx, bitfield.start, value)

      if (bitfield && this.isDefault()) {
        await storeBitfieldRange(this.storage, tx, bitfield.start, bitfield.start + 1, true)
      }

      if (manifest) this.core._setManifest(tx, manifest, null)

      if (batch.commitable()) {
        batch.commit(tx)
      }

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
        this.fork = batch.fork
        this.signature = batch.signature

        this.onappend(head, bitfield, flushed)
      }
    } finally {
      this._clearActiveBatch()
      this.updating = false
      this.mutex.unlock()
    }
  }

  async truncate (length, fork, { signature, keyPair } = {}) {
    if (this.prologue && length < this.prologue.length) {
      throw INVALID_OPERATION('Truncation breaks prologue')
    }

    if (!keyPair && this.isDefault()) keyPair = this.core.header.keyPair

    await this.mutex.lock()

    try {
      const batch = this.createTreeBatch()
      await this.tree.truncate(length, batch, fork)

      if (!signature && keyPair && length > 0) signature = this.core.verifier.sign(batch, keyPair)
      if (signature) batch.signature = signature

      const tx = this.createWriteBatch()

      // upsert compat manifest
      if (this.core.verifier === null && keyPair) this.core._setManifest(tx, null, keyPair)

      const { dependency, tree, roots } = await this._truncate(tx, batch)

      const flushed = await this.flush()

      this.fork = tree.fork
      this.length = tree.length
      this.roots = roots
      this.signature = tree.signature

      if (dependency) this.storage.updateDependencyLength(this.dependencyLength)

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
      this.roots = batch.roots
      this.signature = batch.signature

      if (dependency) this.storage.updateDependencyLength(this.dependencyLength)

      this.ontruncate(tree, batch.ancestors, batch.treeLength, flushed)
    } finally {
      this._unlock()
    }
  }

  async _truncate (storage, batch) {
    storage.deleteBlockRange(batch.ancestors, batch.treeLength)

    if (batch.commitable()) batch.commit(storage)

    const tree = {
      fork: batch.fork,
      length: batch.length,
      rootHash: batch.hash(),
      signature: batch.signature
    }

    if (tree) storage.setHead(tree)

    const truncated = batch.length < this.flushedLength()
    const dependency = truncated ? updateDependency(this, batch.length) : null

    if (dependency) this.dependencyLength = dependency.length

    if (this.isDefault()) {
      await storeBitfieldRange(this.storage, storage, batch.ancestors, batch.treeLength, false)
    }

    return { dependency, tree, roots: batch.roots }
  }

  async clear (start, end, cleared) {
    await this.mutex.lock()

    try {
      const tx = this.createWriteBatch()

      if (this.isDefault()) await storeBitfieldRange(this.storage, tx, start, end, false)

      this.blocks.clear(tx, start, end)

      const dependency = start < this.flushedLength() ? updateDependency(this, start) : null

      if (dependency) this.dependencyLength = dependency.length

      const flushed = await this.flush()

      if (dependency) this.storage.updateDependencyLength(this.dependencyLength)

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

      if (this.isDefault()) await storeBitfieldRange(this.storage, tx, batch.ancestors, batch.length, true)

      this.blocks.putBatch(tx, this.length, values)

      const bitfield = {
        drop: false,
        start: batch.ancestors,
        length: values.length
      }

      const flushed = await this.flush()

      this.fork = batch.fork
      this.roots = batch.roots
      this.length = batch.length
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

    if (!flushed) this._updateBitfield(bitfield)
    else if (this.isDefault()) this.core.ontruncate(tree, bitfield)

    for (let i = this.sessions.length - 1; i >= 0; i--) {
      this.sessions[i].emit('truncate', to, tree.fork)
    }
  }

  _updateBitfield (bitfield, flushed) {
    const p = this._pendingBitfield

    if (p === null) {
      if (bitfield) this._pendingBitfield = bitfield
      return
    }

    const end = bitfield.start + bitfield.length

    if (!p.drop) {
      if (bitfield.drop) throw INVALID_OPERATION('Atomic truncations must be contiguous')
      p.length = bitfield.start + bitfield.length - p.start
      return
    }

    if (p.drop && bitfield.drop) {
      if (p.start !== end) throw INVALID_OPERATION('Atomic truncations must be contiguous')
      p.length += bitfield.length
      p.start = bitfield.start
      return
    }

    if (bitfield.start !== p.start) throw INVALID_OPERATION('Atomic truncations must be contiguous')

    const offset = p.start + p.length

    if (end < offset) {
      p.start = end
    } else {
      p.start = offset
      p.length = end - offset
      p.drop = false
    }
  }

  async _overwrite (source, fork, length, treeLength, signature, isDependent, shallow) {
    const blockPromises = []
    const treePromises = []
    const rootPromises = []

    const rx = source.storage.read()

    for (const root of flat.fullRoots(length * 2)) {
      rootPromises.push(rx.getTreeNode(root))
    }

    if (shallow !== true) {
      for (const index of flat.patch(treeLength * 2, length * 2)) {
        treePromises.push(rx.getTreeNode(index))
      }

      for (let i = treeLength; i < length; i++) {
        treePromises.push(rx.getTreeNode(i * 2))
        treePromises.push(rx.getTreeNode(i * 2 + 1))
        blockPromises.push(rx.getBlock(i))
      }
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

    for (const node of nodes) {
      if (node !== null) tx.putTreeNode(node)
    }

    for (let i = 0; i < blocks.length; i++) {
      tx.putBlock(i + treeLength, blocks[i])
    }

    const totalLength = Math.max(length, this.length)

    const firstPage = getBitfieldPage(treeLength)
    const lastPage = getBitfieldPage(totalLength)

    let index = treeLength

    for (let i = firstPage; i <= lastPage; i++) {
      const page = b4a.alloc(Bitfield.BYTES_PER_PAGE)
      tx.putBitfieldPage(i, page)

      if (index < length) {
        index = fillBitfieldPage(page, index, length, i, true)
        if (index < length) continue
      }

      if (index < this.length) {
        index = fillBitfieldPage(page, index, this.length, i, false)
      }
    }

    const tree = {
      fork,
      length,
      rootHash: crypto.tree(roots),
      signature
    }

    const upgraded = treeLength < this.length || this.length < length || tree.fork !== this.tree.fork

    if (upgraded) tx.setHead(tree)

    const dependency = isDependent ? updateDependency(this, length) : null

    if (dependency) this.dependencyLength = dependency.length

    const flushed = await this.flush()

    this.fork = tree.fork
    this.roots = roots
    this.length = length
    this.signature = signature

    if (dependency) this.storage.updateDependencyLength(this.dependencyLength)

    return { tree, flushed }
  }

  async overwrite (state, { length = state.tree.length, treeLength = state.flushedLength(), shallow = false } = {}) {
    assert(!this.isDefault(), 'Cannot overwrite signed state') // TODO: make this check better

    await this.mutex.lock()

    try {
      const origLength = this.length
      const fork = treeLength < origLength ? this.fork + 1 : this.fork

      const { tree, flushed } = await this._overwrite(state, fork, length, treeLength, null, state === this.core.state, shallow)

      const bitfield = { start: treeLength, length: tree.length - treeLength, drop: false }

      if (treeLength < origLength) this.ontruncate(tree, treeLength, origLength, flushed)
      if (treeLength < tree.length) this.onappend(tree, bitfield, flushed)

      return {
        length: this.length,
        byteLength: this.byteLength
      }
    } finally {
      this._clearActiveBatch()
      this.updating = false
      this.mutex.unlock()
    }
  }

  async commit (state, { signature, keyPair, length = state.length, treeLength = state.flushedLength(), overwrite = false } = {}) {
    assert(this.isDefault() || (this.parent && this.parent.isDefault()), 'Can only commit into default state')

    let srcLocked = false
    await this.mutex.lock()

    try {
      await state.mutex.lock()
      srcLocked = true

      await this.core._validateCommit(state, treeLength)

      if (this.length < length && !signature) {
        if (!keyPair) keyPair = this.core.header.keyPair
        const batch = state.createTreeBatch()
        if (length !== batch.length) await batch.restore(length)
        signature = this.core.verifier.sign(batch, keyPair)
      }

      const { tree, flushed } = await this._overwrite(state, this.fork, length, treeLength, signature, false, false)

      // gc blocks from source
      if (treeLength < length) {
        const tx = state.createWriteBatch()

        state.blocks.clear(tx, treeLength, length)
        const dependency = state.updateDependency(tx, length)

        await state.flush(tx)

        if (dependency) {
          state.storage.updateDependencyLength(dependency.length)
          state.dependencyLength = dependency.length
        }
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

    const roots = await this.tree.getRoots(length)
    const rootHash = crypto.tree(roots)

    head.fork = this.fork
    head.rootHash = rootHash

    return head
  }

  _moveToCore (core) {
    const head = this.core.sessionStates.pop()
    if (head !== this) this.core.sessionStates[(head.index = this.index)] = head

    this.core = core
    this.index = this.core.sessionStates.push(this) - 1

    for (let i = this.sessions.length - 1; i >= 0; i--) this.sessions[i].transferSession(this.core)
  }

  async moveTo (core, length) {
    const state = core.state

    await this.mutex.lock()

    try {
      if (state.storage && (await state.storage.resumeSession(this.name)) !== null) {
        throw STORAGE_CONFLICT('Batch has already been created')
      }

      const treeLength = this.length

      if (!this.isSnapshot()) {
        const truncation = length < this.length ? await truncateAndFlush(this, length) : null

        const treeInfo = truncation ? truncation.tree : await state._getTreeHeadAt(this.length)

        treeInfo.fork = truncation ? this.fork + 1 : this.fork

        // todo: validate treeInfo

        if (!this.storage.atom) {
          this.storage = await state.storage.createSession(this.name, treeInfo)
        } else {
          const s = state.storage.atomize(this.storage.atom)
          this.storage = await s.createSession(this.name, treeInfo)
          await s.close()
        }

        this.tree = new MerkleTree(this.storage)

        this.prologue = state.prologue
        this.fork = treeInfo.fork
        this.length = length
        this.roots = await this.tree.getRoots(length)

        if (truncation) {
          const { dependency, tree, flushed } = truncation

          if (dependency) this.storage.updateDependencyLength(this.dependencyLength)
          this.ontruncate(tree, tree.length, treeLength, flushed)
        }
      }

      if (!this.storage.atom) {
        for (let i = this.core.sessionStates.length - 1; i >= 0; i--) {
          const state = this.core.sessionStates[i]
          if (state === this) continue
          if (state.name === this.name) state._moveToCore(core)
        }
      }

      this._moveToCore(core)
    } finally {
      this.mutex.unlock()
    }
  }

  async createSession (name, length, overwrite, atom) {
    let storage = null
    let treeInfo = null

    if (!atom && !overwrite && this.storage) {
      storage = await this.storage.resumeSession(name)

      if (storage !== null) {
        treeInfo = (await getCoreHead(storage)) || getDefaultTree()
        if (length !== -1 && treeInfo.length !== length) throw STORAGE_CONFLICT('Different batch stored here')
      }
    }

    if (length === -1) length = treeInfo ? treeInfo.length : this.length

    if (storage === null) {
      treeInfo = await this._getTreeHeadAt(length)

      if (atom) {
        storage = await this.storage.createAtomicSession(atom, treeInfo)
      } else {
        storage = await this.storage.createSession(name, treeInfo)
      }
    }

    const tree = new MerkleTree(storage)

    const head = {
      fork: this.fork,
      roots: length === this.length ? this.roots.slice() : await tree.getRoots(length),
      length,
      prologue: this.prologue,
      signature: length === this.length ? this.signature : null
    }

    const state = new SessionState(
      this.core,
      atom ? this : null,
      storage,
      this.core.blocks,
      tree,
      head,
      atom ? this.name : name
    )

    if (atom) atom.onflush(state._commit.bind(state))

    return state
  }
}

function noop () {}

function getBitfieldPage (index) {
  return Math.floor(index / Bitfield.BITS_PER_PAGE)
}

function getBitfieldOffset (index) {
  return index & (Bitfield.BITS_PER_PAGE - 1)
}

function fillBitfieldPage (page, start, end, pageIndex, value) {
  const last = ((pageIndex + 1) * Bitfield.BITS_PER_PAGE) - 1
  const from = getBitfieldOffset(start)

  const index = last < end ? last : end
  const to = getBitfieldOffset(index)

  quickbit.fill(page, value, from, to)

  return index
}

async function storeBitfieldRange (storage, tx, from, to, value) {
  const firstPage = getBitfieldPage(from)
  const lastPage = getBitfieldPage(to)

  let index = from

  const rx = storage.read()

  const promises = []
  for (let i = firstPage; i <= lastPage; i++) {
    promises.push(rx.getBitfieldPage(i))
  }

  rx.tryFlush()
  const pages = await Promise.all(promises)

  for (let i = 0; i <= lastPage - firstPage; i++) {
    const pageIndex = i + firstPage
    if (!pages[i]) pages[i] = b4a.alloc(Bitfield.BYTES_PER_PAGE)

    index = fillBitfieldPage(pages[i], index, to, pageIndex, true)
    tx.putBitfieldPage(pageIndex, pages[i])
  }
}

async function truncateAndFlush (s, length) {
  const batch = s.createTreeBatch()
  await s.tree.truncate(length, batch, s.tree.fork)
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

function updateDependency (state, length) {
  const dependency = findDependency(state.storage, length)
  if (dependency === null) return null // skip default state and overlays of default

  return {
    dataPointer: dependency.dataPointer,
    length
  }
}

function findDependency (storage, length) {
  for (let i = storage.dependencies.length - 1; i >= 0; i--) {
    const dep = storage.dependencies[i]
    if (dep.length < length) return dep
  }

  return null
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
