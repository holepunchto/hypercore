const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const assert = require('nanoassert')
const flat = require('flat-tree')
const quickbit = require('quickbit-universal')

const { STORAGE_CONFLICT, INVALID_OPERATION, INVALID_SIGNATURE } = require('hypercore-errors')

const Mutex = require('./mutex')
const Bitfield = require('./bitfield')
const MerkleTree = require('./merkle-tree')

module.exports = class SessionState {
  constructor (core, storage, blocks, tree, treeInfo, name) {
    this.core = core
    this.index = this.core.sessionStates.push(this) - 1

    this.storage = storage
    this.name = name
    this.sessions = []

    this.mutex = new Mutex()

    this.blocks = blocks
    this.tree = tree

    // merkle state
    this.fork = treeInfo.fork || 0
    this.roots = treeInfo.roots || []
    this.length = treeInfo.roots.length ? totalSpan(this.roots) / 2 : 0
    this.prologue = treeInfo.prologue || null
    this.signature = treeInfo.signature || null

    this.snapshotCompatLength = this.isSnapshot() ? this.length : -1

    this.active = 0

    this._onflush = null
    this._flushing = null
    this._activeBatch = null

    this.ref()
  }

  isSnapshot () {
    return this.storage.snapshotted
  }

  isDefault () {
    return this.core.state === this
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
    return this.storage.dependencyLength()
  }

  unref () {
    if (--this.active > 0) return
    this.destroy()
  }

  ref () {
    this.active++
    return this
  }

  hash () {
    return MerkleTree.hash(this)
  }

  get byteLength () {
    return totalSize(this.roots)
  }

  treeInfo () {
    return {
      fork: this.fork,
      roots: this.roots.slice(),
      length: this.length,
      prologue: this.prologue
    }
  }

  destroy () {
    if (this.index === -1) return

    this.active = 0
    this.storage.destroy()
    this.mutex.destroy(new Error('Closed')).catch(noop)

    const head = this.core.sessionStates.pop()
    if (head !== this) this.core.sessionStates[(head.index = this.index)] = head

    this.index = -1
    this.core.checkIfIdle()
  }

  snapshot () {
    const s = new SessionState(
      this.core,
      this.storage.snapshot(),
      this.blocks,
      this.tree.clone(),
      this.treeInfo(),
      this.name
    )

    return s
  }

  memoryOverlay () {
    const storage = this.storage.createMemoryOverlay()
    const s = new SessionState(
      this.core,
      storage,
      this.blocks,
      this.tree.clone(storage),
      this.treeInfo(),
      this.name
    )

    return s
  }

  updateDependency (storage, length) {
    const dependency = updateDependency(this, length)
    if (dependency) storage.setDataDependency(dependency)

    return dependency
  }

  _clearActiveBatch (err) {
    if (!this._activeBatch) return
    this._activeBatch.destroy()

    if (this._onflush) this._onflush(err)

    this._onflush = null
    this._flushing = null

    this._activeBatch = null
  }

  createWriteBatch (atom) {
    assert(!this._activeBatch && !this.storage.snapshotted)

    this._activeBatch = this.storage.createWriteBatch(atom)
    return this._activeBatch
  }

  _unlock (lock) {
    this._clearActiveBatch()
    lock.release()
    this.core.checkIfIdle()
  }

  async flushWriteBatch () {
    const writer = this._activeBatch
    this._activeBatch = null

    const flushing = writer.flush()

    try {
      if (!this._flushing) this._flushing = flushing

      await flushing
    } finally {
      this._clearActiveBatch()
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

  async setUserData (key, value, atom) {
    const lock = await this.mutex.lock(atom)

    try {
      const storage = this.createWriteBatch(atom)
      storage.setUserData(key, value)

      return await this.flushWriteBatch()
    } finally {
      this._unlock(lock)
    }
  }

  async _verifyBlock (batch, bitfield, value, manifest, from) {
    const lock = await this.mutex.lock()

    try {
      const storage = this.createWriteBatch()
      this.updating = true

      if (bitfield) this.blocks.put(storage, bitfield.start, value)

      if (bitfield && this.isDefault()) {
        await storeBitfieldRange(this.storage, this.storage, storage, bitfield.start, bitfield.start + 1, true)
      }

      if (manifest) this.core._setManifest(storage, manifest, null)

      if (batch.commitable()) {
        batch.commit(storage)

        if (batch.upgraded) {
          this.roots = batch.roots
          this.length = batch.length
          this.fork = batch.fork
          this.signature = batch.signature
        }
      }

      const head = {
        fork: this.fork,
        length: this.length,
        rootHash: this.hash(),
        signature: this.signature
      }

      if (batch.upgraded) storage.setCoreHead(head)

      await this.flushWriteBatch()

      if (batch.upgraded) this.onappend(head, bitfield)
    } finally {
      this._clearActiveBatch()
      this.updating = false
      lock.release()
    }
  }

  async truncate (length, fork, { signature, keyPair } = {}) {
    if (this.prologue && length < this.prologue.length) {
      throw INVALID_OPERATION('Truncation breaks prologue')
    }

    if (!keyPair && this.isDefault()) keyPair = this.core.header.keyPair

    const lock = await this.mutex.lock()

    try {
      const batch = await this.tree.truncate(length, this, fork)

      if (!signature && keyPair && length > 0) signature = this.core.verifier.sign(batch, keyPair)
      if (signature) batch.signature = signature

      const storage = this.createWriteBatch()

      // upsert compat manifest
      if (this.core.verifier === null && keyPair) this.core._setManifest(storage, null, keyPair)

      const { dependency, tree } = await this._truncate(storage, batch)

      await this.flushWriteBatch()

      if (dependency) this.storage.updateDependencies(dependency.length)

      this.ontruncate(tree, tree.length, batch.treeLength)
    } finally {
      this._unlock(lock)
    }
  }

  async reorg (batch) {
    const lock = await this.mutex.lock()

    const storage = this.createWriteBatch()

    try {
      if (!batch.commitable()) return false

      const { dependency, tree } = await this._truncate(storage, batch)

      await this.flushWriteBatch()

      if (dependency) this.storage.updateDependencies(dependency.length)

      this.ontruncate(tree, batch.ancestors, batch.treeLength)
    } finally {
      this._unlock(lock)
    }
  }

  async _truncate (storage, batch) {
    storage.deleteBlockRange(batch.ancestors, batch.treeLength)

    const treeUpdate = batch.commitable() ? batch.commit(storage) : null

    this.fork = batch.fork
    this.length = batch.length
    this.roots = batch.roots
    this.signature = batch.signature

    const tree = {
      fork: this.fork,
      length: this.length,
      rootHash: this.hash(),
      signature: batch.signature
    }

    if (tree) storage.setCoreHead(tree)

    const truncated = batch.length < this.flushedLength()
    const dependency = truncated ? updateDependency(this, batch.length) : null

    if (this.isDefault()) {
      await storeBitfieldRange(this.storage, storage, batch.ancestors, batch.treeLength, false)
    }

    return { dependency, tree, treeUpdate }
  }

  async clear (start, end, cleared) {
    const lock = await this.mutex.lock()

    try {
      const storage = this.createWriteBatch()

      if (this.isDefault()) await storeBitfieldRange(this.storage, storage, start, end, false)

      this.blocks.clear(storage, start, end)

      const dependency = start < this.flushedLength() ? updateDependency(this, start) : null

      await this.flushWriteBatch()

      if (dependency) this.storage.updateDependencies(dependency.length)

      if (this.isDefault()) {
        const length = end - start
        this.core.updateContiguousLength({ start, length, drop: true })
        this.core._setBitfieldRanges(start, end, false)
        this.core.replicator.onhave(start, length, true)
      }
    } finally {
      this._unlock(lock)
    }
  }

  async append (values, { signature, keyPair, preappend, atom } = {}) {
    if (!keyPair && this.isDefault()) keyPair = this.core.header.keyPair

    const lock = await this.mutex.lock(atom)

    try {
      const storage = this.createWriteBatch(atom)

      // upsert compat manifest
      if (this.core.verifier === null && keyPair) this.core._setManifest(storage, null, keyPair)

      if (preappend) await preappend(values)

      if (!values.length) {
        await this.flushWriteBatch()
        return { length: this.length, byteLength: this.byteLength }
      }

      const batch = this.tree.batch(this)
      for (const val of values) batch.append(val)

      // only multisig can have prologue so signature is always present
      if (this.prologue && batch.length < this.prologue.length) {
        throw INVALID_OPERATION('Append is not consistent with prologue')
      }

      if (!signature && keyPair) signature = this.core.verifier.sign(batch, keyPair)
      if (signature) batch.signature = signature

      batch.commit(storage)

      const tree = {
        fork: batch.fork,
        length: batch.length,
        rootHash: batch.hash(),
        signature: batch.signature
      }

      storage.setCoreHead(tree)

      if (this.isDefault()) await storeBitfieldRange(this.storage, storage, batch.ancestors, batch.length, true)

      this.blocks.putBatch(storage, this.length, values)

      const bitfield = {
        drop: false,
        start: batch.ancestors,
        length: values.length
      }

      this.fork = batch.fork
      this.roots = batch.roots
      this.length = batch.length
      this.signature = batch.signature

      await this.flushWriteBatch()

      this.onappend(tree, bitfield)

      return { length: this.length, byteLength: this.byteLength }
    } finally {
      this._unlock(lock)
    }
  }

  onappend (tree, bitfield) {
    if (this.isDefault()) this.core.onappend(tree, bitfield)

    for (let i = this.sessions.length - 1; i >= 0; i--) {
      this.sessions[i].emit('append')
    }
  }

  ontruncate (tree, to, from) {
    if (this.isDefault()) this.core.ontruncate(tree, to, from)

    for (let i = this.sessions.length - 1; i >= 0; i--) {
      this.sessions[i].emit('truncate', to, tree.fork)
    }
  }

  async _overwrite (source, fork, length, treeLength, signature, isDependent, shallow, atom) {
    const blockPromises = []
    const treePromises = []
    const rootPromises = []

    const reader = source.storage.createReadBatch()

    for (const root of flat.fullRoots(length * 2)) {
      rootPromises.push(reader.getTreeNode(root))
    }

    if (shallow !== true) {
      for (const index of flat.patch(treeLength * 2, length * 2)) {
        treePromises.push(reader.getTreeNode(index))
      }

      for (let i = treeLength; i < length; i++) {
        treePromises.push(reader.getTreeNode(i * 2))
        treePromises.push(reader.getTreeNode(i * 2 + 1))
        blockPromises.push(reader.getBlock(i))
      }
    }

    reader.tryFlush()

    const blocks = await Promise.all(blockPromises)
    const nodes = await Promise.all(treePromises)
    const roots = await Promise.all(rootPromises)

    if (this.core.destroyed) throw new Error('Core destroyed')

    if (signature) {
      const batch = this.tree.batch(this)
      batch.roots = roots
      batch.length = length

      if (!this.core.verifier.verify(batch, signature)) {
        throw INVALID_SIGNATURE('Signature is not valid over committed tree')
      }
    }

    const writer = this.createWriteBatch(atom)
    if (atom) await atom.exit()

    // truncate existing tree
    if (treeLength < this.length) {
      writer.deleteBlockRange(treeLength, this.length)
    }

    for (const node of nodes) {
      if (node !== null) writer.putTreeNode(node)
    }

    for (let i = 0; i < blocks.length; i++) {
      writer.putBlock(i + treeLength, blocks[i])
    }

    const totalLength = Math.max(length, this.length)

    const firstPage = getBitfieldPage(treeLength)
    const lastPage = getBitfieldPage(totalLength)

    let index = treeLength

    for (let i = firstPage; i <= lastPage; i++) {
      const page = b4a.alloc(Bitfield.BYTES_PER_PAGE)
      writer.putBitfieldPage(i, page)

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

    this.fork = tree.fork
    this.roots = roots
    this.length = length
    this.signature = signature

    if (upgraded) {
      writer.setCoreHead(tree)
    }

    const dependency = isDependent ? updateDependency(this, length) : null

    await this.flushWriteBatch()

    if (dependency) this.storage.updateDependencies(dependency.length)

    return tree
  }

  async overwrite (state, { length = state.tree.length, treeLength = state.flushedLength(), shallow = false, atom } = {}) {
    assert(!this.isDefault(), 'Cannot overwrite signed state') // TODO: make this check better

    if (atom) atom.enter()
    const lock = await this.mutex.lock(atom)

    try {
      const origLength = this.length
      const fork = treeLength < origLength ? this.fork + 1 : this.fork

      const tree = await this._overwrite(state, fork, length, treeLength, null, state === this.core.state, shallow, atom)

      const bitfield = { start: treeLength, length: tree.length - treeLength, drop: false }

      if (treeLength < origLength) this.ontruncate(tree, treeLength, origLength)
      if (treeLength < tree.length) this.onappend(tree, bitfield)

      return {
        length: this.length,
        byteLength: this.byteLength
      }
    } finally {
      this._clearActiveBatch()
      this.updating = false
      this.mutex.unlock(lock)
    }
  }

  async _getTreeHeadAt (length) {
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

  async moveTo (core, length, { atom } = {}) {
    const state = core.state

    const lock = await this.mutex.lock(atom)

    try {
      if (state.storage && (await state.storage.openBatch(this.name)) !== null) {
        throw STORAGE_CONFLICT('Batch has already been created')
      }

      if (atom) atom.enter()

      const treeLength = this.length

      if (!this.isSnapshot()) {
        const truncation = length < this.length ? await truncateAndFlush(this, length, atom) : null

        const treeInfo = truncation ? truncation.tree : await state._getTreeHeadAt(this.length)

        treeInfo.fork = truncation ? this.fork + 1 : this.fork

        // todo: validate treeInfo

        const registration = state.storage.registerBatch(this.name, treeInfo)

        this.prologue = state.prologue
        this.fork = treeInfo.fork
        this.length = length
        this.roots = await this.tree.getRoots(length)

        if (atom) atom.exit()

        if (truncation) await truncation.flushing

        this.storage = await registration
        this.tree = new MerkleTree(this.storage)

        if (truncation) {
          const { dependency, tree } = truncation

          if (dependency) this.storage.updateDependencies(dependency.length)
          this.ontruncate(tree, tree.length, treeLength)
        }
      }

      for (let i = this.core.sessionStates.length - 1; i >= 0; i--) {
        const state = this.core.sessionStates[i]
        if (state === this) continue
        if (state.name === this.name) state._moveToCore(core)
      }

      this._moveToCore(core)
    } finally {
      lock.release()
    }
  }

  async createSession (name, length, overwrite, draft) {
    let storage = null
    let treeInfo = null

    if (!overwrite && this.storage) {
      storage = await this.storage.openBatch(name)

      if (storage !== null) {
        treeInfo = (await getCoreHead(storage)) || getDefaultTree()
        if (length !== -1 && treeInfo.length !== length) throw STORAGE_CONFLICT('Different batch stored here')
      }
    }

    if (length === -1) length = treeInfo ? treeInfo.length : this.length

    if (storage === null) {
      treeInfo = await this._getTreeHeadAt(length)

      if (draft !== true) {
        storage = await this.storage.registerBatch(name, treeInfo)
      } else {
        storage = await this.storage.registerOverlay(treeInfo)
      }
    }

    const tree = new MerkleTree(storage)

    const head = {
      fork: this.fork,
      roots: length === this.length ? this.roots.slice() : await tree.getRoots(length),
      length,
      prologue: this.prologue
    }

    return new SessionState(this.core, storage, this.core.blocks, tree, head, name)
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

async function storeBitfieldRange (storage, writer, from, to, value) {
  const firstPage = getBitfieldPage(from)
  const lastPage = getBitfieldPage(to)

  let index = from

  const reader = storage.createReadBatch()

  const promises = []
  for (let i = firstPage; i <= lastPage; i++) {
    promises.push(reader.getBitfieldPage(i))
  }

  reader.tryFlush()
  const pages = await Promise.all(promises)

  for (let i = 0; i <= lastPage - firstPage; i++) {
    const pageIndex = i + firstPage
    if (!pages[i]) pages[i] = b4a.alloc(Bitfield.BYTES_PER_PAGE)

    index = fillBitfieldPage(pages[i], index, to, pageIndex, true)
    writer.putBitfieldPage(pageIndex, pages[i])
  }
}

async function truncateAndFlush (s, length, atom) {
  const batch = await s.tree.truncate(length, s, s.tree.fork)
  const storage = s.createWriteBatch(atom)

  const info = await s._truncate(storage, batch)

  return {
    tree: info.tree,
    treeUpdate: info.treeUpdate,
    dependency: info.dependency,
    flushing: s.flushWriteBatch()
  }
}

function updateDependency (state, length) {
  const dependency = state.storage.findDependency(length)
  if (dependency === null) return null // skip default state and overlays of default

  return {
    data: dependency.data,
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
  const b = storage.createReadBatch()
  const p = b.getCoreHead()
  b.tryFlush()
  return p
}

function totalSize (nodes) {
  let s = 0
  for (const node of nodes) s += node.size
  return s
}

function totalSpan (nodes) {
  let s = 0
  for (const node of nodes) s += 2 * ((node.index - s) + 1)
  return s
}
