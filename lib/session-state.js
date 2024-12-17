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
  constructor (core, storage, blocks, tree, name) {
    this.core = core
    this.index = this.core.sessionStates.push(this) - 1

    this.storage = storage
    this.name = name
    this.sessions = []

    this.mutex = new Mutex()

    this.blocks = blocks
    this.tree = tree

    this.snapshotCompatLength = this.isSnapshot() ? this.tree.length : -1

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
    if (this.isDefault() || this.isSnapshot()) return this.tree.length
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

  createWriteBatch (atomizer) {
    assert(!this._activeBatch && !this.storage.snapshotted)

    this._activeBatch = this.storage.createWriteBatch(atomizer)
    return this._activeBatch
  }

  _unlock () {
    this._clearActiveBatch()
    this.mutex.unlock()
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

  async setUserData (key, value, atomizer) {
    await this.mutex.lock()

    try {
      const storage = this.createWriteBatch(atomizer)
      storage.setUserData(key, value)

      return await this.flushWriteBatch()
    } finally {
      this._unlock()
    }
  }

  async _verifyBlock (batch, bitfield, value, manifest, from) {
    await this.mutex.lock()

    try {
      const storage = this.createWriteBatch()
      this.updating = true

      if (bitfield) this.blocks.put(storage, bitfield.start, value)

      if (bitfield && this.isDefault()) {
        await storeBitfieldRange(this.storage, this.storage, storage, bitfield.start, bitfield.start + 1, true)
      }

      if (manifest) this.core._setManifest(storage, manifest, null)

      const treeUpdate = batch.commitable() ? batch.commit(storage) : null

      const tree = {
        fork: batch.fork,
        length: batch.length,
        rootHash: batch.hash(),
        signature: batch.signature
      }

      if (batch.upgraded) storage.setCoreHead(tree)

      await this.flushWriteBatch()

      if (treeUpdate) this.tree.onupdate(treeUpdate)

      if (batch.upgraded) this.onappend(tree, bitfield)
    } finally {
      this._clearActiveBatch()
      this.updating = false
      this.mutex.unlock()
    }
  }

  async truncate (length, fork, { signature, keyPair } = {}) {
    if (this.tree.prologue && length < this.tree.prologue.length) {
      throw INVALID_OPERATION('Truncation breaks prologue')
    }

    if (!keyPair && this.isDefault()) keyPair = this.core.header.keyPair

    await this.mutex.lock()

    try {
      const batch = await this.tree.truncate(length, fork)

      if (!signature && keyPair && length > 0) signature = this.core.verifier.sign(batch, keyPair)
      if (signature) batch.signature = signature

      const storage = this.createWriteBatch()

      // upsert compat manifest
      if (this.core.verifier === null && keyPair) this.core._setManifest(storage, null, keyPair)

      const { dependency, tree, treeUpdate } = await this._truncate(storage, batch)

      await this.flushWriteBatch()

      if (dependency) this.storage.updateDependencies(dependency.length)
      this.tree.onupdate(treeUpdate)

      this.ontruncate(tree, tree.length, batch.treeLength)
    } finally {
      this._unlock()
    }
  }

  async reorg (batch) {
    await this.mutex.lock()

    const storage = this.createWriteBatch()

    try {
      if (!batch.commitable()) return false

      const { dependency, tree, treeUpdate } = await this._truncate(storage, batch)

      await this.flushWriteBatch()

      if (dependency) this.storage.updateDependencies(dependency.length)
      this.tree.onupdate(treeUpdate)

      this.ontruncate(tree, batch.ancestors, batch.treeLength)
    } finally {
      this._unlock()
    }
  }

  async _truncate (storage, batch) {
    storage.deleteBlockRange(batch.ancestors, batch.treeLength)

    const treeUpdate = batch.commitable() ? batch.commit(storage) : null

    const tree = {
      fork: batch.fork,
      length: batch.length,
      rootHash: batch.hash(),
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
    await this.mutex.lock()

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
      this._unlock()
    }
  }

  async append (values, { signature, keyPair, preappend, atomizer } = {}) {
    if (!keyPair && this.isDefault()) keyPair = this.core.header.keyPair

    if (atomizer) atomizer.enter()
    await this.mutex.lock()

    try {
      const storage = this.createWriteBatch(atomizer)
      if (atomizer) await atomizer.exit()

      // upsert compat manifest
      if (this.core.verifier === null && keyPair) this.core._setManifest(storage, null, keyPair)

      if (preappend) await preappend(values)

      if (!values.length) {
        await this.flushWriteBatch()
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

      const treeUpdate = batch.commitable() ? batch.commit(storage) : null

      const tree = {
        fork: batch.fork,
        length: batch.length,
        rootHash: batch.hash(),
        signature: batch.signature
      }

      storage.setCoreHead(tree)

      if (this.isDefault()) await storeBitfieldRange(this.storage, storage, batch.ancestors, batch.length, true)

      this.blocks.putBatch(storage, this.tree.length, values)

      const bitfield = {
        drop: false,
        start: batch.ancestors,
        length: values.length
      }

      await this.flushWriteBatch()

      this.tree.onupdate(treeUpdate)
      this.onappend(tree, bitfield)

      return { length: batch.length, byteLength: batch.byteLength }
    } finally {
      this._unlock()
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

  async _overwrite (source, length, treeLength, signature, isDependent, atomizer) {
    const blockPromises = []
    const treePromises = []
    const rootPromises = []

    const reader = source.storage.createReadBatch()

    for (const root of flat.fullRoots(length * 2)) {
      rootPromises.push(reader.getTreeNode(root))
    }

    for (const index of flat.patch(treeLength * 2, length * 2)) {
      treePromises.push(reader.getTreeNode(index))
    }

    for (let i = treeLength; i < length; i++) {
      treePromises.push(reader.getTreeNode(i * 2))
      treePromises.push(reader.getTreeNode(i * 2 + 1))
      blockPromises.push(reader.getBlock(i))
    }

    reader.tryFlush()

    const blocks = await Promise.all(blockPromises)
    const nodes = await Promise.all(treePromises)
    const roots = await Promise.all(rootPromises)

    if (signature) {
      const batch = this.tree.batch()
      batch.roots = roots
      batch.length = length

      if (!this.core.verifier.verify(batch, signature)) {
        throw INVALID_SIGNATURE('Signature is not valid over committed tree')
      }
    }

    const writer = this.createWriteBatch(atomizer)
    if (atomizer) await atomizer.exit()

    // truncate existing tree
    if (treeLength < this.tree.length) {
      writer.deleteBlockRange(treeLength, this.tree.length)
    }

    for (const node of nodes) {
      if (node !== null) writer.putTreeNode(node)
    }

    for (let i = 0; i < blocks.length; i++) {
      writer.putBlock(i + treeLength, blocks[i])
    }

    const totalLength = Math.max(length, this.tree.length)

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

      if (index < this.tree.length) {
        index = fillBitfieldPage(page, index, this.tree.length, i, false)
      }
    }

    const tree = {
      fork: this.tree.fork,
      length,
      rootHash: crypto.tree(roots),
      signature
    }

    const upgraded = treeLength < this.tree.length || this.tree.length < length

    if (upgraded) writer.setCoreHead(tree)

    const dependency = isDependent ? updateDependency(this, length) : null

    await this.flushWriteBatch()

    if (upgraded) this.tree.setRoots(roots, signature)
    if (dependency) this.storage.updateDependencies(dependency.length)

    return tree
  }

  async overwrite (state, { length = state.tree.length, treeLength = state.flushedLength(), atomizer } = {}) {
    assert(!this.isDefault(), 'Cannot overwrite signed state') // TODO: make this check better

    if (atomizer) atomizer.enter()
    await this.mutex.lock()

    try {
      const origLength = this.tree.length

      const tree = await this._overwrite(state, length, treeLength, null, state === this.core.state, atomizer)

      const bitfield = { start: treeLength, length: tree.length - treeLength, drop: false }

      if (treeLength < origLength) this.ontruncate(tree, treeLength, origLength)
      if (treeLength < tree.length) this.onappend(tree, bitfield)

      return {
        length: this.tree.length,
        byteLength: this.tree.byteLength
      }
    } finally {
      this._clearActiveBatch()
      this.updating = false
      this.mutex.unlock()
    }
  }

  async _getTreeHeadAt (length) {
    const head = getDefaultTree()

    head.length = length

    const roots = await this.tree.getRoots(length)
    const rootHash = crypto.tree(roots)

    head.fork = this.tree.fork
    head.rootHash = rootHash

    return head
  }

  async moveTo (core) {
    const state = core.state

    if (state.storage && (await state.storage.openBatch(this.name)) !== null) {
      throw STORAGE_CONFLICT('Batch has already been created')
    }

    const head = this.core.sessionStates.pop()
    if (head !== this) this.core.sessionStates[(head.index = this.index)] = head

    this.core = core
    this.index = this.core.sessionStates.push(this) - 1

    if (!this.isSnapshot()) {
      const treeInfo = await state._getTreeHeadAt(this.tree.length)
      const prologue = state.tree.prologue

      // todo: validate treeInfo

      this.storage = await state.storage.registerBatch(this.name, treeInfo)
      this.tree = await MerkleTree.open(this.storage, treeInfo.length, { prologue })
    }

    for (const s of this.sessions) s.transferSession(this.core)
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

    if (storage === null) {
      treeInfo = await this._getTreeHeadAt(length === -1 ? this.tree.length : length)

      if (draft !== true) {
        storage = await this.storage.registerBatch(name, treeInfo)
      } else {
        storage = await this.storage.registerOverlay(treeInfo)
      }
    }

    const tree = await MerkleTree.open(storage, treeInfo.length, {
      prologue: this.tree.prologue
    })

    return new SessionState(this.core, storage, this.core.blocks, tree, name)
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
