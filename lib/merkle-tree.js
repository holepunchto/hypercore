const flat = require('flat-tree')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const unslab = require('unslab')
const caps = require('./caps')
const { INVALID_PROOF, INVALID_CHECKSUM, INVALID_OPERATION, BAD_ARGUMENT, ASSERTION } = require('hypercore-errors')

class NodeQueue {
  constructor (nodes, extra = null) {
    this.i = 0
    this.nodes = nodes
    this.extra = extra
    this.length = nodes.length + (this.extra === null ? 0 : 1)
  }

  shift (index) {
    if (this.extra !== null && this.extra.index === index) {
      const node = this.extra
      this.extra = null
      this.length--
      return node
    }

    if (this.i >= this.nodes.length) {
      throw INVALID_OPERATION('Expected node ' + index + ', got (nil)')
    }

    const node = this.nodes[this.i++]
    if (node.index !== index) {
      throw INVALID_OPERATION('Expected node ' + index + ', got node ' + node.index)
    }

    this.length--
    return node
  }
}

class MerkleTreeBatch {
  constructor (session) {
    this.fork = session.fork
    this.roots = [...session.roots]
    this.length = session.length
    this.signature = session.signature
    this.ancestors = session.length
    this.byteLength = session.byteLength
    this.prologue = session.prologue
    this.hashCached = null

    this.committed = false
    this.truncated = false
    this.treeLength = session.length
    this.treeFork = session.fork
    this.storage = session.storage
    this.session = session
    this.nodes = []
    this.upgraded = false
  }

  checkout (length, additionalRoots) {
    const roots = []
    let r = 0

    const head = 2 * length - 2
    const gaps = new Set()
    const all = new Map()

    // additional roots is so the original roots can be passed (we mutate the array in appendRoot)
    if (additionalRoots) {
      for (const node of additionalRoots) all.set(node.index, node)
    }

    for (const node of this.nodes) all.set(node.index, node)

    for (const index of flat.fullRoots(head + 2)) {
      const left = flat.leftSpan(index)
      if (left !== 0) gaps.add(left - 1)

      if (r < this.roots.length && this.roots[r].index === index) {
        roots.push(this.roots[r++])
        continue
      }
      const node = all.get(index)
      if (!node) throw new BAD_ARGUMENT('root missing for given length')
      roots.push(node)
    }

    this.roots = roots
    this.length = length
    this.byteLength = totalSize(roots)
    this.hashCached = null
    this.signature = null

    for (let i = 0; i < this.nodes.length; i++) {
      const index = this.nodes[i].index
      if (index <= head && !gaps.has(index)) continue
      const last = this.nodes.pop()
      if (i < this.nodes.length) this.nodes[i--] = last
    }
  }

  prune (length) {
    if (length === 0) return

    const head = 2 * length - 2
    const gaps = new Set()

    // TODO: make a function for this in flat-tree
    for (const index of flat.fullRoots(head + 2)) {
      const left = flat.leftSpan(index)
      if (left !== 0) gaps.add(left - 1)
    }

    for (let i = 0; i < this.nodes.length; i++) {
      const index = this.nodes[i].index
      if (index > head || gaps.has(index)) continue
      const last = this.nodes.pop()
      if (i < this.nodes.length) this.nodes[i--] = last
    }
  }

  clone () {
    const b = new MerkleTreeBatch(this.session)

    b.fork = this.fork
    b.roots = [...this.roots]
    b.length = this.length
    b.byteLength = this.byteLength
    b.signature = this.signature
    b.treeLength = this.treeLength
    b.treeFork = this.treeFork
    b.tree = this.tree
    b.nodes = [...this.nodes]
    b.upgraded = this.upgraded

    return b
  }

  hash () {
    if (this.hashCached === null) this.hashCached = unslab(crypto.tree(this.roots))
    return this.hashCached
  }

  signable (manifestHash) {
    return caps.treeSignable(manifestHash, this.hash(), this.length, this.fork)
  }

  signableCompat (noHeader) {
    return caps.treeSignableCompat(this.hash(), this.length, this.fork, noHeader)
  }

  get (index) {
    if (index >= this.length * 2) {
      return null
    }

    for (const n of this.nodes) {
      if (n.index === index) return n
    }

    return getTreeNodeFromStorage(this.session.storage, index)
  }

  // deprecated, use sssion proof instead
  proof (batch, { block, hash, seek, upgrade }) {
    return generateProof(this.session, batch, block, hash, seek, upgrade)
  }

  verifyUpgrade (proof) {
    const unverified = verifyTree(proof, this.nodes)

    if (!proof.upgrade) throw INVALID_OPERATION('Expected upgrade proof')

    return verifyUpgrade(proof, unverified, this)
  }

  addNodesUnsafe (nodes) {
    for (let i = 0; i < nodes.length; i++) {
      this.nodes.push(nodes[i])
    }
  }

  append (buf) {
    const head = this.length * 2
    const ite = flat.iterator(head)
    const node = blockNode(head, buf)

    this.appendRoot(node, ite)
  }

  appendRoot (node, ite) {
    node = unslabNode(node)
    this.hashCached = null
    this.upgraded = true
    this.length += ite.factor / 2
    this.byteLength += node.size
    this.roots.push(node)
    this.nodes.push(node)

    while (this.roots.length > 1) {
      const a = this.roots[this.roots.length - 1]
      const b = this.roots[this.roots.length - 2]

      // TODO: just have a peek sibling instead? (pretty sure it's always the left sib as well)
      if (ite.sibling() !== b.index) {
        ite.sibling() // unset so it always points to last root
        break
      }

      const node = unslabNode(parentNode(ite.parent(), a, b))
      this.nodes.push(node)
      this.roots.pop()
      this.roots.pop()
      this.roots.push(node)
    }
  }

  commitable () {
    return this.treeFork === this.session.fork && (
      this.upgraded
        ? this.treeLength === this.session.length
        : this.treeLength <= this.session.length
    )
  }

  commit (tx) {
    if (tx === undefined) throw INVALID_OPERATION('No database batch was passed')
    if (!this.commitable()) throw INVALID_OPERATION('Tree was modified during batch, refusing to commit')

    if (this.upgraded) this._commitUpgrade(tx)

    for (let i = 0; i < this.nodes.length; i++) {
      const node = this.nodes[i]
      tx.putTreeNode(node)
    }

    this.committed = true

    return this
  }

  _commitUpgrade (tx) {
    // TODO: If easy to detect, we should refuse an trunc+append here without a fork id
    // change. Will only happen on user error so mostly to prevent that.

    if (this.ancestors < this.treeLength) {
      tx.deleteTreeNodeRange(this.ancestors * 2, this.treeLength * 2)

      if (this.ancestors > 0) {
        const head = this.ancestors * 2
        const ite = flat.iterator(head - 2)

        while (true) {
          if (ite.contains(head) && ite.index < head) {
            tx.deleteTreeNode(ite.index)
          }
          if (ite.offset === 0) break
          ite.parent()
        }

        this.truncated = true
      }
    }
  }

  seek (bytes, padding) {
    return new ByteSeeker(this, this, bytes, padding)
  }

  byteRange (index) {
    const rx = this.storage.read()
    const range = getByteRange(this, index, rx)
    rx.tryFlush()

    return range
  }

  byteOffset (index) {
    if (index === 2 * this.length) return this.byteLength

    const rx = this.storage.read()
    const offset = getByteOffset(this, index, rx)
    rx.tryFlush()

    return offset
  }

  async restore (length) {
    if (length === this.length) return this

    const roots = unslabNodes(await MerkleTree.getRootsFromStorage(this.storage, length))

    this.roots = roots
    this.length = length
    this.byteLength = totalSize(roots)
    this.ancestors = length

    for (const node of roots) this.byteLength += node.size

    return this
  }
}

class ReorgBatch extends MerkleTreeBatch {
  constructor (session) {
    super(session)

    this.roots = []
    this.length = 0
    this.byteLength = 0
    this.diff = null
    this.ancestors = 0
    // We set upgraded because reorgs are signed so hit will
    // hit the same code paths (like the treeLength check in commit)
    this.upgraded = true
    this.want = {
      nodes: 0,
      start: 0,
      end: 0
    }
  }

  get finished () {
    return this.want === null
  }

  update (proof) {
    if (this.want === null) return true

    const nodes = []
    const root = verifyTree(proof, nodes)

    if (root === null || !b4a.equals(root.hash, this.diff.hash)) return false

    this.nodes.push(...nodes)
    return this._update(nodes)
  }

  async _update (nodes) {
    const n = new Map()
    for (const node of nodes) n.set(node.index, node)

    let diff = null
    const ite = flat.iterator(this.diff.index)
    const startingDiff = this.diff

    while ((ite.index & 1) !== 0) {
      const left = n.get(ite.leftChild())
      if (!left) break

      const existing = await getTreeNodeFromStorage(this.session.storage, left.index)
      if (!existing || !b4a.equals(existing.hash, left.hash)) {
        diff = left
      } else {
        diff = n.get(ite.sibling())
      }
    }

    if ((this.diff.index & 1) === 0) return true
    if (diff === null) return false
    if (startingDiff !== this.diff) return false

    return this._updateDiffRoot(diff)
  }

  _updateDiffRoot (diff) {
    if (this.want === null) return true

    const spans = flat.spans(diff.index)
    const start = spans[0] / 2
    const end = Math.min(this.treeLength, spans[1] / 2 + 1)
    const len = end - start

    this.ancestors = start
    this.diff = diff

    if ((diff.index & 1) === 0 || this.want.start >= this.treeLength || len <= 0) {
      this.want = null
      return true
    }

    this.want.start = start
    this.want.end = end
    this.want.nodes = log2(spans[1] - spans[0] + 2) - 1

    return false
  }
}

class ByteSeeker {
  constructor (session, bytes, padding = 0) {
    this.session = session
    this.bytes = bytes
    this.padding = padding

    const size = session.byteLength - (session.length * padding)

    this.start = bytes >= size ? session.length : 0
    this.end = bytes < size ? session.length : 0
  }

  async _seek (bytes) {
    if (!bytes) return [0, 0]

    for (const node of this.session.roots) { // all async ticks happen once we find the root so safe
      const size = getUnpaddedSize(node, this.padding, null)

      if (bytes === size) return [flat.rightSpan(node.index) + 2, 0]
      if (bytes > size) {
        bytes -= size
        continue
      }

      const ite = flat.iterator(node.index)

      while ((ite.index & 1) !== 0) {
        const l = await getTreeNodeFromStorage(this.session.storage, ite.leftChild())

        if (l) {
          const size = getUnpaddedSize(l, this.padding, ite)

          if (size === bytes) return [ite.rightSpan() + 2, 0]
          if (size > bytes) continue
          bytes -= size
          ite.sibling()
        } else {
          ite.parent()
          return [ite.index, bytes]
        }
      }

      return [ite.index, bytes]
    }

    return null
  }

  async update () { // TODO: combine _seek and this, much simpler
    const res = await this._seek(this.bytes)
    if (!res) return null
    if ((res[0] & 1) === 0) return [res[0] / 2, res[1]]

    const span = flat.spans(res[0])
    this.start = span[0] / 2
    this.end = span[1] / 2 + 1

    return null
  }
}

class TreeProof {
  constructor (session, block, hash, seek, upgrade) {
    this.fork = session.fork
    this.signature = session.signature

    this.block = block
    this.hash = hash
    this.seek = seek
    this.upgrade = upgrade

    this.pending = {
      node: null,
      seek: null,
      upgrade: null,
      additionalUpgrade: null
    }
  }

  async settle () {
    const result = { fork: this.fork, block: null, hash: null, seek: null, upgrade: null, manifest: null }

    const [pNode, pSeek, pUpgrade, pAdditional] = await settleProof(this.pending)

    if (this.block) {
      if (pNode === null) throw INVALID_OPERATION('Invalid block request')
      result.block = {
        index: this.block.index,
        value: null, // populated upstream, alloc it here for simplicity
        nodes: pNode
      }
    } else if (this.hash) {
      if (pNode === null) throw INVALID_OPERATION('Invalid block request')
      result.hash = {
        index: this.hash.index,
        nodes: pNode
      }
    }

    if (this.seek && pSeek !== null) {
      result.seek = {
        bytes: this.seek.bytes,
        nodes: pSeek
      }
    }

    if (this.upgrade) {
      result.upgrade = {
        start: this.upgrade.start,
        length: this.upgrade.length,
        nodes: pUpgrade,
        additionalNodes: pAdditional || [],
        signature: this.signature
      }
    }

    return result
  }
}

class MerkleTree {
  static hash (s) {
    return unslab(crypto.tree(s.roots))
  }

  static signable (s, namespace) {
    return caps.treeSignable(namespace, MerkleTree.hash(s), s.length, s.fork)
  }

  static size (roots) {
    return totalSize(roots)
  }

  static span (roots) {
    return totalSpan(roots)
  }

  static getRoots (session, length) {
    return MerkleTree.getRootsFromStorage(session.storage, length)
  }

  static getRootsFromStorage (storage, length) {
    const indexes = flat.fullRoots(2 * length)
    const roots = new Array(indexes.length)
    const rx = storage.read()

    for (let i = 0; i < indexes.length; i++) {
      roots[i] = getTreeNodeOrError(rx, indexes[i])
    }

    rx.tryFlush()

    return Promise.all(roots)
  }

  static async upgradeable (session, length) {
    const indexes = flat.fullRoots(2 * length)
    const roots = new Array(indexes.length)
    const rx = session.storage.read()

    for (let i = 0; i < indexes.length; i++) {
      roots[i] = rx.getTreeNode(indexes[i])
    }

    rx.tryFlush()

    for (const node of await Promise.all(roots)) {
      if (node === null) return false
    }

    return true
  }

  static seek (session, bytes, padding) {
    return new ByteSeeker(session, bytes, padding)
  }

  static get (session, index) {
    return getTreeNodeFromStorage(session.storage, index)
  }

  static async truncate (session, length, batch, fork = batch.fork) {
    const head = length * 2
    const fullRoots = flat.fullRoots(head)

    for (let i = 0; i < fullRoots.length; i++) {
      const root = fullRoots[i]
      if (i < batch.roots.length && batch.roots[i].index === root) continue

      while (batch.roots.length > i) batch.roots.pop()
      batch.roots.push(unslabNode(await getTreeNodeFromStorageOrError(session.storage, root)))
    }

    while (batch.roots.length > fullRoots.length) {
      batch.roots.pop()
    }

    batch.fork = fork
    batch.length = length
    batch.ancestors = length
    batch.byteLength = totalSize(batch.roots)
    batch.upgraded = true

    return batch
  }

  static async reorg (session, proof, batch) {
    let unverified = null

    if (proof.block || proof.hash || proof.seek) {
      unverified = verifyTree(proof, batch.nodes)
    }

    if (!verifyUpgrade(proof, unverified, batch)) {
      throw INVALID_PROOF('Fork proof not verifiable')
    }

    for (const root of batch.roots) {
      const existing = await getTreeNodeFromStorage(session.storage, root.index)
      if (existing && b4a.equals(existing.hash, root.hash)) continue
      batch._updateDiffRoot(root)
      break
    }

    if (batch.diff !== null) {
      await batch._update(batch.nodes)
    } else {
      batch.want = null
      batch.ancestors = batch.length
    }

    return batch
  }

  static verifyFullyRemote (session, proof) {
    // TODO: impl this less hackishly
    const batch = new MerkleTreeBatch(session)

    batch.fork = proof.fork
    batch.roots = []
    batch.length = 0
    batch.ancestors = 0
    batch.byteLength = 0

    let unverified = verifyTree(proof, batch.nodes)

    if (proof.upgrade) {
      if (verifyUpgrade(proof, unverified, batch)) {
        unverified = null
      }
    }

    return batch
  }

  static async verify (session, proof) {
    const batch = new MerkleTreeBatch(session)

    let unverified = verifyTree(proof, batch.nodes)

    if (proof.upgrade) {
      if (verifyUpgrade(proof, unverified, batch)) {
        unverified = null
      }
    }

    if (unverified) {
      const verified = await getTreeNodeFromStorageOrError(session.storage, unverified.index)
      if (!b4a.equals(verified.hash, unverified.hash)) {
        throw INVALID_CHECKSUM('Invalid checksum at node ' + unverified.index)
      }
    }

    return batch
  }

  static proof (session, rx, { block, hash, seek, upgrade }) {
    return generateProof(session, rx, block, hash, seek, upgrade)
  }

  static async missingNodes (session, index, length) {
    const head = 2 * length
    const ite = flat.iterator(index)

    // See iterator.rightSpan()
    const iteRightSpan = ite.index + ite.factor / 2 - 1
    // If the index is not in the current tree, we do not know how many missing nodes there are...
    if (iteRightSpan >= head) return 0

    let cnt = 0
    // TODO: we could prop use a read batch here and do this in blocks of X for perf
    while (!ite.contains(head) && !(await hasTreeNode(session.storage, ite.index))) {
      cnt++
      ite.parent()
    }

    return cnt
  }

  static byteOffset (session, index) {
    return getByteOffsetSession(session, index, null)
  }

  static byteRange (session, index) {
    const rx = session.storage.read()
    const offset = getByteOffsetSession(session, index, rx)
    const size = getNodeSize(index, rx)
    rx.tryFlush()
    return Promise.all([offset, size])
  }
}

module.exports = {
  MerkleTreeBatch,
  ReorgBatch,
  MerkleTree
}

async function getNodeSize (index, rx) {
  return (await getTreeNodeOrError(rx, index)).size
}

async function getByteOffsetSession (session, index, rx) {
  if (index === 2 * session.length) return session.byteLength

  const treeNodes = rx === null
    ? await getByteOffsetBatchFlush(session.roots, index, session.storage.read())
    : await getByteOffsetBatch(session.roots, index, rx)

  let offset = 0
  for (const node of treeNodes) offset += node.size

  return offset
}

async function getByteOffset (tree, index, rx) {
  if (index === 2 * tree.length) return tree.byteLength

  const treeNodes = await getByteOffsetBatch(tree.roots, index, rx)

  let offset = 0
  for (const node of treeNodes) offset += node.size

  return offset
}

function getByteOffsetBatchFlush (roots, index, rx) {
  const treeNodes = getByteOffsetBatch(roots, index, rx)
  rx.tryFlush()
  return treeNodes
}

function getByteOffsetBatch (roots, index, rx) {
  if ((index & 1) === 1) index = flat.leftSpan(index)

  let head = 0

  const promises = []

  for (const node of roots) { // all async ticks happen once we find the root so safe
    head += 2 * ((node.index - head) + 1)

    if (index >= head) {
      promises.push(node.size)
      continue
    }

    const ite = flat.iterator(node.index)

    while (ite.index !== index) {
      if (index < ite.index) {
        ite.leftChild()
      } else {
        promises.push(getTreeNodeOrError(rx, ite.leftChild()))
        ite.sibling()
      }
    }

    return Promise.all(promises)
  }

  throw ASSERTION('Failed to find offset')
}

function getByteRange (tree, index, rx) {
  const head = 2 * tree.length
  if (((index & 1) === 0 ? index : flat.rightSpan(index)) >= head) {
    throw BAD_ARGUMENT('Index is out of bounds')
  }

  const offset = getByteOffset(tree, index, rx)
  const size = getNodeSize(index, rx)

  return Promise.all([offset, size])
}

// All the methods needed for proof verification

function verifyTree ({ block, hash, seek }, nodes) {
  const untrustedNode = block
    ? { index: 2 * block.index, value: block.value, nodes: block.nodes }
    : hash
      ? { index: hash.index, value: null, nodes: hash.nodes }
      : null

  if (untrustedNode === null && (!seek || !seek.nodes.length)) return null

  let root = null

  if (seek && seek.nodes.length) {
    const ite = flat.iterator(seek.nodes[0].index)
    const q = new NodeQueue(seek.nodes)

    root = q.shift(ite.index)
    nodes.push(root)

    while (q.length > 0) {
      const node = q.shift(ite.sibling())

      root = parentNode(ite.parent(), root, node)
      nodes.push(node)
      nodes.push(root)
    }
  }

  if (untrustedNode === null) return root

  const ite = flat.iterator(untrustedNode.index)
  const blockHash = untrustedNode.value && blockNode(ite.index, untrustedNode.value)

  const q = new NodeQueue(untrustedNode.nodes, root)

  root = blockHash || q.shift(ite.index)
  nodes.push(root)

  while (q.length > 0) {
    const node = q.shift(ite.sibling())

    root = parentNode(ite.parent(), root, node)
    nodes.push(node)
    nodes.push(root)
  }

  return root
}

function verifyUpgrade ({ fork, upgrade }, blockRoot, batch) {
  const prologue = batch.prologue

  if (prologue) {
    const { start, length } = upgrade
    if (start < prologue.length && (start !== 0 || length < prologue.length)) {
      throw INVALID_PROOF('Upgrade does not satisfy prologue')
    }
  }

  const q = new NodeQueue(upgrade.nodes, blockRoot)

  let grow = batch.roots.length > 0
  let i = 0

  const to = 2 * (upgrade.start + upgrade.length)
  const ite = flat.iterator(0)

  for (; ite.fullRoot(to); ite.nextTree()) {
    if (i < batch.roots.length && batch.roots[i].index === ite.index) {
      i++
      continue
    }

    if (grow) {
      grow = false
      const root = ite.index
      if (i < batch.roots.length) {
        ite.seek(batch.roots[batch.roots.length - 1].index)
        while (ite.index !== root) {
          batch.appendRoot(q.shift(ite.sibling()), ite)
        }
        continue
      }
    }

    batch.appendRoot(q.shift(ite.index), ite)
  }

  if (prologue && batch.length === prologue.length) {
    if (!b4a.equals(prologue.hash, batch.hash())) {
      throw INVALID_PROOF('Invalid hash')
    }
  }

  const extra = upgrade.additionalNodes

  ite.seek(batch.roots[batch.roots.length - 1].index)
  i = 0

  while (i < extra.length && extra[i].index === ite.sibling()) {
    batch.appendRoot(extra[i++], ite)
  }

  while (i < extra.length) {
    const node = extra[i++]

    while (node.index !== ite.index) {
      if (ite.factor === 2) throw INVALID_OPERATION('Unexpected node: ' + node.index)
      ite.leftChild()
    }

    batch.appendRoot(node, ite)
    ite.sibling()
  }

  batch.signature = unslab(upgrade.signature)
  batch.fork = fork

  return q.extra === null
}

async function seekFromHead (session, head, bytes, padding) {
  const roots = flat.fullRoots(head)

  for (let i = 0; i < roots.length; i++) {
    const root = roots[i]
    const node = await getTreeNodeFromStorage(session.storage, root)
    const size = getUnpaddedSize(node, padding, null)

    if (bytes === size) return root
    if (bytes > size) {
      bytes -= size
      continue
    }

    return seekTrustedTree(session, root, bytes, padding)
  }

  return head
}

// trust that bytes are within the root tree and find the block at bytes

async function seekTrustedTree (session, root, bytes, padding) {
  if (!bytes) return root

  const ite = flat.iterator(root)

  while ((ite.index & 1) !== 0) {
    const l = await getTreeNodeFromStorage(session.storage, ite.leftChild())

    if (l) {
      const size = getUnpaddedSize(l, padding, ite)
      if (size === bytes) return ite.index
      if (size > bytes) continue
      bytes -= size
      ite.sibling()
    } else {
      ite.parent()
      return ite.index
    }
  }

  return ite.index
}

// try to find the block at bytes without trusting that is *is* within the root passed

async function seekUntrustedTree (session, root, bytes, padding) {
  const offset = await getByteOffsetSession(session, root, null) - (padding ? padding * flat.leftSpan(root) / 2 : 0)

  if (offset > bytes) throw INVALID_OPERATION('Invalid seek')
  if (offset === bytes) return root

  bytes -= offset

  const node = await getTreeNodeFromStorageOrError(session.storage, root)

  if (getUnpaddedSize(node, padding, null) <= bytes) throw INVALID_OPERATION('Invalid seek')

  return seekTrustedTree(session, root, bytes, padding)
}

// Below is proof production, ie, construct proofs to verify a request
// Note, that all these methods are sync as we can statically infer which nodes
// are needed for the remote to verify given they arguments they passed us

function seekProof (session, rx, seekRoot, root, p) {
  const ite = flat.iterator(seekRoot)

  p.seek = []
  p.seek.push(getTreeNodeOrError(rx, ite.index))

  while (ite.index !== root) {
    ite.sibling()
    p.seek.push(getTreeNodeOrError(rx, ite.index))
    ite.parent()
  }
}

function blockAndSeekProof (session, rx, node, seek, seekRoot, root, p) {
  if (!node) return seekProof(session, rx, seekRoot, root, p)

  const ite = flat.iterator(node.index)

  p.node = []
  if (!node.value) p.node.push(getTreeNodeOrError(rx, ite.index))

  while (ite.index !== root) {
    ite.sibling()

    if (seek && ite.contains(seekRoot) && ite.index !== seekRoot) {
      seekProof(session, rx, seekRoot, ite.index, p)
    } else {
      p.node.push(getTreeNodeOrError(rx, ite.index))
    }

    ite.parent()
  }
}

function upgradeProof (session, rx, node, seek, from, to, subTree, p) {
  if (from === 0) p.upgrade = []

  for (const ite = flat.iterator(0); ite.fullRoot(to); ite.nextTree()) {
    // check if they already have the node
    if (ite.index + ite.factor / 2 < from) continue

    // connect existing tree
    if (p.upgrade === null && ite.contains(from - 2)) {
      p.upgrade = []

      const root = ite.index
      const target = from - 2

      ite.seek(target)

      while (ite.index !== root) {
        ite.sibling()
        if (ite.index > target) {
          if (p.node === null && p.seek === null && ite.contains(subTree)) {
            blockAndSeekProof(session, rx, node, seek, subTree, ite.index, p)
          } else {
            p.upgrade.push(getTreeNodeOrError(rx, ite.index))
          }
        }
        ite.parent()
      }

      continue
    }

    if (p.upgrade === null) {
      p.upgrade = []
    }

    // if the subtree included is a child of this tree, include that one
    // instead of a dup node
    if (p.node === null && p.seek === null && ite.contains(subTree)) {
      blockAndSeekProof(session, rx, node, seek, subTree, ite.index, p)
      continue
    }

    // add root (can be optimised since the root might be in tree.roots)
    p.upgrade.push(getTreeNodeOrError(rx, ite.index))
  }
}

function additionalUpgradeProof (session, rx, from, to, p) {
  if (from === 0) p.additionalUpgrade = []

  for (const ite = flat.iterator(0); ite.fullRoot(to); ite.nextTree()) {
    // check if they already have the node
    if (ite.index + ite.factor / 2 < from) continue

    // connect existing tree
    if (p.additionalUpgrade === null && ite.contains(from - 2)) {
      p.additionalUpgrade = []

      const root = ite.index
      const target = from - 2

      ite.seek(target)

      while (ite.index !== root) {
        ite.sibling()
        if (ite.index > target) {
          p.additionalUpgrade.push(getTreeNodeOrError(rx, ite.index))
        }
        ite.parent()
      }

      continue
    }

    if (p.additionalUpgrade === null) {
      p.additionalUpgrade = []
    }

    // add root (can be optimised since the root is in tree.roots)
    p.additionalUpgrade.push(getTreeNodeOrError(rx, ite.index))
  }
}

function nodesToRoot (index, nodes, head) {
  const ite = flat.iterator(index)

  for (let i = 0; i < nodes; i++) {
    ite.parent()
    if (ite.contains(head)) throw BAD_ARGUMENT('Nodes is out of bounds')
  }

  return ite.index
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

function blockNode (index, value) {
  return { index, size: value.byteLength, hash: crypto.data(value) }
}

function parentNode (index, a, b) {
  return { index, size: a.size + b.size, hash: crypto.parent(a, b) }
}

function log2 (n) {
  let res = 1

  while (n > 2) {
    n /= 2
    res++
  }

  return res
}

function normalizeIndexed (block, hash) {
  if (block) return { value: true, index: block.index * 2, nodes: block.nodes, lastIndex: block.index }
  if (hash) return { value: false, index: hash.index, nodes: hash.nodes, lastIndex: flat.rightSpan(hash.index) / 2 }
  return null
}

async function getTreeNodeOrError (rx, index) {
  const node = await rx.getTreeNode(index)
  if (node === null) throw INVALID_OPERATION('Expected tree node ' + index + ' from storage, got (nil)')
  return node
}

function getTreeNodeFromStorageOrError (storage, index) {
  const rx = storage.read()
  const p = getTreeNodeOrError(rx, index)
  rx.tryFlush()
  return p
}

function getTreeNodeFromStorage (storage, index) {
  const rx = storage.read()
  const node = rx.getTreeNode(index)
  rx.tryFlush()
  return node
}

function hasTreeNode (storage, index) {
  const rx = storage.read()
  const has = rx.hasTreeNode(index)
  rx.tryFlush()
  return has
}

async function settleProof (p) {
  const result = [
    p.node && Promise.all(p.node),
    p.seek && Promise.all(p.seek),
    p.upgrade && Promise.all(p.upgrade),
    p.additionalUpgrade && Promise.all(p.additionalUpgrade)
  ]

  try {
    return await Promise.all(result)
  } catch (err) {
    if (p.node) await Promise.allSettled(p.node)
    if (p.seek) await Promise.allSettled(p.seek)
    if (p.upgrade) await Promise.allSettled(p.upgrade)
    if (p.additionalUpgrade) await Promise.allSettled(p.additionalUpgrade)
    throw err
  }
}

// tree can be either the merkle tree or a merkle tree batch
async function generateProof (session, rx, block, hash, seek, upgrade) {
  // Important that this does not throw inbetween making the promise arrays
  // and finalise being called, otherwise there will be lingering promises in the background

  if (session.prologue && upgrade) {
    upgrade.start = upgrade.start < session.prologue.length ? 0 : upgrade.start
    upgrade.length = upgrade.start < session.prologue.length ? session.prologue.length : upgrade.length
  }

  const head = 2 * session.length
  const from = upgrade ? upgrade.start * 2 : 0
  const to = upgrade ? from + upgrade.length * 2 : head
  const node = normalizeIndexed(block, hash)

  // can't do anything as we have no data...
  if (head === 0) return new TreeProof(session, null, null, null, null)

  if (from >= to || to > head) {
    throw INVALID_OPERATION('Invalid upgrade')
  }
  if (seek && upgrade && node !== null && node.index >= from) {
    throw INVALID_OPERATION('Cannot both do a seek and block/hash request when upgrading')
  }

  let subTree = head

  const p = new TreeProof(session, block, hash, seek, upgrade)

  if (node !== null && (!upgrade || node.lastIndex < upgrade.start)) {
    subTree = nodesToRoot(node.index, node.nodes, to)
    const seekRoot = seek ? await seekUntrustedTree(session, subTree, seek.bytes, seek.padding) : head
    blockAndSeekProof(session, rx, node, seek, seekRoot, subTree, p.pending)
  } else if ((node || seek) && upgrade) {
    subTree = seek ? await seekFromHead(session, to, seek.bytes, seek.padding) : node.index
  }

  if (upgrade) {
    upgradeProof(session, rx, node, seek, from, to, subTree, p.pending)
    if (head > to) additionalUpgradeProof(session, rx, to, head, p.pending)
  }

  return p
}

function getUnpaddedSize (node, padding, ite) {
  return padding === 0 ? node.size : node.size - padding * (ite ? ite.countLeaves() : flat.countLeaves(node.index))
}

function unslabNodes (nodes) {
  for (const node of nodes) unslabNode(node)
  return nodes
}

function unslabNode (node) {
  if (node === null) return node
  node.hash = unslab(node.hash)
  return node
}
