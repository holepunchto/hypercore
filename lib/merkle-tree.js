const flat = require('flat-tree')
const crypto = require('hypercore-crypto')
const c = require('compact-encoding')
const Xache = require('xache')
const b4a = require('b4a')
const caps = require('./caps')
const { INVALID_PROOF, INVALID_CHECKSUM, INVALID_OPERATION, BAD_ARGUMENT } = require('hypercore-errors')

const BLANK_HASH = b4a.alloc(32)
const OLD_TREE = b4a.from([5, 2, 87, 2, 0, 0, 40, 7, 66, 76, 65, 75, 69, 50, 98])
const TREE_CACHE = 128 // speeds up linear scans by A LOT

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
  constructor (tree) {
    this.fork = tree.fork
    this.roots = [...tree.roots]
    this.length = tree.length
    this.ancestors = tree.length
    this.byteLength = tree.byteLength
    this.signature = null

    this.treeLength = tree.length
    this.treeFork = tree.fork
    this.tree = tree
    this.nodes = []
    this.upgraded = false
  }

  hash () {
    return this.tree.crypto.tree(this.roots)
  }

  signable (hash = this.hash()) {
    return caps.treeSignable(hash, this.length, this.fork)
  }

  signableLegacy (hash = this.hash()) {
    return caps.treeSignableLegacy(hash, this.length, this.fork)
  }

  append (buf) {
    const head = this.length * 2
    const ite = flat.iterator(head)
    const node = blockNode(this.tree.crypto, head, buf)

    this.appendRoot(node, ite)
  }

  appendRoot (node, ite) {
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

      const node = parentNode(this.tree.crypto, ite.parent(), a, b)
      this.nodes.push(node)
      this.roots.pop()
      this.roots.pop()
      this.roots.push(node)
    }
  }

  commitable () {
    return this.treeFork === this.tree.fork && (
      this.upgraded
        ? this.treeLength === this.tree.length
        : this.treeLength <= this.tree.length
    )
  }

  commit () {
    if (!this.commitable()) throw INVALID_OPERATION('Tree was modified during batch, refusing to commit')

    if (this.upgraded) this._commitUpgrade()

    for (let i = 0; i < this.nodes.length; i++) {
      const node = this.nodes[i]
      this.tree.unflushed.set(node.index, node)
    }
  }

  _commitUpgrade () {
    // TODO: If easy to detect, we should refuse an trunc+append here without a fork id
    // change. Will only happen on user error so mostly to prevent that.

    if (this.ancestors < this.treeLength) {
      if (this.ancestors > 0) {
        const head = 2 * this.ancestors
        const ite = flat.iterator(head - 2)

        while (true) {
          if (ite.contains(head) && ite.index < head) {
            this.tree.unflushed.set(ite.index, blankNode(ite.index))
          }
          if (ite.offset === 0) break
          ite.parent()
        }
      }

      this.tree.truncateTo = this.tree.truncated
        ? Math.min(this.tree.truncateTo, this.ancestors)
        : this.ancestors

      this.tree.truncated = true
      this.tree.cache = new Xache({ maxSize: this.tree.cache.maxSize })
      truncateMap(this.tree.unflushed, this.ancestors)
      if (this.tree.flushing !== null) truncateMap(this.tree.flushing, this.ancestors)
    }

    this.tree.roots = this.roots
    this.tree.length = this.length
    this.tree.byteLength = this.byteLength
    this.tree.fork = this.fork
    this.tree.signature = this.signature
  }

  // TODO: this is the only async method on the batch, so unsure if it should go here
  // this is important so you know where to right data without committing the batch
  // so we'll keep it here for now.

  async byteOffset (index) {
    if (2 * this.tree.length === index) return this.tree.byteLength

    const ite = flat.iterator(index)

    let treeOffset = 0
    let isRight = false
    let parent = null

    for (const node of this.nodes) {
      if (node.index === ite.index) {
        if (isRight && parent) treeOffset += node.size - parent.size
        parent = node
        isRight = ite.isRight()
        ite.parent()
      }
    }

    const r = this.roots.indexOf(parent)
    if (r > -1) {
      for (let i = 0; i < r; i++) {
        treeOffset += this.roots[i].size
      }

      return treeOffset
    }

    const byteOffset = await this.tree.byteOffset(parent ? parent.index : index)

    return byteOffset + treeOffset
  }
}

class ReorgBatch extends MerkleTreeBatch {
  constructor (tree) {
    super(tree)
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
    const root = verifyTree(proof, this.tree.crypto, nodes)

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

      const existing = await this.tree.get(left.index, false)
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
  constructor (tree, bytes, padding = 0) {
    this.tree = tree
    this.bytes = bytes
    this.padding = padding

    const size = tree.byteLength - (tree.length * padding)

    this.start = bytes >= size ? tree.length : 0
    this.end = bytes < size ? tree.length : 0
  }

  nodes () {
    return this.tree.nodes(this.start * 2)
  }

  async _seek (bytes) {
    if (!bytes) return [0, 0]

    for (const node of this.tree.roots) { // all async ticks happen once we find the root so safe
      let size = node.size
      if (this.padding > 0) size -= this.padding * flat.countLeaves(node.index)

      if (bytes === size) return [flat.rightSpan(node.index) + 2, 0]
      if (bytes > size) {
        bytes -= size
        continue
      }

      const ite = flat.iterator(node.index)

      while ((ite.index & 1) !== 0) {
        const l = await this.tree.get(ite.leftChild(), false)
        if (l) {
          let size = l.size
          if (this.padding > 0) size -= this.padding * ite.countLeaves()

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

module.exports = class MerkleTree {
  constructor (storage, roots, fork, signature) {
    this.crypto = crypto
    this.fork = fork
    this.roots = roots
    this.length = roots.length ? totalSpan(roots) / 2 : 0
    this.byteLength = totalSize(roots)
    this.signature = signature

    this.storage = storage
    this.unflushed = new Map()
    this.cache = new Xache({ maxSize: TREE_CACHE })
    this.flushing = null
    this.truncated = false
    this.truncateTo = 0
  }

  addNode (node) {
    if (node.size === 0 && b4a.equals(node.hash, BLANK_HASH)) node = blankNode(node.index)
    this.unflushed.set(node.index, node)
  }

  batch () {
    return new MerkleTreeBatch(this)
  }

  seek (bytes, padding) {
    return new ByteSeeker(this, bytes, padding)
  }

  hash () {
    return this.crypto.tree(this.roots)
  }

  signable (hash = this.hash()) {
    return caps.treeSignable(hash, this.length, this.fork)
  }

  getRoots (length) {
    const indexes = flat.fullRoots(2 * length)
    const roots = new Array(indexes.length)

    for (let i = 0; i < indexes.length; i++) {
      roots[i] = this.get(indexes[i], true)
    }

    return Promise.all(roots)
  }

  async upgradeable (length) {
    const indexes = flat.fullRoots(2 * length)
    const roots = new Array(indexes.length)

    for (let i = 0; i < indexes.length; i++) {
      roots[i] = this.get(indexes[i], false)
    }

    for (const node of await Promise.all(roots)) {
      if (node === null) return false
    }

    return true
  }

  get (index, error = true) {
    const c = this.cache.get(index)
    if (c) return c

    let node = this.unflushed.get(index)

    if (this.flushing !== null && node === undefined) {
      node = this.flushing.get(index)
    }

    // TODO: test this
    if (this.truncated && node !== undefined && node.index >= 2 * this.truncateTo) {
      node = blankNode(index)
    }

    if (node !== undefined) {
      if (node.hash === BLANK_HASH) {
        if (error) throw INVALID_OPERATION('Could not load node: ' + index)
        return Promise.resolve(null)
      }
      return Promise.resolve(node)
    }

    return getStoredNode(this.storage, index, this.cache, error)
  }

  async flush () {
    this.flushing = this.unflushed
    this.unflushed = new Map()

    try {
      if (this.truncated) await this._flushTruncation()
      await this._flushNodes()
    } catch (err) {
      for (const node of this.flushing.values()) {
        if (!this.unflushed.has(node.index)) this.unflushed.set(node.index, node)
      }
      throw err
    } finally {
      this.flushing = null
    }
  }

  _flushTruncation () {
    return new Promise((resolve, reject) => {
      const t = this.truncateTo
      const offset = t === 0 ? 0 : (t - 1) * 80 + 40

      this.storage.truncate(offset, (err) => {
        if (err) return reject(err)

        if (this.truncateTo === t) {
          this.truncateTo = 0
          this.truncated = false
        }

        resolve()
      })
    })
  }

  _flushNodes () {
    // TODO: write neighbors together etc etc
    // TODO: bench loading a full disk page and copy to that instead
    return new Promise((resolve, reject) => {
      const slab = b4a.allocUnsafe(40 * this.flushing.size)

      let error = null
      let missing = this.flushing.size + 1
      let offset = 0

      for (const node of this.flushing.values()) {
        const state = {
          start: 0,
          end: 40,
          buffer: slab.subarray(offset, offset += 40)
        }

        c.uint64.encode(state, node.size)
        c.raw.encode(state, node.hash)

        this.storage.write(node.index * 40, state.buffer, done)
      }

      done(null)

      function done (err) {
        if (err) error = err
        if (--missing > 0) return
        if (error) reject(error)
        else resolve()
      }
    })
  }

  clear () {
    this.cache = new Xache({ maxSize: this.cache.maxSize })
    this.truncated = true
    this.truncateTo = 0
    this.roots = []
    this.length = 0
    this.byteLength = 0
    this.fork = 0
    this.signature = null
    if (this.flushing !== null) this.flushing.clear()
    this.unflushed.clear()
    return this.flush()
  }

  close () {
    return new Promise((resolve, reject) => {
      this.storage.close(err => {
        if (err) reject(err)
        else resolve()
      })
    })
  }

  async truncate (length, fork = this.fork) {
    const head = length * 2
    const batch = new MerkleTreeBatch(this)
    const fullRoots = flat.fullRoots(head)

    for (let i = 0; i < fullRoots.length; i++) {
      const root = fullRoots[i]
      if (i < batch.roots.length && batch.roots[i].index === root) continue

      while (batch.roots.length > i) batch.roots.pop()
      batch.roots.push(await this.get(root))
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

  async reorg (proof) {
    const batch = new ReorgBatch(this)

    let unverified = null

    if (proof.block || proof.hash || proof.seek) {
      unverified = verifyTree(proof, this.crypto, batch.nodes)
    }

    if (!verifyUpgrade(proof, unverified, batch)) {
      throw INVALID_PROOF('Fork proof not verifiable')
    }

    for (const root of batch.roots) {
      const existing = await this.get(root.index, false)
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

  verifyFullyRemote (proof) {
    // TODO: impl this less hackishly
    const batch = new MerkleTreeBatch(this)

    batch.fork = proof.fork
    batch.roots = []
    batch.length = 0
    batch.ancestors = 0
    batch.byteLength = 0

    let unverified = verifyTree(proof, this.crypto, batch.nodes)

    if (proof.upgrade) {
      if (verifyUpgrade(proof, unverified, batch)) {
        unverified = null
      }
    }

    return batch
  }

  async verify (proof) {
    const batch = new MerkleTreeBatch(this)

    let unverified = verifyTree(proof, this.crypto, batch.nodes)

    if (proof.upgrade) {
      if (verifyUpgrade(proof, unverified, batch)) {
        unverified = null
      }
    }

    if (unverified) {
      const verified = await this.get(unverified.index)
      if (!b4a.equals(verified.hash, unverified.hash)) {
        throw INVALID_CHECKSUM('Invalid checksum at node ' + unverified.index)
      }
    }

    return batch
  }

  async proof ({ block, hash, seek, upgrade }) {
    // Important that this does not throw inbetween making the promise arrays
    // and finalise being called, otherwise there will be lingering promises in the background

    const fork = this.fork
    const signature = this.signature
    const head = 2 * this.length
    const from = upgrade ? upgrade.start * 2 : 0
    const to = upgrade ? from + upgrade.length * 2 : head
    const node = normalizeIndexed(block, hash)

    if (from >= to || to > head) {
      throw INVALID_OPERATION('Invalid upgrade')
    }
    if (seek && upgrade && node !== null && node.index >= from) {
      throw INVALID_OPERATION('Cannot both do a seek and block/hash request when upgrading')
    }

    let subTree = head

    const p = {
      node: null,
      seek: null,
      upgrade: null,
      additionalUpgrade: null
    }

    if (node !== null && (!upgrade || node.lastIndex < upgrade.start)) {
      subTree = nodesToRoot(node.index, node.nodes, to)
      const seekRoot = seek ? await seekUntrustedTree(this, subTree, seek.bytes) : head
      blockAndSeekProof(this, node, seek, seekRoot, subTree, p)
    } else if ((node || seek) && upgrade) {
      subTree = seek ? await seekFromHead(this, to, seek.bytes) : node.index
    }

    if (upgrade) {
      upgradeProof(this, node, seek, from, to, subTree, p)
      if (head > to) additionalUpgradeProof(this, to, head, p)
    }

    const [pNode, pSeek, pUpgrade, pAdditional] = await settleProof(p)
    const result = { fork, block: null, hash: null, seek: null, upgrade: null }

    if (block) {
      result.block = {
        index: block.index,
        value: null, // populated upstream, alloc it here for simplicity
        nodes: pNode
      }
    } else if (hash) {
      result.hash = {
        index: hash.index,
        nodes: pNode
      }
    }

    if (seek && pSeek !== null) {
      result.seek = {
        bytes: seek.bytes,
        nodes: pSeek
      }
    }

    if (upgrade) {
      result.upgrade = {
        start: upgrade.start,
        length: upgrade.length,
        nodes: pUpgrade,
        additionalNodes: pAdditional || [],
        signature
      }
    }

    return result
  }

  // Successor to .nodes()
  async missingNodes (index) {
    const head = 2 * this.length
    const ite = flat.iterator(index)

    // See iterator.rightSpan()
    const iteRightSpan = ite.index + ite.factor / 2 - 1
    // If the index is not in the current tree, we do not know how many missing nodes there are...
    if (iteRightSpan >= head) return 0

    let cnt = 0
    while (!ite.contains(head) && (await this.get(ite.index, false)) === null) {
      cnt++
      ite.parent()
    }

    return cnt
  }

  // Deprecated
  async nodes (index) {
    const head = 2 * this.length
    const ite = flat.iterator(index)

    let cnt = 0
    while (!ite.contains(head) && (await this.get(ite.index, false)) === null) {
      cnt++
      ite.parent()
    }

    return cnt
  }

  async byteRange (index) {
    const head = 2 * this.length
    if (((index & 1) === 0 ? index : flat.rightSpan(index)) >= head) {
      throw BAD_ARGUMENT('Index is out of bounds')
    }
    return [await this.byteOffset(index), (await this.get(index)).size]
  }

  async byteOffset (index) {
    if ((index & 1) === 1) index = flat.leftSpan(index)

    let head = 0
    let offset = 0

    for (const node of this.roots) { // all async ticks happen once we find the root so safe
      head += 2 * ((node.index - head) + 1)

      if (index >= head) {
        offset += node.size
        continue
      }

      const ite = flat.iterator(node.index)

      while (ite.index !== index) {
        if (index < ite.index) {
          ite.leftChild()
        } else {
          offset += (await this.get(ite.leftChild())).size
          ite.sibling()
        }
      }

      return offset
    }
  }

  static async open (storage, opts = {}) {
    await new Promise((resolve, reject) => {
      storage.read(0, OLD_TREE.length, (err, buf) => {
        if (err) return resolve()
        if (b4a.equals(buf, OLD_TREE)) return reject(new Error('Storage contains an incompatible merkle tree'))
        resolve()
      })
    })

    const length = typeof opts.length === 'number'
      ? opts.length
      : await autoLength(storage)

    const roots = []
    for (const index of flat.fullRoots(2 * length)) {
      roots.push(await getStoredNode(storage, index, null, true))
    }

    return new MerkleTree(storage, roots, opts.fork || 0, opts.signature || null)
  }
}

// All the methods needed for proof verification

function verifyTree ({ block, hash, seek }, crypto, nodes) {
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

      root = parentNode(crypto, ite.parent(), root, node)
      nodes.push(node)
      nodes.push(root)
    }
  }

  if (untrustedNode === null) return root

  const ite = flat.iterator(untrustedNode.index)
  const blockHash = untrustedNode.value && blockNode(crypto, ite.index, untrustedNode.value)

  const q = new NodeQueue(untrustedNode.nodes, root)

  root = blockHash || q.shift(ite.index)
  nodes.push(root)

  while (q.length > 0) {
    const node = q.shift(ite.sibling())

    root = parentNode(crypto, ite.parent(), root, node)
    nodes.push(node)
    nodes.push(root)
  }

  return root
}

function verifyUpgrade ({ fork, upgrade }, blockRoot, batch) {
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

  batch.signature = upgrade.signature
  batch.fork = fork

  return q.extra === null
}

async function seekFromHead (tree, head, bytes) {
  const roots = flat.fullRoots(head)

  for (let i = 0; i < roots.length; i++) {
    const root = roots[i]
    const node = await tree.get(root)

    if (bytes === node.size) return root
    if (bytes > node.size) {
      bytes -= node.size
      continue
    }

    return seekTrustedTree(tree, root, bytes)
  }

  return head
}

// trust that bytes are within the root tree and find the block at bytes

async function seekTrustedTree (tree, root, bytes) {
  if (!bytes) return root

  const ite = flat.iterator(root)

  while ((ite.index & 1) !== 0) {
    const l = await tree.get(ite.leftChild(), false)
    if (l) {
      if (l.size === bytes) return ite.index
      if (l.size > bytes) continue
      bytes -= l.size
      ite.sibling()
    } else {
      ite.parent()
      return ite.index
    }
  }

  return ite.index
}

// try to find the block at bytes without trusting that is *is* within the root passed

async function seekUntrustedTree (tree, root, bytes) {
  const offset = await tree.byteOffset(root)

  if (offset > bytes) throw INVALID_OPERATION('Invalid seek')
  if (offset === bytes) return root

  bytes -= offset

  const node = await tree.get(root)

  if (node.size <= bytes) throw INVALID_OPERATION('Invalid seek')

  return seekTrustedTree(tree, root, bytes)
}

// Below is proof production, ie, construct proofs to verify a request
// Note, that all these methods are sync as we can statically infer which nodes
// are needed for the remote to verify given they arguments they passed us

function seekProof (tree, seekRoot, root, p) {
  const ite = flat.iterator(seekRoot)

  p.seek = []
  p.seek.push(tree.get(ite.index))

  while (ite.index !== root) {
    ite.sibling()
    p.seek.push(tree.get(ite.index))
    ite.parent()
  }
}

function blockAndSeekProof (tree, node, seek, seekRoot, root, p) {
  if (!node) return seekProof(tree, seekRoot, root, p)

  const ite = flat.iterator(node.index)

  p.node = []
  if (!node.value) p.node.push(tree.get(ite.index))

  while (ite.index !== root) {
    ite.sibling()

    if (seek && ite.contains(seekRoot) && ite.index !== seekRoot) {
      seekProof(tree, seekRoot, ite.index, p)
    } else {
      p.node.push(tree.get(ite.index))
    }

    ite.parent()
  }
}

function upgradeProof (tree, node, seek, from, to, subTree, p) {
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
            blockAndSeekProof(tree, node, seek, subTree, ite.index, p)
          } else {
            p.upgrade.push(tree.get(ite.index))
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
      blockAndSeekProof(tree, node, seek, subTree, ite.index, p)
      continue
    }

    // add root (can be optimised since the root might be in tree.roots)
    p.upgrade.push(tree.get(ite.index))
  }
}

function additionalUpgradeProof (tree, from, to, p) {
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
          p.additionalUpgrade.push(tree.get(ite.index))
        }
        ite.parent()
      }

      continue
    }

    if (p.additionalUpgrade === null) {
      p.additionalUpgrade = []
    }

    // add root (can be optimised since the root is in tree.roots)
    p.additionalUpgrade.push(tree.get(ite.index))
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

function blockNode (crypto, index, value) {
  return { index, size: value.byteLength, hash: crypto.data(value) }
}

function parentNode (crypto, index, a, b) {
  return { index, size: a.size + b.size, hash: crypto.parent(a, b) }
}

function blankNode (index) {
  return { index, size: 0, hash: BLANK_HASH }
}

// Storage methods

function getStoredNode (storage, index, cache, error) {
  return new Promise((resolve, reject) => {
    storage.read(40 * index, 40, (err, data) => {
      if (err) {
        if (error) return reject(err)
        else resolve(null)
        return
      }

      const hash = data.subarray(8)
      const size = c.decode(c.uint64, data)

      if (size === 0 && b4a.compare(hash, BLANK_HASH) === 0) {
        if (error) reject(new Error('Could not load node: ' + index))
        else resolve(null)
        return
      }

      const node = { index, size, hash }
      if (cache !== null) cache.set(index, node)
      resolve(node)
    })
  })
}

function storedNodes (storage) {
  return new Promise((resolve) => {
    storage.stat((_, st) => {
      if (!st) return resolve(0)
      resolve((st.size - (st.size % 40)) / 40)
    })
  })
}

async function autoLength (storage) {
  const nodes = await storedNodes(storage)
  if (!nodes) return 0
  const ite = flat.iterator(nodes - 1)
  let index = nodes - 1
  while (await getStoredNode(storage, ite.parent(), null, false)) index = ite.index
  return flat.rightSpan(index) / 2 + 1
}

function truncateMap (map, len) {
  for (const node of map.values()) {
    if (node.index >= 2 * len) map.delete(node.index)
  }
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
