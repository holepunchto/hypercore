const flat = require('flat-tree')
const crypto = require('hypercore-crypto')
const uint64le = require('uint64le')

const BLANK_HASH = Buffer.alloc(32)

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
      throw new Error('Expected node ' + index + ', got (nil)')
    }

    const node = this.nodes[this.i++]
    if (node.index !== index) {
      throw new Error('Expected node ' + index + ', got node ' + node.index)
    }

    this.length--
    return node
  }
}

class MerkleBatch {
  constructor (tree) {
    this.tree = tree
    this.roots = null
    this.nodes = null
    this.expectedLength = tree.length
    this.length = tree.length
    this.byteLength = tree.byteLength
    this.upgraded = false
  }

  _parent (index, a, b) {
    return { index, size: a.size + b.size, hash: this.tree.crypto.parent(a, b) }
  }

  append (buf) {
    if (this.nodes === null) {
      this.nodes = []
      this.roots = [...this.tree.roots]
      this.upgraded = true
    }

    const head = this.length * 2
    const ite = flat.iterator(head)
    const node = { index: head, size: buf.byteLength, hash: this.tree.crypto.data(buf) }

    this.byteLength += buf.byteLength
    this.length++
    this.roots.push(node)
    this.nodes.push(node)

    while (this.roots.length > 1) {
      const a = this.roots[this.roots.length - 1]
      const b = this.roots[this.roots.length - 2]

      if (ite.sibling() !== b.index) break

      const node = this._parent(ite.parent(), a, b)
      this.nodes.push(node)
      this.roots.pop()
      this.roots.pop()
      this.roots.push(node)
    }
  }

  async truncate (len, force = false) {
    if (len >= this.length && !force) return

    this.roots = []
    this.nodes = []

    const prev = this.tree.roots
    let i = 0

    for (const root of flat.fullRoots(len * 2)) {
      const node = i < prev.length && prev[i].index === root ? prev[i] : await this.tree.get(root)
      this.roots.push(node)
      i++
    }

    if (len) {
      const head = len * 2
      const ite = flat.iterator(2 * (len - 1))

      while (true) {
        if (ite.contains(head) && ite.index < head) {
          this.nodes.push({ index: ite.index, size: 0, hash: BLANK_HASH })
        }
        if (ite.offset === 0) break
        ite.parent()
      }
    }

    this.byteLength = totalSize(this.roots)
    this.length = len
  }

  async verify ({ seek, block, upgrade }) {
    this.nodes = []

    const blockRoot = (seek || block) ? this._verifyBlock(seek, block) : null

    if (!upgrade) {
      await this._verifyBlockRoot(blockRoot)
      return
    }

    if (upgrade.start !== this.tree.length) {
      // TODO: should prob not be an error.
      // TODO: see if block can still be applied (prob can)
      throw new Error('Update was supersucceded by another one')
    }

    this.roots = [...this.tree.roots]

    if (!this._verifyUpgrade(upgrade, blockRoot)) {
      await this._verifyBlockRoot(blockRoot)
    }

    this._verifyAdditionalTree(upgrade)

    this.byteLength = totalSize(this.roots)
    this.length = totalSpan(this.roots) / 2
    this.upgraded = this.length > this.expectedLength
  }

  commit () {
    if (this.nodes === null) return
    if (this.roots && this.tree.length !== this.expectedLength) {
      // TODO: see if block can still be applied (prob can)
      throw new Error('Update was supersucceded by another one')
    }

    for (const node of this.nodes) this.tree.unflushed.set(node.index, node)

    this.nodes = null

    if (!this.roots) return
    if (this.length < this.tree.length) this.tree.untruncted = true
    this.tree.roots = this.roots
    this.tree.length = this.length
    this.tree.byteLength = this.byteLength
  }

  _verifyBlock (seek, block) {
    if (!block && (!seek || !seek.nodes.length)) return null

    let seekRoot = null

    if (seek && seek.nodes.length) {
      const ite = flat.iterator(seek.nodes[0].index)
      const q = new NodeQueue(seek.nodes)

      seekRoot = q.shift(ite.index)
      this.nodes.push(seekRoot)

      while (q.length > 0) {
        const node = q.shift(ite.sibling())

        seekRoot = this._parent(ite.parent(), seekRoot, node)
        this.nodes.push(node)
        this.nodes.push(seekRoot)
      }
    }

    if (!block) return seekRoot

    const ite = flat.iterator(2 * block.index)
    const blockHash = block.value && { index: ite.index, size: block.value.byteLength, hash: this.tree.crypto.data(block.value) }

    const q = new NodeQueue(block.nodes, seekRoot)

    let blockRoot = blockHash || q.shift(ite.index)
    this.nodes.push(blockRoot)

    while (q.length > 0) {
      const node = q.shift(ite.sibling())

      blockRoot = this._parent(ite.parent(), blockRoot, node)
      this.nodes.push(node)
      this.nodes.push(blockRoot)
    }

    return blockRoot
  }

  _pushRoot (node, ite) {
    const a = this.roots[this.roots.length - 1]
    const p = this._parent(ite.parent(), a, node)
    this.nodes.push(node, p)
    this.roots[this.roots.length - 1] = p

    while (this.roots.length > 1) {
      const b = this.roots[this.roots.length - 2]

      if (ite.sibling() !== b.index) {
        ite.sibling()
        break
      }

      const a = this.roots.pop()
      const p = this.roots[this.roots.length - 1] = this._parent(ite.parent(), a, this.roots[this.roots.length - 1])

      this.nodes.push(p)
    }
  }

  _verifyAdditionalTree (upgrade) {
    let i = 0
    const nodes = upgrade.additionalNodes

    const ite = flat.iterator(this.roots[this.roots.length - 1].index)
    while (i < nodes.length && nodes[i].index === ite.sibling()) {
      this._pushRoot(nodes[i++], ite)
    }

    while (i < nodes.length) {
      const node = nodes[i++]

      while (node.index !== ite.index) {
        if (ite.factor === 2) throw new Error('Unexpected node: ' + node.index)
        ite.leftChild()
      }

      this.nodes.push(node)
      this.roots.push(node)
      ite.sibling()
    }
  }

  async _verifyBlockRoot (blockRoot) {
    if (!(await this.tree.get(blockRoot.index)).hash.equals(blockRoot.hash)) {
      throw new Error('Block root hash mismatch')
    }
  }

  _verifyUpgrade (upgrade, blockRoot) {
    const q = new NodeQueue(upgrade.nodes, blockRoot)

    let grow = this.roots.length > 0
    let i = 0

    const to = upgrade.start + upgrade.length
    for (const root of flat.fullRoots(to * 2)) {
      if (i < this.roots.length && this.roots[i].index === root) {
        i++
        continue
      }

      if (grow) {
        grow = false
        const ite = flat.iterator(this.roots[this.roots.length - 1].index)
        while (ite.index !== root) {
          this._pushRoot(q.shift(ite.sibling()), ite)
        }
        continue
      }

      const node = q.shift(root)
      this.roots.push(node)
      this.nodes.push(node)
    }

    return q.extra === null
  }
}

class ByteSeeker {
  constructor (tree, bytes) {
    this.tree = tree
    this.bytes = bytes
    this.start = bytes >= tree.byteLength ? tree.length : 0
    this.end = bytes < tree.byteLength ? tree.length : -1
  }

  nodes () {
    return this.tree.nodes(this.start * 2)
  }

  async _seek (bytes) {
    if (!bytes) return [0, 0]

    for (const node of this.tree.roots) { // all async ticks happen once we find the root so safe
      if (bytes === node.size) {
        return [flat.rightSpan(node.index) + 2, 0]
      }

      if (bytes > node.size) {
        bytes -= node.size
        continue
      }

      const ite = flat.iterator(node.index)

      while ((ite.index & 1) !== 0) {
        const l = await this.tree.get(ite.leftChild(), false)
        if (l) {
          if (l.size === bytes) return [ite.rightSpan() + 2, 0]
          if (l.size > bytes) continue
          bytes -= l.size
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
  constructor (roots, storage, opts) {
    const len = (opts && opts.length) || 0
    const blen = (opts && opts.byteLength) || 0

    this.crypto = (opts && opts.crypto) || crypto
    this.roots = roots
    this.length = len || (roots.length ? totalSpan(roots) / 2 : 0)
    this.byteLength = blen || totalSize(roots)
    this.untruncted = false
    this.unflushed = new Map()
    this.storage = storage
  }

  static async open (storage, opts) {
    let length = (opts && opts.length)
    if (typeof length !== 'number') length = await autoLength(storage)
    const roots = []
    for (const index of flat.fullRoots(2 * length)) roots.push(await getNode(storage, index, true))
    return new MerkleTree(roots, storage, opts)
  }

  async has (index) {
    return (await this.get(index, false)) === null
  }

  get (index, error = true) {
    const c = this.unflushed.get(index)
    if (c === undefined) return getNode(this.storage, index, error)
    if (c.size !== 0 || BLANK_HASH.compare(c.hash, 0) !== 0) return Promise.resolve(c)
    if (error) throw new Error('Node not stored: ' + index)
    return Promise.resolve(null)
  }

  async byteRange (index) {
    const head = 2 * this.length
    if (((index & 1) === 0 ? index : flat.rightSpan(index)) >= head) {
      throw new Error('Index is out of bounds')
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

    if (head === index) return offset

    throw new Error('Index is out of bounds')
  }

  flush () {
    // TODO: all kinds of optimisations that can be done here, like batching the writes etc...
    // TODO: needs locking...
    return new Promise((resolve, reject) => {
      let error = null
      let missing = this.unflushed.size + 1
      let offset = 0

      const self = this
      const slab = Buffer.allocUnsafe(this.unflushed.size * 40)
      const flushed = []

      for (const node of this.unflushed.values()) {
        flushed.push(node)
        const b = slab.slice(offset, offset += 40)
        uint64le.encode(node.size, b, 0)
        node.hash.copy(b, 8)
        this.storage.write(node.index * 40 + 32, b, done)
      }

      if (this.untruncted) this.storage.del(this.length * 80 + 32, Infinity, done)
      else done(null)

      function done (err) {
        if (err) error = err
        if (--missing > 0) return
        if (error) return reject(error)

        for (const node of flushed) {
          if (self.unflushed.get(node.index) === node) {
            self.unflushed.delete(node.index)
          }
        }

        self.untruncted = false
        resolve()
      }
    })
  }

  maxNodes (index) {
    const head = this.length * 2
    const ite = flat.iterator(index)
    let cnt = 0

    while ((ite.index + ite.factor / 2) < head) {
      cnt++
      ite.parent()
    }

    return cnt - 1
  }

  async nodes (index) {
    const max = this.maxNodes(index)
    const ite = flat.iterator(index)

    let cnt = 0
    while (cnt < max && (await this.get(ite.index, false)) === null) {
      cnt++
      ite.parent()
    }

    return cnt
  }

  _getRoot (index, i) {
    return i < this.roots.length && this.roots[i].index === index
      ? Promise.resolve(this.roots[i])
      : this.get(index)
  }

  async _seekFromHead (head, bytes) {
    const roots = flat.fullRoots(head)

    for (let i = 0; i < roots.length; i++) {
      const root = roots[i]
      const node = await this._getRoot(root, i)

      if (bytes === node.size) return root
      if (bytes > node.size) {
        bytes -= node.size
        continue
      }

      return this._seekTrustedTree(root, bytes)
    }

    return head
  }

  async _seekUntrustedTree (root, bytes) {
    const offset = await this.byteOffset(root)

    if (offset > bytes) throw new Error('Invalid seek')
    if (offset === bytes) return root

    bytes -= offset

    const node = await this.get(root)

    if (node.size <= bytes) throw new Error('Invalid seek')

    return this._seekTrustedTree(root, bytes)
  }

  async _seekTrustedTree (root, bytes) {
    if (!bytes) return root

    const ite = flat.iterator(root)

    while ((ite.index & 1) !== 0) {
      const l = await this.get(ite.leftChild(), false)
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

  async _expandTree (block, seek, from, to, subTree, proof) {
    let nodes = from ? null : []
    let i = 0

    // TODO: all nodes could prob be loaded in parallel?
    for (const ite = flat.iterator(0); ite.fullRoot(to); ite.nextTree()) {
      const idx = i++

      if (ite.index + ite.factor / 2 < from) continue

      if (nodes === null && ite.contains(from - 2)) { // connect existing tree
        nodes = []

        const root = ite.index
        const target = from - 2

        ite.seek(target)

        while (ite.index !== root) {
          ite.sibling()
          if (ite.index > target) {
            if (!proof.block && !proof.seek && ite.contains(subTree)) {
              await this._addBlockAndSeekProof(block, seek, subTree, ite.index, proof)
            } else {
              nodes.push(await this.get(ite.index))
            }
          }
          ite.parent()
        }

        continue
      }

      if (nodes === null) nodes = []

      if (!proof.block && !proof.seek && ite.contains(subTree)) {
        await this._addBlockAndSeekProof(block, seek, subTree, ite.index, proof)
        continue
      }

      nodes.push(await this._getRoot(ite.index, idx))
    }

    return nodes
  }

  async _expandAdditionalTree (from, to) {
    let nodes = from ? null : []
    let i = 0

    // TODO: all nodes could prob be loaded in parallel?
    for (const ite = flat.iterator(0); ite.fullRoot(to); ite.nextTree()) {
      const idx = i++

      if (ite.index + ite.factor / 2 < from) continue

      if (nodes === null && ite.contains(from - 2)) { // connect existing tree
        nodes = []

        const root = ite.index
        const target = from - 2

        ite.seek(target)

        while (ite.index !== root) {
          ite.sibling()
          if (ite.index > target) nodes.push(await this.get(ite.index))
          ite.parent()
        }

        continue
      }

      if (nodes === null) nodes = []

      nodes.push(await this._getRoot(ite.index, idx))
    }

    return nodes
  }

  async _addSeekProof (seekIndex, root, bytes, proof) {
    const ite = flat.iterator(seekIndex)

    proof.seek = { bytes, nodes: [] }
    proof.seek.nodes.push(await this.get(ite.index))

    while (ite.index !== root) {
      ite.sibling()
      proof.seek.nodes.push(await this.get(ite.index))
      ite.parent()
    }
  }

  async _addBlockAndSeekProof (block, seek, seekIndex, root, proof) {
    if (!block) return this._addSeekProof(seekIndex, root, seek.bytes, proof)

    const ite = flat.iterator(2 * block.index)

    proof.block = { index: block.index, value: null, nodes: [] }

    if (!block.value) {
      proof.block.nodes.push(await this.get(ite.index))
    }

    while (ite.index !== root) {
      ite.sibling()

      if (seek && ite.contains(seekIndex) && ite.index !== seekIndex) {
        await this._addSeekProof(seekIndex, ite.index, seek.bytes, proof)
      } else {
        proof.block.nodes.push(await this.get(ite.index))
      }

      ite.parent()
    }
  }

  async _addUpgradeProof (block, seek, upgrade, from, to, head, subTree, proof) {
    proof.upgrade = {
      start: upgrade.start,
      length: upgrade.length,
      nodes: await this._expandTree(block, seek, from, to, subTree, proof),
      additionalNodes: to === head ? [] : await this._expandAdditionalTree(to, head),
      signature: null
    }
    return proof
  }

  async proof ({ seek, block, upgrade }) {
    const head = this.length * 2
    const from = upgrade ? upgrade.start * 2 : head
    const to = upgrade ? from + upgrade.length * 2 : head

    if (seek && !seek.bytes) seek = null
    if (from > head || to > head) throw new Error('Invalid upgrade')

    const proof = {
      seek: null,
      block: null,
      upgrade: null
    }

    // block + (optional seek)
    if (block && (!upgrade || block.index < upgrade.start)) {
      const root = nodesToRoot(2 * block.index, block.nodes, from)
      const seekIndex = seek ? await this._seekUntrustedTree(root, seek.bytes) : head

      await this._addBlockAndSeekProof(block, seek, seekIndex, root, proof)
      if (upgrade) return this._addUpgradeProof(block, seek, upgrade, from, to, head, head, proof)
      return proof
    }

    if ((block || seek) && upgrade) {
      if (seek && block) throw new Error('Can only do a seek or block request when upgrading')

      if (seek) {
        const seekIndex = await this._seekFromHead(to, seek.bytes)
        return this._addUpgradeProof(block, seek, upgrade, from, to, head, seekIndex, proof)
      }

      return this._addUpgradeProof(block, seek, upgrade, from, to, head, 2 * block.index, proof)
    }

    if (upgrade) {
      return this._addUpgradeProof(block, seek, upgrade, from, to, head, head, proof)
    }

    throw new Error('Invalid request')
  }

  seek (bytes) {
    return new ByteSeeker(this, bytes)
  }

  batch () {
    return new MerkleBatch(this)
  }
}

function nodesToRoot (index, nodes, head) {
  const ite = flat.iterator(index)
  for (let i = 0; i < nodes; i++) ite.parent()
  if (ite.contains(head)) throw new Error('Nodes is out of bounds')
  return ite.index
}

async function autoLength (storage) {
  const nodes = await new Promise((resolve) => {
    storage.stat(function (err, st) {
      if (err) return resolve(0)
      if (st.size < 32) return resolve(0)
      const nodes = ((st.size - 32) - ((st.size - 32) % 40)) / 40
      if (!nodes) return nodes
      return resolve(nodes)
    })
  })

  if (!nodes) return 0

  const ite = flat.iterator(nodes - 1)
  let index = nodes - 1
  while (await getNode(storage, ite.parent(), false)) index = ite.index
  return flat.rightSpan(index) / 2 + 1
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

function getNode (storage, index, error) {
  return new Promise((resolve, reject) => {
    storage.read(32 + index * 40, 40, (err, data) => {
      if (err) return error ? reject(err) : resolve(null)
      const size = uint64le.decode(data, 0)
      if (size === 0 && BLANK_HASH.compare(data, 8) === 0) {
        return error ? reject(new Error('Node not stored: ' + index)) : resolve(null)
      }
      resolve({ index, size, hash: data.slice(8) })
    })
  })
}
