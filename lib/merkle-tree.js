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

  async verify ({ block, upgrade }) {
    this.nodes = []

    const blockRoot = block ? this._verifyBlock(block) : null

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
    if (this.roots && this.tree.length !== this.expectedLength) {
      // TODO: see if block can still be applied (prob can)
      throw new Error('Update was supersucceded by another one')
    }

    for (const node of this.nodes) this.tree.unflushed.set(node.index, node)

    this.nodes = null

    if (!this.roots) return
    this.tree.roots = this.roots
    this.tree.length = this.length
    this.tree.byteLength = this.byteLength
  }

  _verifyBlock (block) {
    const ite = flat.iterator(2 * block.index)
    const extra = block.value && { index: ite.index, size: block.value.byteLength, hash: this.tree.crypto.data(block.value) }
    const q = new NodeQueue(block.nodes, extra)

    let root = q.shift(ite.index)
    this.nodes.push(root)

    while (q.length > 0) {
      const node = q.shift(ite.sibling())

      root = this._parent(ite.parent(), root, node)
      this.nodes.push(node)
      this.nodes.push(root)
    }

    return root
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

module.exports = class MerkleTree {
  constructor (roots, storage, opts) {
    const len = (opts && opts.length) || 0
    const blen = (opts && opts.byteLength) || 0

    this.crypto = (opts && opts.crypto) || crypto
    this.roots = roots
    this.length = len || (roots.length ? totalSpan(roots) / 2 : 0)
    this.byteLength = blen || totalSize(roots)
    this.unflushed = new Map()
    this.storage = storage
  }

  async has (index) {
    return this.unflushed.has(index) || (await getNode(this.storage, index, false)) !== null
  }

  static async open (storage, opts) {
    let length = (opts && opts.length)
    if (typeof length !== 'number') length = await autoLength(storage)
    const roots = []
    for (const index of flat.fullRoots(2 * length)) roots.push(await getNode(storage, index, true))
    return new MerkleTree(roots, storage, opts)
  }

  get (index) {
    const c = this.unflushed.get(index)
    if (c) return Promise.resolve(c)
    return getNode(this.storage, index, true)
  }

  async blockOffset (bytes) {
    if (!bytes) return [0, 0]

    for (const node of this.roots) { // all async ticks happen once we find the root so safe
      if (bytes === node.size) {
        return [flat.rightSpan(node.index) + 2, 0]
      }

      if (bytes > node.size) {
        bytes -= node.size
        continue
      }

      const ite = flat.iterator(node.index)

      while ((ite.index & 1) !== 0) {
        const l = await this.get(ite.leftChild())
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
    return new Promise((resolve, reject) => {
      let error = null
      let missing = this.unflushed.size + 1
      let offset = 0

      const self = this
      const slab = Buffer.allocUnsafe(this.unflushed.size * 40)

      for (const node of this.unflushed.values()) {
        const b = slab.slice(offset, offset += 40)
        uint64le.encode(node.size, b, 0)
        node.hash.copy(b, 8)
        this.storage.write(node.index * 40 + 32, b, done)
      }

      done(null)

      function done (err) {
        if (err) error = err
        if (--missing > 0) return
        if (error) return reject(error)
        self.unflushed.clear()
        resolve()
      }
    })
  }

  async nodes (index) {
    const head = this.length * 2
    const ite = flat.iterator(index)
    let cnt = 0

    while (!(await this.has(ite.index)) && (ite.index + ite.factor / 2) < head) {
      cnt++
      ite.parent()
    }

    return cnt
  }

  async _blockProof (index, root, value) {
    const ite = flat.iterator(root)
    const nodes = []

    if (!ite.contains(index)) throw new Error('Bad block root')

    while (ite.index !== index) {
      if (index < ite.index) ite.rightChild()
      else ite.leftChild()

      nodes.push(await this.get(ite.index))
      ite.sibling()
    }

    if (value === false) {
      nodes.push(await this.get(index))
    }

    return nodes.reverse()
  }

  async proof ({ block, upgrade }) {
    const head = this.length * 2
    const index = block ? block.index * 2 : head

    const from = upgrade ? upgrade.start * 2 : 0
    const to = upgrade ? (upgrade.start + upgrade.length) * 2 : 0
    const includesValue = block ? !!block.value : false

    const res = {
      block: block
        ? { index: block.index, value: null, nodes: null }
        : null,
      upgrade: upgrade && upgrade.length
        ? { start: upgrade.start, length: upgrade.length, nodes: null, additionalNodes: null, signature: null }
        : null
    }

    if (index < from || !res.upgrade) {
      const ite = flat.iterator(index)
      for (let i = 0; i < block.nodes; i++) ite.parent()
      res.block.nodes = await this._blockProof(index, ite.index, includesValue)
    }

    if (res.upgrade === null) return res
    if (block && block.index >= (upgrade.start + upgrade.length)) throw new Error('Block out of bounds')

    if (!from) res.upgrade.nodes = []

    let end = 0
    let i = 0
    for (const root of flat.fullRoots(to)) {
      end += 2 * ((root - end) + 1)
      i++

      if (from > end) continue // has it

      if (res.upgrade.nodes === null) {
        res.upgrade.nodes = []

        const ite = flat.iterator(root)
        const target = from - 2

        while ((ite.index + ite.factor / 2) > target && ite.factor !== 2) {
          if (target < ite.index) ite.rightChild()
          else ite.leftChild()

          if (ite.index > target) {
            if (res.block && res.block.nodes === null && ite.contains(index)) res.block.nodes = await this._blockProof(index, ite.index, includesValue)
            else res.upgrade.nodes.push(await this.get(ite.index))
          }

          ite.sibling()
        }

        res.upgrade.nodes.reverse()
        continue
      }

      if (index < end && res.block.nodes === null) {
        res.block.nodes = await this._blockProof(index, root, includesValue)
        continue
      }

      res.upgrade.nodes.push(i < this.roots.length && this.roots[i].index === root ? this.roots[i] : await this.get(root))
    }

    if (to >= head) {
      if (!res.upgrade.additionalNodes) res.upgrade.additionalNodes = []
      return res
    }

    end = 0
    i = 0
    for (const root of flat.fullRoots(head)) {
      end += 2 * ((root - end) + 1)
      i++

      if (to > end) continue // has it

      if (res.upgrade.additionalNodes === null) {
        res.upgrade.additionalNodes = []

        const ite = flat.iterator(root)
        const target = to - 2

        while ((ite.index + ite.factor / 2) > target && ite.factor !== 2) {
          if (target < ite.index) ite.rightChild()
          else ite.leftChild()

          if (ite.index > target) res.upgrade.additionalNodes.push(await this.get(ite.index))
          ite.sibling()
        }

        res.upgrade.additionalNodes.reverse()
        continue
      }

      res.upgrade.additionalNodes.push(i < this.roots.length && this.roots[i].index === root ? this.roots[i] : await this.get(root))
    }

    return res
  }

  batch () {
    return new MerkleBatch(this)
  }
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
