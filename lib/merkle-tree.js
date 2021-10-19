const flat = require('flat-tree')

module.exports = class MerkleTree {
  constructor (crypto) {
    this.storage = new Map()
    this.roots = []
    this.head = 0
    this.crypto = crypto
  }

  fullRoots (head) {
    if (this.head === head) return [...this.roots]

    const indexes = flat.fullRoots(head)
    const roots = new Array(indexes.length)
    for (let i = 0; i < roots.length; i++) roots[i] = this.get(indexes[i])
    return roots
  }

  root (index) {
    const ite = flat.iterator(index)
    while (!this.has(ite.index) && (ite.index + ite.factor / 2) < this.head) ite.parent()
    return ite.index
  }

  _nodes (index, root, value) {
    const ite = flat.iterator(root)
    const nodes = []

    if (!ite.contains(index)) throw new Error('Bad block root')

    while (ite.index !== index) {
      if (index < ite.index) ite.rightChild()
      else ite.leftChild()

      nodes.push(this.get(ite.index))
      ite.sibling()
    }

    if (value === false) {
      nodes.push(this.get(index))
    }

    return nodes.reverse()
  }

  verify ({ block, upgrade }) {
    let blockVerified = false
    let blockRoot = null
    const blockNodes = []

    if (block) {
      let i = 0
      const ite = flat.iterator(2 * block.index)

      blockRoot = block.nodes[i++] // TODO: handle value = buf
      if (blockRoot.index !== ite.index) throw new Error('Unexpected node')

      blockNodes.push(blockRoot)
      while (i < block.nodes.length) {
        const node = block.nodes[i++]
        if (node.index !== ite.sibling()) throw new Error('Expected sibling')
        blockNodes.push(node)
        blockRoot = { index: ite.parent(), hash: this.crypto.parent(blockRoot, node), size: blockRoot.size + node.size }
        blockNodes.push(blockRoot)
      }
    }

    const roots = []
    const upgradeNodes = []
    if (upgrade) {
      if (2 * upgrade.from !== this.head) return 'partial'

      let i = 0
      const from = this.head
      const to = upgrade.to * 2

      let end = 0
      let upgradedExisting = this.roots.length === 0
      let j = 0

      for (const root of flat.fullRoots(to)) {
        i++
        end += 2 * ((root - end) + 1)

        if (from > end) {
          roots.push(this.roots[i - 1])
          continue
        }

        if (!upgradedExisting) {
          while (this.roots.length > i) {
            console.log('upgrade roots!')
            return
          }
          upgradedExisting = true
          continue
        }

        if (blockRoot && root === blockRoot.index) {
          roots.push(blockRoot)
          blockVerified = true
          continue
        }

        const node = j < upgrade.nodes.length ? upgrade.nodes[j++] : null
        if (node && root === node.index) {
          upgradeNodes.push(node)
          roots.push(node)
          continue
        }

        throw new Error('Missing root: ' + root)
      }

      j = 0
      const ite = flat.iterator(roots[roots.length - 1].index)

      while (j < upgrade.additionalNodes.length) {
        const sibling = ite.sibling()

        if (roots.length > 1 && roots[roots.length - 2].index === sibling) {
          const a = roots.pop()
          const b = roots.pop()
          const p = { index: ite.parent(), hash: this.crypto.parent(a, b), size: a.size + b.size }
          upgradeNodes.push(p)
          roots.push(p)
          continue
        }

        const node = upgrade.additionalNodes[j++]
        if (node && node.index === sibling) {
          const a = roots.pop()
          const p = { index: ite.parent(), hash: this.crypto.parent(a, node), size: a.size + node.size }
          upgradeNodes.push(node)
          upgradeNodes.push(p)
          roots.push(p)
          continue
        }

        ite.sibling()
        ite.next()

        while (ite.index !== node.index && ite.factor !== 2) ite.leftChild()

        if (node.index !== ite.index) throw new Error('Unexpected node')

        upgradeNodes.push(node)
        roots.push(node)
      }
    }

    if (!blockVerified && blockRoot) {
      const node = this.get(blockRoot.index)
      if (!node.hash.equals(blockRoot.hash)) {
        throw new Error('Checksum mismatch')
      }
    }

    if (roots.length) { // verify them!
      this.roots = roots
      this.head = flat.rightSpan(roots[roots.length - 1].index) + 2 // TODO: use an iterator
      for (const node of upgradeNodes) this.storage.set(node.index, node)
    }

    for (const node of blockNodes) this.storage.set(node.index, node)
  }

  proof ({ block, upgrade }) {
    const head = this.head
    const index = block ? block.index * 2 : head

    const from = upgrade ? upgrade.from * 2 : 0
    const to = upgrade ? upgrade.to * 2 : 0
    const includesValue = false

    const res = {
      block: block
        ? { index: block.index, value: null, nodes: null }
        : null,
      upgrade: upgrade
        ? { from: upgrade.from, to: upgrade.to, nodes: null, additionalNodes: null, signature: null }
        : null
    }

    if (index < from || !upgrade) {
      res.block.nodes = this._nodes(index, block.trust, includesValue)
    }

    if (res.upgrade === null) return res

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
            if (res.block.nodes === null && ite.contains(index)) res.block.nodes = this._nodes(index, ite.index, includesValue)
            else res.upgrade.nodes.push(this.get(ite.index))
          }

          ite.sibling()
        }

        res.upgrade.nodes.reverse()
        continue
      }

      if (index < end && res.block.nodes === null) {
        res.block.nodes = this._nodes(index, root, includesValue)
        continue
      }

      res.upgrade.nodes.push(i < this.roots.length && this.roots[i].index === root ? this.roots[i] : this.get(root))
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

          if (ite.index > target) res.upgrade.additionalNodes.push(this.get(ite.index))
          ite.sibling()
        }

        res.upgrade.additionalNodes.reverse()
        continue
      }

      res.upgrade.additionalNodes.push(i < this.roots.length && this.roots[i].index === root ? this.roots[i] : this.get(root))
    }

    return res
  }

  append (buf) {
    const ite = flat.iterator(this.head)
    const node = { index: this.head, size: buf.byteLength, hash: this.crypto.data(buf) }

    this.head += 2
    this.roots.push(node)
    this.storage.set(node.index, node)

    while (this.roots.length > 1) {
      const a = this.roots[this.roots.length - 1]
      const b = this.roots[this.roots.length - 2]

      if (ite.sibling() !== b.index) break

      const size = a.size + b.size
      const node = { index: ite.parent(), size, hash: this.crypto.parent(a, b) }
      this.storage.set(node.index, node)
      this.roots.pop()
      this.roots.pop()
      this.roots.push(node)
    }
  }

  has (index) {
    return this.storage.has(index)
  }

  get (index) {
    return this.storage.get(index)
  }

  blockOffset (bytes) {
    if (!bytes) return [0, 0]

    for (const node of this.roots) {
      if (bytes === node.size) {
        return [flat.rightSpan(node.index) + 2, 0]
      }

      if (bytes > node.size) {
        bytes -= node.size
        continue
      }

      const ite = flat.iterator(node.index)

      while ((ite.index & 1) !== 0) {
        const l = this.get(ite.leftChild())
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

  byteOffset (index) {
    if ((index & 1) === 1) index = flat.leftSpan(index)

    let head = 0
    let offset = 0

    for (const node of this.roots) {
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
          const node = this.get(ite.leftChild())
          if (!node) throw new Error('Node not found: ' + ite.index)
          offset += node.size
          ite.sibling()
        }
      }

      return offset
    }

    if (head === index) return offset
    throw new Error('Index is out of bounds')
  }
}
