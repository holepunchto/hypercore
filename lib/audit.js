const crypto = require('hypercore-crypto')
const flat = require('flat-tree')
const b4a = require('b4a')
const { MerkleTree } = require('./merkle-tree')

module.exports = async function auditCore (core, { tree = true, blocks = true, bitfield = true, dryRun = false } = {}) {
  const length = core.state.length
  const stats = { treeNodes: 0, blocks: 0, bits: 0, droppedTreeNodes: 0, droppedBlocks: 0, droppedBits: 0, corrupt: false }

  // audit the tree
  if (tree) {
    let tx = null

    const roots = await MerkleTree.getRootsFromStorage(core.state.storage, length)
    const stack = []

    for (const r of roots) {
      if (r === null) {
        if (!dryRun) {
          const storage = core.state.storage
          await storage.store.deleteCore(storage.core)
          return null
        }

        stats.corrupt = true
      }

      stack.push(r)
    }

    stats.treeNodes += roots.length

    while (stack.length > 0) {
      const node = stack.pop()

      if ((node.index & 1) === 0) continue

      const [left, right] = flat.children(node.index)

      const rx = core.state.storage.read()
      const leftNodePromise = rx.getTreeNode(left)
      const rightNodePromise = rx.getTreeNode(right)

      rx.tryFlush()

      const [leftNode, rightNode] = await Promise.all([leftNodePromise, rightNodePromise])

      if (isBadTree(node, leftNode, rightNode)) {
        if (!tx && !stats.corrupt) tx = core.state.storage.write()
        const [l, r] = flat.spans(node.index)
        tx.deleteTreeNodeRange(l, r + 1)
        stats.droppedTreeNodes++
        continue
      }

      if (!leftNode) continue

      stats.treeNodes += 2
      stack.push(leftNode, rightNode)
    }

    if (tx && !dryRun) await tx.flush()
  }

  // audit the blocks
  if (blocks) {
    let tx = null

    for await (const block of core.state.storage.createBlockStream()) {
      if (!core.bitfield.get(block.index)) {
        if (!tx && !stats.corrupt) tx = core.state.storage.write()
        tx.deleteBlock(block.index)
        stats.droppedBlocks++
      }

      const rx = core.state.storage.read()
      const treeNodePromise = rx.getTreeNode(2 * block.index)

      rx.tryFlush()

      const treeNode = await treeNodePromise

      if (isBadBlock(treeNode, block.value)) {
        if (!tx && !stats.corrupt) tx = core.state.storage.write()
        tx.deleteBlock(block.index)
        stats.droppedBlocks++
        continue
      }

      stats.blocks++
    }

    if (tx && !dryRun) await tx.flush()
  }

  if (bitfield) {
    let tx = null

    for (const index of allBits(core.bitfield)) {
      const rx = core.state.storage.read()
      const blockPromise = rx.getBlock(index)

      rx.tryFlush()

      const block = await blockPromise
      if (!block) {
        stats.droppedBits++
        if (dryRun) continue

        if (!tx && !stats.corrupt) tx = core.state.storage.write()

        core.bitfield.set(index, false)

        const page = core.bitfield.getBitfield(index)
        if (page.bitfield) tx.setBitfieldPage(page.index, page.bitfield)
        else tx.deleteBitfieldPage(page.idnex)
        continue
      }

      stats.bits++
    }

    if (tx && !dryRun) await tx.flush()
  }

  return stats
}

function isBadBlock (node, block) {
  if (!node) return true
  const hash = crypto.data(block)
  return !b4a.equals(hash, node.hash) || node.size !== block.byteLength
}

function isBadTree (parent, left, right) {
  if (!left && !right) return false
  if (!left || !right) return true
  const hash = crypto.parent(left, right)
  return !b4a.equals(hash, parent.hash) || parent.size !== (left.size + right.size)
}

function * allBits (bitfield) {
  let i = 0
  if (bitfield.get(0)) yield 0
  while (true) {
    i = bitfield.findFirst(true, i + 1)
    if (i === -1) break
    yield i
  }
}
