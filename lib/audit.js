const hypercoreCrypto = require('hypercore-crypto')
const flat = require('flat-tree')
const c = require('compact-encoding')
const b4a = require('b4a')

const empty = b4a.alloc(32)

// this is optimised for speed over mem atm
// can be tweaked in the future

module.exports = async function auditCore (core) {
  const corrections = {
    tree: 0,
    blocks: 0
  }

  const length = core.header.tree.length

  const data = await readFullStorage(core.blocks.storage)
  const tree = await readFullStorage(core.tree.storage)

  const valid = new Uint8Array(Math.ceil(tree.byteLength / 40))
  const stack = []

  for (const r of core.tree.roots) {
    valid[r.index] = 1
    stack.push(r)
  }

  while (stack.length > 0) {
    const node = stack.pop()
    if ((node.index & 1) === 0) continue

    const [left, right] = flat.children(node.index)
    const leftNode = getNode(left)
    const rightNode = getNode(right)

    if (!rightNode && !leftNode) continue

    stack.push(leftNode, rightNode)

    if (valid[node.index]) {
      const hash = hypercoreCrypto.parent(leftNode, rightNode)
      if (b4a.equals(hash, node.hash) && node.size === (leftNode.size + rightNode.size)) {
        valid[leftNode.index] = 1
        valid[rightNode.index] = 1
        continue
      }
    }

    if (leftNode.size) clearNode(leftNode)
    if (rightNode.size) clearNode(rightNode)
  }

  if (corrections.tree) {
    core.tree.cache.clear()
  }

  let i = 0
  let nextOffset = -1
  while (i < length) {
    const has = core.bitfield.get(i)

    if (!has) {
      if (i + 1 === length) break
      i = core.bitfield.findFirst(true, i + 1)
      if (i < 0) break
      nextOffset = -1
      continue
    }

    if (nextOffset === -1) {
      try {
        nextOffset = await core.tree.byteOffset(i * 2)
      } catch {
        core._setBitfield(i, false)
        corrections.blocks++
        i++
        continue
      }
    }

    const node = getNode(i * 2)
    const blk = data.subarray(nextOffset, nextOffset + node.size)
    const hash = hypercoreCrypto.data(blk)

    nextOffset += blk.byteLength

    if (!b4a.equals(hash, node.hash)) {
      core._setBitfield(i, false)
      corrections.blocks++
    }

    i++
  }

  return corrections

  function getNode (index) {
    if (index * 40 + 40 > tree.byteLength) return null
    const state = { start: index * 40, end: index * 40 + 40, buffer: tree }
    const size = c.uint64.decode(state)
    const hash = c.fixed32.decode(state)
    if (size === 0 && hash.equals(empty)) return null
    return { index, size, hash }
  }

  function clearNode (node) {
    valid[node.index] = 0

    if (node.size) {
      b4a.fill(tree, 0, node.index * 40, node.index * 40 + 40)
      core.tree.unflushed.set(node.index, core.tree.blankNode(node.index))
      corrections.tree++
    }
  }
}

function readFullStorage (storage) {
  return new Promise((resolve, reject) => {
    storage.stat((_, st) => {
      if (!st) return resolve(b4a.alloc(0))
      storage.read(0, st.size, (err, data) => {
        if (err) reject(err)
        else resolve(data)
      })
    })
  })
}
