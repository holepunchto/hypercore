const crypto = require('hypercore-crypto')
const flat = require('flat-tree')
const b4a = require('b4a')
const quickbit = require('quickbit-universal')
const Bitfield = require('./bitfield')

const MAX_BATCH_USED = 4 * 1024 * 1024
const MIN_BATCH_USED = 512 * 1024

// just in its own file as its a bit involved

module.exports = copyPrologue

async function copyPrologue (src, dst) {
  const prologue = dst.header.manifest.prologue

  if (src.length < prologue.length || prologue.length === 0) return

  const stack = []
  const roots = flat.fullRoots(prologue.length * 2)
  const batch = { roots, first: true, last: false, contig: 0, used: 0, tree: [], blocks: [] }

  for (let i = 0; i < roots.length; i++) {
    const node = roots[i]
    batch.tree.push(node)
    stack.push(node)
  }

  let lastPage = -1
  let lastBlock = -1

  for await (const data of src.storage.createBlockStream({ gte: 0, lt: prologue.length, reverse: true })) {
    if (walkTree(stack, data.index * 2, batch) === false) {
      throw new Error('Missing block or tree node for ' + data.index)
    }

    batch.contig = data.index + 1 === lastBlock ? batch.contig + 1 : 1
    lastBlock = data.index

    const page = getBitfieldPage(data.index)
    batch.blocks.push(data)

    if (lastPage !== page) batch.used += 4096
    batch.used += Math.max(data.value.byteLength, 128) // 128 is just a sanity number to avoid mega batches

    // always safe to partially flush so we do that ondemand to reduce memory usage...
    if ((batch.used >= MIN_BATCH_USED && page !== lastPage) || (batch.used >= MAX_BATCH_USED)) {
      await flushBatch(prologue, src, dst, batch)
    }

    lastPage = page
  }

  if (lastBlock !== 0) batch.contig = 0

  batch.last = true
  await flushBatch(prologue, src, dst, batch)
}

async function flushBatch (prologue, src, dst, batch) {
  const nodePromises = []

  const srcReader = src.storage.read()
  for (const index of batch.tree) {
    nodePromises.push(srcReader.getTreeNode(index))
  }
  srcReader.tryFlush()

  const nodes = await Promise.all(nodePromises)

  const pagePromises = []
  const dstReader = dst.storage.read()

  const headPromise = batch.first ? dstReader.getHead() : null
  if (headPromise) headPromise.catch(noop)

  let lastPage = -1
  for (const { index } of batch.blocks) {
    const page = getBitfieldPage(index)
    if (page === lastPage) continue
    lastPage = page
    pagePromises.push(dstReader.getBitfieldPage(page))
  }

  dstReader.tryFlush()

  const pages = await Promise.all(pagePromises)
  const head = headPromise === null ? null : await headPromise
  const userData = []

  // reads done!

  if (batch.first) {
    const roots = nodes.slice(0, batch.roots.length)

    for (const node of roots) {
      if (!node) throw new Error('Missing nodes for prologue hash')
    }

    const treeHash = crypto.tree(roots)
    if (!b4a.equals(treeHash, prologue.hash)) throw new Error('Prologue does not match source')
  }

  if (batch.first) {
    for await (const data of src.storage.createUserDataStream()) userData.push(data)
  }

  for (let i = 0; i < pages.length; i++) {
    if (!pages[i]) pages[i] = b4a.alloc(4096)
  }

  const tx = dst.storage.write()

  for (const node of nodes) tx.putTreeNode(node)

  lastPage = -1
  let pageIndex = -1

  for (const { index, value } of batch.blocks) {
    const page = getBitfieldPage(index)

    if (page !== lastPage) {
      lastPage = page
      pageIndex++
      // queue the page now, we mutate it below but its the same ref
      tx.putBitfieldPage(pageIndex, pages[pageIndex])
    }

    const pageBuffer = pages[pageIndex]
    quickbit.set(pageBuffer, getBitfieldOffset(index), true)
    tx.putBlock(index, value)
  }

  for (const { key, value } of userData) {
    tx.putUserData(key, value)
  }

  let upgraded = batch.first && !head
  if (upgraded) {
    tx.setHead(prologueToTree(prologue))
  }

  await tx.flush()

  if (upgraded) {
    const roots = nodes.slice(0, batch.roots.length)
    dst.state.setRoots(roots)
    dst.header.tree = prologueToTree(prologue)
  }

  if (userData.length > 0) {
    dst.header.userData = userData.concat(dst.header.userData)
  }

  if (batch.contig) {
    // TODO: we need to persist this somehow
    dst.header.hints.contiguousLength = batch.contig
  }

  let start = 0
  let length = 0

  // update in memory bitfield
  for (const { index } of batch.blocks) {
    if (start === 0 || start - 1 === index) {
      length++
    } else {
      if (length > 0) signalReplicator(dst, upgraded, start, length)
      upgraded = false
      length = 1
    }

    start = index
    dst.bitfield.set(index, true)
  }

  if (length > 0) signalReplicator(dst, upgraded, start, length)

  // unlink
  batch.tree = []
  batch.blocks = []
  batch.first = false
  batch.used = 0
}

function signalReplicator (core, upgraded, start, length) {
  if (upgraded) {
    core.replicator.cork()
    core.replicator.onhave(start, length, false)
    core.replicator.onupgrade()
    core.replicator.uncork()
  } else {
    core.replicator.onhave(start, length, false)
  }
}

function prologueToTree (prologue) {
  return {
    fork: 0,
    length: prologue.length,
    rootHash: prologue.hash,
    signature: null
  }
}

function getBitfieldPage (index) {
  return Math.floor(index / Bitfield.BITS_PER_PAGE)
}

function getBitfieldOffset (index) {
  return index & (Bitfield.BITS_PER_PAGE - 1)
}

function walkTree (stack, target, batch) {
  while (stack.length > 0) {
    const node = stack.pop()

    if ((node & 1) === 0) {
      if (node === target) return true
      continue
    }

    const ite = flat.iterator(node)
    if (!ite.contains(target)) continue

    while ((ite.index & 1) !== 0) {
      const left = ite.leftChild()
      const right = ite.sibling() // is right child

      batch.tree.push(left, right)

      if (ite.contains(target)) stack.push(left)
      else ite.sibling()
    }

    if (ite.index === target) return true
  }

  return false
}

function noop () {}
