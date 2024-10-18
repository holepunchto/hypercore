const flat = require('flat-tree')
const b4a = require('b4a')
const quickbit = require('quickbit-universal')

// just in its own file as its a bit involved

module.exports = copyPrologue

async function copyPrologue (src, dst) {
  const prologue = dst.header.manifest.prologue

  if (src.tree.length < prologue.length || prologue.length === 0) return

  const stack = []
  const roots = flat.fullRoots(prologue.length * 2)
  const batch = { roots, first: true, last: false, contig: 0, tree: [], blocks: [] }

  for (let i = 0; i < roots.length; i++) {
    const node = roots[i]
    batch.tree.push(node)
    stack.push(node)
  }

  let lastPage = -1
  let contigPrev = -1

  for await (const data of src.storage.createBlockStream({ reverse: true, lt: prologue.length })) {
    if (walkTree(stack, data.index * 2, batch) === false) throw new Error('Missing block or tree node for ' + data.index)

    batch.contig = data.index + 1 === contigPrev ? batch.contig + 1 : 1
    contigPrev = data.index

    const page = getBitfieldPage(data.index)
    const waiting = batch.blocks.push(data)

    // always safe to partially flush so we do that ondemand to reduce memory usage...
    if ((waiting >= 4096 && page !== lastPage) || (waiting >= 32768)) await flushBatch(prologue, src, dst, batch)

    lastPage = page
  }

  if (contigPrev !== 0) batch.contig = 0

  batch.last = true
  await flushBatch(prologue, src, dst, batch)
}

async function flushBatch (prologue, src, dst, batch) {
  const nodePromises = []

  const srcReader = src.storage.createReadBatch()
  for (const index of batch.tree) nodePromises.push(srcReader.getTreeNode(index).catch(noop))
  srcReader.tryFlush()

  const nodes = await Promise.all(nodePromises)

  const pagePromises = []
  const dstReader = dst.storage.createReadBatch()

  const headPromise = batch.first ? dstReader.getCoreHead() : null
  if (headPromise) headPromise.catch(noop)

  let lastPage = -1
  for (const { index } of batch.blocks) {
    const page = getBitfieldPage(index)
    if (page === lastPage) continue
    lastPage = page
    pagePromises.push(dstReader.getBitfieldPage(page).catch(noop))
  }

  dstReader.tryFlush() // error handles here

  const pages = await Promise.all(pagePromises)
  const head = headPromise === null ? null : await headPromise
  const userData = []

  // reads done!

  if (batch.first) {
    const treeHash = dst.crypto.tree(nodes.slice(0, batch.roots.length))
    if (!b4a.equals(treeHash, prologue.hash)) throw new Error('Prologue does not match source')
  }

  if (batch.first) {
    for await (const data of src.storage.createUserDataStream()) userData.push(data)
  }

  for (let i = 0; i < pages.length; i++) {
    if (!pages[i]) pages[i] = b4a.alloc(4096)
  }

  const writer = dst.storage.createWriteBatch()

  for (const node of nodes) writer.putTreeNode(node)

  lastPage = -1
  let pageIndex = -1

  for (const { index, value } of batch.blocks) {
    const page = getBitfieldPage(index)

    if (page !== lastPage) {
      lastPage = page
      pageIndex++
      writer.putBitfieldPage(pageIndex, pages[pageIndex])
    }

    const pageBuffer = pages[pageIndex]
    quickbit.set(pageBuffer, getBitfieldOffset(index), true)
    writer.putBlock(index, value)
  }

  for (const { key, value } of userData) {
    writer.setUserData(key, value)
  }

  let upgraded = batch.first && !head
  if (upgraded) {
    writer.setCoreHead(prologueToTree(prologue))
  }

  await writer.flush()

  if (upgraded) {
    const roots = await dst.tree.getRoots(prologue.length)

    dst.tree.setRoots(roots, null)
    dst.header.tree = prologueToTree(prologue)
  }

  if (batch.contig) {
    // TODO: we need to persist this somehow
    dst.header.hints.contiguousLength = batch.contig
  }

  let start = 0
  let length = 0

  // update in memory bitfield
  for (const { index } of batch.blocks) {
    if (start - 1 === index) {
      length++
    } else {
      if (length > 0) signalReplicator(dst, upgraded, start, length)
      length = 1
    }

    start = index
    dst.bitfield.set(index, true)
    upgraded = false
  }

  if (length > 0) signalReplicator(dst, upgraded, start, length)

  // unlink
  batch.tree = []
  batch.blocks = []
  batch.first = false
}

function signalReplicator (core, upgraded, start, length) {
  const status = upgraded ? 0b0011 : 0b0010
  const bitfield = { drop: false, start, length }
  core.onupdate({ status, bitfield, value: null, from: null })
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
  return Math.floor(index / 32768)
}

function getBitfieldOffset (index) {
  return index & 32767
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