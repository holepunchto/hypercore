const test = require('brittle')
const b4a = require('b4a')
const createTempDir = require('test-tmp')
const CoreStorage = require('hypercore-on-the-rocks')
const Tree = require('../lib/merkle-tree')

test('nodes', async function (t) {
  const { storage, tree } = await create(t)

  const b = tree.batch()

  for (let i = 0; i < 8; i++) {
    b.append(b4a.from([i]))
  }

  const wb = storage.createWriteBatch()
  await b.commit(wb)
  await wb.flush()

  t.is(await tree.nodes(0), 0)

  await t.exception(tree.byteOffset(18))
})

test('proof only block', async function (t) {
  const { tree } = await create(t, 10)

  const proof = await tree.proof({
    block: { index: 4, nodes: 2 }
  })

  t.is(proof.upgrade, null)
  t.is(proof.seek, null)
  t.is(proof.block.index, 4)
  t.is(proof.block.nodes.length, 2)
  t.alike(proof.block.nodes.map(n => n.index), [10, 13])
})

test('proof with upgrade', async function (t) {
  const { tree } = await create(t, 10)

  const proof = await tree.proof({
    block: { index: 4, nodes: 0 },
    upgrade: { start: 0, length: 10 }
  })

  t.is(proof.seek, null)
  t.is(proof.block.index, 4)
  t.is(proof.block.nodes.length, 3)
  t.alike(proof.block.nodes.map(n => n.index), [10, 13, 3])
  t.is(proof.upgrade.start, 0)
  t.is(proof.upgrade.length, 10)
  t.alike(proof.upgrade.nodes.map(n => n.index), [17])
  t.alike(proof.upgrade.additionalNodes.map(n => n.index), [])
})

test('proof with upgrade + additional', async function (t) {
  const { tree } = await create(t, 10)

  const proof = await tree.proof({
    block: { index: 4, nodes: 0 },
    upgrade: { start: 0, length: 8 }
  })

  t.is(proof.seek, null)
  t.is(proof.block.index, 4)
  t.is(proof.block.nodes.length, 3)
  t.alike(proof.block.nodes.map(n => n.index), [10, 13, 3])
  t.is(proof.upgrade.start, 0)
  t.is(proof.upgrade.length, 8)
  t.alike(proof.upgrade.nodes.map(n => n.index), [])
  t.alike(proof.upgrade.additionalNodes.map(n => n.index), [17])
})

test('proof with upgrade from existing state', async function (t) {
  const { tree } = await create(t, 10)

  const proof = await tree.proof({
    block: { index: 1, nodes: 0 },
    upgrade: { start: 1, length: 9 }
  })

  t.is(proof.seek, null)
  t.is(proof.block.index, 1)
  t.is(proof.block.nodes.length, 0)
  t.alike(proof.block.nodes.map(n => n.index), [])
  t.is(proof.upgrade.start, 1)
  t.is(proof.upgrade.length, 9)
  t.alike(proof.upgrade.nodes.map(n => n.index), [5, 11, 17])
  t.alike(proof.upgrade.additionalNodes.map(n => n.index), [])
})

test('proof with upgrade from existing state + additional', async function (t) {
  const { tree } = await create(t, 10)

  const proof = await tree.proof({
    block: { index: 1, nodes: 0 },
    upgrade: { start: 1, length: 5 }
  })

  t.is(proof.seek, null)
  t.is(proof.block.index, 1)
  t.is(proof.block.nodes.length, 0)
  t.alike(proof.block.nodes.map(n => n.index), [])
  t.is(proof.upgrade.start, 1)
  t.is(proof.upgrade.length, 5)
  t.alike(proof.upgrade.nodes.map(n => n.index), [5, 9])
  t.alike(proof.upgrade.additionalNodes.map(n => n.index), [13, 17])
})

test('proof block and seek, no upgrade', async function (t) {
  const { tree } = await create(t, 10)

  const proof = await tree.proof({
    seek: { bytes: 8, padding: 0 },
    block: { index: 4, nodes: 2 }
  })

  t.is(proof.upgrade, null)
  t.is(proof.seek, null) // seek included in the block
  t.is(proof.block.index, 4)
  t.is(proof.block.nodes.length, 2)
  t.alike(proof.block.nodes.map(n => n.index), [10, 13])
})

test('proof block and seek #2, no upgrade', async function (t) {
  const { tree } = await create(t, 10)

  const proof = await tree.proof({
    seek: { bytes: 10, padding: 0 },
    block: { index: 4, nodes: 2 }
  })

  t.is(proof.upgrade, null)
  t.is(proof.seek, null) // seek included in the block
  t.is(proof.block.index, 4)
  t.is(proof.block.nodes.length, 2)
  t.alike(proof.block.nodes.map(n => n.index), [10, 13])
})

test('proof block and seek #3, no upgrade', async function (t) {
  const { tree } = await create(t, 10)

  const proof = await tree.proof({
    seek: { bytes: 13, padding: 0 },
    block: { index: 4, nodes: 2 }
  })

  t.is(proof.upgrade, null)
  t.alike(proof.seek.nodes.map(n => n.index), [12, 14])
  t.is(proof.block.index, 4)
  t.is(proof.block.nodes.length, 1)
  t.alike(proof.block.nodes.map(n => n.index), [10])
})

test('proof seek with padding, no upgrade', async function (t) {
  const { tree } = await create(t, 16)

  const proof = await tree.proof({
    seek: { bytes: 7, padding: 1 },
    block: { index: 0, nodes: 4 }
  })

  t.is(proof.upgrade, null)
  t.alike(proof.block.nodes.map(n => n.index), [2, 5, 23])
  t.alike(proof.seek.nodes.map(n => n.index), [12, 14, 9])
})

test('proof block and seek that results in tree, no upgrade', async function (t) {
  const { tree } = await create(t, 16)

  const proof = await tree.proof({
    seek: { bytes: 26, padding: 0 },
    block: { index: 0, nodes: 4 }
  })

  t.is(proof.upgrade, null)
  t.alike(proof.block.nodes.map(n => n.index), [2, 5, 11])
  t.alike(proof.seek.nodes.map(n => n.index), [19, 27])
})

test('proof block and seek, with upgrade', async function (t) {
  const { tree } = await create(t, 10)

  const proof = await tree.proof({
    seek: { bytes: 13, padding: 0 },
    block: { index: 4, nodes: 2 },
    upgrade: { start: 8, length: 2 }
  })

  t.alike(proof.seek.nodes.map(n => n.index), [12, 14])
  t.is(proof.block.index, 4)
  t.is(proof.block.nodes.length, 1)
  t.alike(proof.block.nodes.map(n => n.index), [10])
  t.is(proof.upgrade.start, 8)
  t.is(proof.upgrade.length, 2)
  t.alike(proof.upgrade.nodes.map(n => n.index), [17])
  t.alike(proof.upgrade.additionalNodes.map(n => n.index), [])
})

test('proof seek with upgrade', async function (t) {
  const { tree } = await create(t, 10)

  const proof = await tree.proof({
    seek: { bytes: 13, padding: 0 },
    upgrade: { start: 0, length: 10 }
  })

  t.alike(proof.seek.nodes.map(n => n.index), [12, 14, 9, 3])
  t.is(proof.block, null)
  t.is(proof.upgrade.start, 0)
  t.is(proof.upgrade.length, 10)
  t.alike(proof.upgrade.nodes.map(n => n.index), [17])
  t.alike(proof.upgrade.additionalNodes.map(n => n.index), [])
})

test('verify proof #1', async function (t) {
  const { tree } = await create(t, 10)
  const clone = await create(t)

  const p = await tree.proof({
    hash: { index: 6, nodes: 0 },
    upgrade: { start: 0, length: 10 }
  })

  const b = await clone.tree.verify(p)

  const wb = clone.storage.createWriteBatch()
  b.commit(wb)

  await wb.flush()

  t.is(clone.tree.length, tree.length)
  t.is(clone.tree.byteLength, tree.byteLength)
  t.is(await clone.tree.byteOffset(6), await tree.byteOffset(6))
  t.alike(await clone.tree.get(6), await tree.get(6))
})

test('verify proof #2', async function (t) {
  const { tree } = await create(t, 10)
  const clone = await create(t)

  const p = await tree.proof({
    seek: { bytes: 10, padding: 0 },
    upgrade: { start: 0, length: 10 }
  })

  const b = await clone.tree.verify(p)
  const wb = clone.storage.createWriteBatch()
  await b.commit(wb)
  await wb.flush()

  t.is(clone.tree.length, tree.length)
  t.is(clone.tree.byteLength, tree.byteLength)
  t.alike(await clone.tree.byteRange(10), await tree.byteRange(10))
})

test('upgrade edgecase when no roots need upgrade', async function (t) {
  const { tree, storage } = await create(t, 4)
  const clone = await create(t)

  {
    const proof = await tree.proof({
      upgrade: { start: 0, length: 4 }
    })

    const b = await clone.tree.verify(proof)
    const wb = clone.storage.createWriteBatch()
    await b.commit(wb)
    await wb.flush()
  }

  const b = tree.batch()
  b.append(b4a.from('#5'))
  const wb = storage.createWriteBatch()
  await b.commit(wb)
  await wb.flush()

  {
    const proof = await tree.proof({
      upgrade: { start: 4, length: 1 }
    })

    const b = await clone.tree.verify(proof)
    const wb = clone.tree.storage.createWriteBatch()
    await b.commit(wb)
    await wb.flush()
  }

  t.is(tree.length, 5)
})

test('lowest common ancestor - small gap', async function (t) {
  const { tree } = await create(t, 10)
  const clone = await create(t, 8)
  const ancestors = await reorg(clone, { tree })

  t.is(ancestors, 8)
  t.is(clone.tree.length, tree.length)
})

test('lowest common ancestor - bigger gap', async function (t) {
  const { tree } = await create(t, 20)
  const clone = await create(t, 1)
  const ancestors = await reorg(clone, { tree })

  t.is(ancestors, 1)
  t.is(clone.tree.length, tree.length)
})

test('lowest common ancestor - remote is shorter than local', async function (t) {
  const { tree } = await create(t, 5)
  const clone = await create(t, 10)
  const ancestors = await reorg(clone, { tree })

  t.is(ancestors, 5)
  t.is(clone.tree.length, tree.length)
})

test('lowest common ancestor - simple fork', async function (t) {
  const { tree, storage } = await create(t, 5)
  const clone = await create(t, 5)

  {
    const b = tree.batch()
    b.append(b4a.from('fork #1'))
    const wb = storage.createWriteBatch()
    await b.commit(wb)
    await wb.flush()
  }

  {
    const b = clone.tree.batch()
    b.append(b4a.from('fork #2'))
    const wb = clone.storage.createWriteBatch()
    await b.commit(wb)
    await wb.flush()
  }

  const ancestors = await reorg(clone, { tree })

  t.is(ancestors, 5)
  t.is(clone.tree.length, tree.length)
})

test('lowest common ancestor - long fork', async function (t) {
  const { tree, storage } = await create(t, 5)
  const clone = await create(t, 5)

  {
    const b = tree.batch()
    b.append(b4a.from('fork #1'))
    const wb = storage.createWriteBatch()
    await b.commit(wb)
    await wb.flush()
  }

  {
    const b = clone.tree.batch()
    b.append(b4a.from('fork #2'))
    const wb = clone.storage.createWriteBatch()
    await b.commit(wb)
    await wb.flush()
  }

  {
    const b = tree.batch()
    for (let i = 0; i < 100; i++) b.append(b4a.from('#' + i))
    const wb = storage.createWriteBatch()
    await b.commit(wb)
    await wb.flush()
  }

  {
    const b = clone.tree.batch()
    for (let i = 0; i < 100; i++) b.append(b4a.from('#' + i))
    const wb = clone.storage.createWriteBatch()
    await b.commit(wb)
    await wb.flush()
  }

  const ancestors = await reorg(clone, { tree })

  t.is(ancestors, 5)
  t.is(clone.tree.length, tree.length)

  t.ok(await audit(tree))
  t.ok(await audit(tree))
})

test('tree hash', async function (t) {
  const a = await create(t, 5)
  const b = await create(t, 5)

  t.alike(a.tree.hash(), b.tree.hash())

  {
    const b = a.tree.batch()
    t.alike(b.hash(), a.tree.hash())
    b.append(b4a.from('hi'))
    const h = b.hash()
    t.unlike(h, a.tree.hash())
    const wb = a.storage.createWriteBatch()
    await b.commit(wb)
    await wb.flush()
    t.alike(h, a.tree.hash())
  }

  {
    const ba = b.tree.batch()
    ba.append(b4a.from('hi'))
    const h = ba.hash()
    t.unlike(h, b.tree.hash())
    t.alike(h, a.tree.hash())
    const wba = b.storage.createWriteBatch()
    await ba.commit(wba)
    await wba.flush()
    t.alike(h, b.tree.hash())
  }
})

test('basic tree seeks', async function (t) {
  const a = await create(t, 5)

  {
    const b = a.tree.batch()
    b.append(b4a.from('bigger'))
    b.append(b4a.from('block'))
    b.append(b4a.from('tiny'))
    b.append(b4a.from('s'))
    b.append(b4a.from('another'))
    const wb = a.storage.createWriteBatch()
    await b.commit(wb)
    await wb.flush()
  }

  t.is(a.tree.length, 10)
  t.is(a.tree.byteLength, 33)

  for (let i = 0; i < a.byteLength; i++) {
    const s = a.tree.seek(i)

    const actual = await s.update()
    const expected = await linearSeek(a.tree, i)

    if (actual[0] !== expected[0] || actual[1] !== expected[1]) {
      t.is(actual, expected, 'bad seek at ' + i)
      return
    }
  }

  t.pass('checked all byte seeks')

  async function linearSeek (tree, bytes) {
    for (let i = 0; i < tree.length * 2; i += 2) {
      const node = await tree.get(i)
      if (node.size > bytes) return [i / 2, bytes]
      bytes -= node.size
    }
    return [tree.length, bytes]
  }
})

test('clear full tree', async function (t) {
  const a = await create(t, 5)

  t.is(a.tree.length, 5)

  const w = a.storage.createWriteBatch()
  a.tree.clear(w)
  await w.flush()

  t.is(a.tree.length, 0)

  try {
    await a.tree.get(2)
    t.fail('node should not exist now')
  } catch {
    t.pass('node should fail')
  }
})

test('get older roots', async function (t) {
  const a = await create(t, 5)

  const roots = await a.tree.getRoots(5)
  t.alike(roots, a.tree.roots, 'same roots')

  {
    const b = a.tree.batch()
    b.append(b4a.from('next'))
    b.append(b4a.from('next'))
    b.append(b4a.from('next'))
    const wb = a.storage.createWriteBatch()
    await b.commit(wb)
    await wb.flush()
  }

  const oldRoots = await a.tree.getRoots(5)
  t.alike(oldRoots, roots, 'same old roots')

  const expected = []
  const len = a.tree.length

  for (let i = 0; i < 40; i++) {
    expected.push([...a.tree.roots])
    {
      const b = a.tree.batch()
      b.append(b4a.from('tick'))
      const wb = a.storage.createWriteBatch()
      await b.commit(wb)
      await wb.flush()
    }
  }

  const actual = []

  for (let i = 0; i < 40; i++) {
    actual.push(await a.tree.getRoots(len + i))
  }

  t.alike(actual, expected, 'check a bunch of different roots')
})

test('check if a length is upgradeable', async function (t) {
  const { tree } = await create(t, 5)
  const clone = await create(t)

  // Full clone, has it all

  t.is(await tree.upgradeable(0), true)
  t.is(await tree.upgradeable(1), true)
  t.is(await tree.upgradeable(2), true)
  t.is(await tree.upgradeable(3), true)
  t.is(await tree.upgradeable(4), true)
  t.is(await tree.upgradeable(5), true)

  const p = await tree.proof({
    upgrade: { start: 0, length: 5 }
  })

  const b = await clone.tree.verify(p)
  const wb = clone.storage.createWriteBatch()
  await b.commit(wb)
  await wb.flush()

  /*
    Merkle tree looks like

    0─┐
      1─┐
    2─┘ │
        3 <-- root
    4─┐ │
      5─┘
    6─┘

    8 <-- root

    So length = 0, length = 4 (node 3) and length = 5 (node 8 + 3) should be upgradeable
  */

  t.is(await clone.tree.upgradeable(0), true)
  t.is(await clone.tree.upgradeable(1), false)
  t.is(await clone.tree.upgradeable(2), false)
  t.is(await clone.tree.upgradeable(3), false)
  t.is(await clone.tree.upgradeable(4), true)
  t.is(await clone.tree.upgradeable(5), true)
})

test('clone a batch', async t => {
  const a = await create(t, 5)

  const b = a.tree.batch()
  const c = b.clone()

  t.is(b.fork, c.fork)
  t.not(b.roots, c.roots)
  t.is(b.roots.length, c.roots.length)
  t.is(b.length, c.length)
  t.is(b.byteLength, c.byteLength)
  t.is(b.signature, c.signature)
  t.is(b.treeLength, c.treeLength)
  t.is(b.treeFork, c.treeFork)
  t.is(b.tree, c.tree)
  t.not(b.nodes, c.nodes)
  t.is(b.nodes.length, c.nodes.length)
  t.is(b.upgraded, c.upgraded)

  b.append(b4a.from('bigger'))
  b.append(b4a.from('block'))
  b.append(b4a.from('tiny'))
  b.append(b4a.from('s'))
  b.append(b4a.from('another'))

  const wb = a.storage.createWriteBatch()
  await b.commit(wb)
  await wb.flush()

  let same = b.roots.length === c.roots.length
  for (let i = 0; i < b.roots.length; i++) {
    if (b.roots[i].index !== c.roots[i].index) same = false
    if (!same) break
  }

  t.absent(same)
  t.not(b.nodes.length, c.nodes.length)
})

test('prune nodes in a batch', async t => {
  const a = await create(t, 0)
  const b = a.tree.batch()

  for (let i = 0; i < 16; i++) {
    b.append(b4a.from('tick tock'))
  }

  b.prune(15)

  const nodes = b.nodes.sort((a, b) => a.index - b.index).map(n => n.index)

  t.alike(nodes, [15, 23, 27, 29, 30])
})

test('checkout nodes in a batch', async t => {
  const a = await create(t, 0)
  const b = a.tree.batch()

  for (let i = 0; i < 16; i++) {
    b.append(b4a.from('tick tock'))
  }

  b.checkout(15)

  t.alike(b.length, 15)
  t.alike(b.byteLength, 135)
  t.alike(b.roots.map(n => n.index), [7, 19, 25, 28])

  const nodes = b.nodes.sort((a, b) => a.index - b.index).map(n => n.index)

  t.alike(nodes, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 19, 20, 21, 22, 24, 25, 26, 28])
})

test.skip('buffer of cached nodes is copied to small slab', async function (t) {
  // RAM does not use slab-allocated memory,
  // so we need to us random-access-file to reproduce this issue
  const { tree, storage } = await create(t)

  const b = tree.batch()
  b.append(b4a.from('tree-entry'))
  const wb = storage.createWriteBatch()
  await b.commit(wb)
  await wb.flush()

  const node = await tree.get(0)
  t.is(node.hash.buffer.byteLength, 32, 'created a new memory slab of the correct (small) size')

  await tree.close()
})

test('reopen a tree', async t => {
  const dir = await createTempDir(t)

  const a = await create(t, 16, dir)
  const b = a.tree.batch()

  for (let i = 0; i < 16; i++) {
    b.append(b4a.from('#' + i))
  }

  const wb = a.storage.createWriteBatch()
  await b.commit(wb)
  await wb.flush()

  t.alike(a.tree.length, 32)

  const byteLength = a.tree.byteLength

  t.alike(a.tree.roots.map(n => n.index), [31])

  await a.storage.close()

  const a1 = await create(t, 0, dir)

  t.alike(a1.tree.length, 32)
  t.alike(a1.tree.byteLength, byteLength)
  t.alike(a1.tree.roots.map(n => n.index), [31])
})

async function audit (tree) {
  const flat = require('flat-tree')
  const expectedRoots = flat.fullRoots(tree.length * 2)

  for (const root of tree.roots) {
    if (expectedRoots.shift() !== root.index) return false
    if (!(await check(root))) return false
  }

  if (expectedRoots.length) return false

  return true

  async function check (node) {
    if ((node.index & 1) === 0) return true

    const [l, r] = flat.children(node.index)
    const nl = await tree.get(l, false)
    const nr = await tree.get(r, false)

    if (!nl && !nr) return true

    return b4a.equals(tree.crypto.parent(nl, nr), node.hash) && await check(nl) && await check(nr)
  }
}

async function reorg (local, remote) {
  const upgrade = { start: 0, length: remote.tree.length }
  const r = await local.tree.reorg(await remote.tree.proof({ upgrade }))

  while (!r.finished) {
    const index = 2 * (r.want.end - 1)
    const nodes = r.want.nodes

    await r.update(await remote.tree.proof({ hash: { index, nodes } }))
  }

  const wb = local.storage.createWriteBatch()
  r.commit(wb)
  await wb.flush()
  return r.ancestors
}

async function create (t, length = 0, dir) {
  if (!dir) dir = await createTempDir(t)

  const db = new CoreStorage(dir)

  t.teardown(() => db.close())

  const dkey = b4a.alloc(32)

  const storage = db.get(dkey)
  if (!await storage.open()) await storage.create({ key: b4a.alloc(32) })

  const tree = await Tree.open(storage)

  if (!length) return { storage, tree }

  const b = tree.batch()
  for (let i = 0; i < length; i++) {
    b.append(b4a.from('#' + i))
  }

  const wb = storage.createWriteBatch()
  await b.commit(wb)
  await wb.flush()

  return { storage, tree }
}
