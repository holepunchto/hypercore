const path = require('path')
const test = require('brittle')
const RAM = require('random-access-memory')
const b4a = require('b4a')
const RandomAccessFile = require('random-access-file')
const createTempDir = require('test-tmp')
const Tree = require('../lib/merkle-tree')

test('nodes', async function (t) {
  const tree = await create()

  const b = tree.batch()

  for (let i = 0; i < 8; i++) {
    b.append(b4a.from([i]))
  }

  b.commit()

  t.is(await tree.nodes(0), 0)

  await t.exception(tree.byteOffset(18))
})

test('proof only block', async function (t) {
  const tree = await create(10)

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
  const tree = await create(10)

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
  const tree = await create(10)

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
  const tree = await create(10)

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
  const tree = await create(10)

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
  const tree = await create(10)

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
  const tree = await create(10)

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
  const tree = await create(10)

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
  const tree = await create(16)

  const proof = await tree.proof({
    seek: { bytes: 7, padding: 1 },
    block: { index: 0, nodes: 4 }
  })

  t.is(proof.upgrade, null)
  t.alike(proof.block.nodes.map(n => n.index), [2, 5, 23])
  t.alike(proof.seek.nodes.map(n => n.index), [12, 14, 9])
})

test('proof block and seek that results in tree, no upgrade', async function (t) {
  const tree = await create(16)

  const proof = await tree.proof({
    seek: { bytes: 26, padding: 0 },
    block: { index: 0, nodes: 4 }
  })

  t.is(proof.upgrade, null)
  t.alike(proof.block.nodes.map(n => n.index), [2, 5, 11])
  t.alike(proof.seek.nodes.map(n => n.index), [19, 27])
})

test('proof block and seek, with upgrade', async function (t) {
  const tree = await create(10)

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
  const tree = await create(10)

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
  const tree = await create(10)
  const clone = await create()

  const p = await tree.proof({
    hash: { index: 6, nodes: 0 },
    upgrade: { start: 0, length: 10 }
  })

  const b = await clone.verify(p)
  b.commit()

  t.is(clone.length, tree.length)
  t.is(clone.byteLength, tree.byteLength)
  t.is(await clone.byteOffset(6), await tree.byteOffset(6))
  t.is(await clone.get(6), await tree.get(6))
})

test('verify proof #2', async function (t) {
  const tree = await create(10)
  const clone = await create()

  const p = await tree.proof({
    seek: { bytes: 10, padding: 0 },
    upgrade: { start: 0, length: 10 }
  })

  const b = await clone.verify(p)
  b.commit()

  t.is(clone.length, tree.length)
  t.is(clone.byteLength, tree.byteLength)
  t.alike(await clone.byteRange(10), await tree.byteRange(10))
})

test('upgrade edgecase when no roots need upgrade', async function (t) {
  const tree = await create(4)
  const clone = await create()

  {
    const proof = await tree.proof({
      upgrade: { start: 0, length: 4 }
    })

    const b = await clone.verify(proof)
    b.commit()
  }

  const b = tree.batch()
  b.append(b4a.from('#5'))
  b.commit()

  {
    const proof = await tree.proof({
      upgrade: { start: 4, length: 1 }
    })

    const b = await clone.verify(proof)
    b.commit()
  }

  t.is(tree.length, 5)
})

test('lowest common ancestor - small gap', async function (t) {
  const tree = await create(10)
  const clone = await create(8)
  const ancestors = await reorg(clone, tree)

  t.is(ancestors, 8)
  t.is(clone.length, tree.length)
})

test('lowest common ancestor - bigger gap', async function (t) {
  const tree = await create(20)
  const clone = await create(1)
  const ancestors = await reorg(clone, tree)

  t.is(ancestors, 1)
  t.is(clone.length, tree.length)
})

test('lowest common ancestor - remote is shorter than local', async function (t) {
  const tree = await create(5)
  const clone = await create(10)
  const ancestors = await reorg(clone, tree)

  t.is(ancestors, 5)
  t.is(clone.length, tree.length)
})

test('lowest common ancestor - simple fork', async function (t) {
  const tree = await create(5)
  const clone = await create(5)

  {
    const b = tree.batch()
    b.append(b4a.from('fork #1'))
    b.commit()
  }

  {
    const b = clone.batch()
    b.append(b4a.from('fork #2'))
    b.commit()
  }

  const ancestors = await reorg(clone, tree)

  t.is(ancestors, 5)
  t.is(clone.length, tree.length)
})

test('lowest common ancestor - long fork', async function (t) {
  const tree = await create(5)
  const clone = await create(5)

  {
    const b = tree.batch()
    b.append(b4a.from('fork #1'))
    b.commit()
  }

  {
    const b = clone.batch()
    b.append(b4a.from('fork #2'))
    b.commit()
  }

  {
    const b = tree.batch()
    for (let i = 0; i < 100; i++) b.append(b4a.from('#' + i))
    b.commit()
  }

  {
    const b = clone.batch()
    for (let i = 0; i < 100; i++) b.append(b4a.from('#' + i))
    b.commit()
  }

  const ancestors = await reorg(clone, tree)

  t.is(ancestors, 5)
  t.is(clone.length, tree.length)

  t.ok(await audit(tree))
  await tree.flush()
  t.ok(await audit(tree))
})

test('tree hash', async function (t) {
  const a = await create(5)
  const b = await create(5)

  t.alike(a.hash(), b.hash())

  {
    const b = a.batch()
    t.alike(b.hash(), a.hash())
    b.append(b4a.from('hi'))
    const h = b.hash()
    t.unlike(h, a.hash())
    b.commit()
    t.alike(h, a.hash())
  }

  {
    const ba = b.batch()
    ba.append(b4a.from('hi'))
    const h = ba.hash()
    t.unlike(h, b.hash())
    t.alike(h, a.hash())
    ba.commit()
    t.alike(h, b.hash())
  }
})

test('basic tree seeks', async function (t) {
  const a = await create(5)

  {
    const b = a.batch()
    b.append(b4a.from('bigger'))
    b.append(b4a.from('block'))
    b.append(b4a.from('tiny'))
    b.append(b4a.from('s'))
    b.append(b4a.from('another'))
    b.commit()
  }

  t.is(a.length, 10)
  t.is(a.byteLength, 33)

  for (let i = 0; i < a.byteLength; i++) {
    const s = a.seek(i)

    const actual = await s.update()
    const expected = await linearSeek(a, i)

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
  const a = await create(5)

  t.is(a.length, 5)

  await a.clear()

  t.is(a.length, 0)

  try {
    await a.get(2)
    t.fail('node should not exist now')
  } catch {
    t.pass('node should fail')
  }
})

test('get older roots', async function (t) {
  const a = await create(5)

  const roots = await a.getRoots(5)
  t.alike(roots, a.roots, 'same roots')

  {
    const b = a.batch()
    b.append(b4a.from('next'))
    b.append(b4a.from('next'))
    b.append(b4a.from('next'))
    b.commit()
  }

  const oldRoots = await a.getRoots(5)
  t.alike(oldRoots, roots, 'same old roots')

  const expected = []
  const len = a.length

  for (let i = 0; i < 40; i++) {
    expected.push([...a.roots])
    {
      const b = a.batch()
      b.append(b4a.from('tick'))
      b.commit()
    }
  }

  const actual = []

  for (let i = 0; i < 40; i++) {
    actual.push(await a.getRoots(len + i))
  }

  t.alike(actual, expected, 'check a bunch of different roots')
})

test('check if a length is upgradeable', async function (t) {
  const tree = await create(5)
  const clone = await create()

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

  const b = await clone.verify(p)
  b.commit()

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

  t.is(await clone.upgradeable(0), true)
  t.is(await clone.upgradeable(1), false)
  t.is(await clone.upgradeable(2), false)
  t.is(await clone.upgradeable(3), false)
  t.is(await clone.upgradeable(4), true)
  t.is(await clone.upgradeable(5), true)
})

test('clone a batch', async t => {
  const a = await create(5)

  const b = a.batch()
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

  b.commit()

  let same = b.roots.length === c.roots.length
  for (let i = 0; i < b.roots.length; i++) {
    if (b.roots[i].index !== c.roots[i].index) same = false
    if (!same) break
  }

  t.absent(same)
  t.not(b.nodes.length, c.nodes.length)
})

test('prune nodes in a batch', async t => {
  const a = await create(0)
  const b = a.batch()

  for (let i = 0; i < 16; i++) {
    b.append(b4a.from('tick tock'))
  }

  b.prune(15)

  const nodes = b.nodes.sort((a, b) => a.index - b.index).map(n => n.index)

  t.alike(nodes, [15, 23, 27, 29, 30])
})

test('checkout nodes in a batch', async t => {
  const a = await create(0)
  const b = a.batch()

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

test('roots get unslabbed', async function (t) {
  const tree = await create()

  const b = tree.batch()

  for (let i = 0; i < 100; i++) {
    b.append(b4a.from([i]))
  }

  b.commit()

  t.is(tree.roots.length > 1, true, 'sanity check')

  const rootByteLength = 32
  const buffer = tree.roots[0].hash.buffer

  t.is(
    buffer.byteLength,
    rootByteLength,
    'unslabbed the first root'
  )
  t.is(
    tree.roots[1].hash.buffer.byteLength,
    rootByteLength,
    'unslabbed the second root'
  )
  t.is(
    tree.roots[2].hash.buffer.byteLength,
    rootByteLength,
    'unslabbed the third root'
  )
})

test('buffer of cached nodes is copied to small slab', async function (t) {
  // RAM does not use slab-allocated memory,
  // so we need to us random-access-file to reproduce this issue
  const dir = await createTempDir(t)
  const storage = new RandomAccessFile(path.join(dir, 'tree'))

  const tree = await Tree.open(storage)

  const b = tree.batch()
  b.append(b4a.from('tree-entry'))
  b.commit()

  // Note: if the batch is committed but not yet flushed,
  // it still uses the original slab-allocated memory
  await tree.flush()

  const node = await tree.get(0)
  t.is(node.hash.buffer.byteLength, 32, 'created a new memory slab of the correct (small) size')

  await tree.close()
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
  const upgrade = { start: 0, length: remote.length }
  const r = await local.reorg(await remote.proof({ upgrade }))

  while (!r.finished) {
    const index = 2 * (r.want.end - 1)
    const nodes = r.want.nodes

    await r.update(await remote.proof({ hash: { index, nodes } }))
  }

  r.commit()
  return r.ancestors
}

async function create (length = 0) {
  const tree = await Tree.open(new RAM())
  const b = tree.batch()
  for (let i = 0; i < length; i++) {
    b.append(b4a.from('#' + i))
  }
  b.commit()
  return tree
}
