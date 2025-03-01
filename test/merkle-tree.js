const test = require('brittle')
const b4a = require('b4a')
const CoreStorage = require('hypercore-storage')
const crypto = require('hypercore-crypto')
const { ReorgBatch, MerkleTreeBatch, MerkleTree } = require('../lib/merkle-tree')

test('missing nodes', async function (t) {
  const { session, storage } = await create(t)

  const b = new MerkleTreeBatch(session)

  for (let i = 0; i < 8; i++) {
    b.append(b4a.from([i]))
  }

  const tx = storage.write()
  await b.commit(tx)
  await tx.flush()

  t.is(await MerkleTree.missingNodes(session, 0), 0)

  await t.exception(b.byteOffset(18))
})

test('proof only block', async function (t) {
  const { session, storage } = await create(t, 10)

  const b = storage.read()
  const p = await MerkleTree.proof(session, b, {
    block: { index: 4, nodes: 2 }
  })

  b.tryFlush()

  const proof = await p.settle()

  t.is(proof.upgrade, null)
  t.is(proof.seek, null)
  t.is(proof.block.index, 4)
  t.is(proof.block.nodes.length, 2)
  t.alike(proof.block.nodes.map(n => n.index), [10, 13])
})

test('proof with upgrade', async function (t) {
  const { session, storage } = await create(t, 10)

  const b = storage.read()
  const p = await MerkleTree.proof(session, b, {
    block: { index: 4, nodes: 0 },
    upgrade: { start: 0, length: 10 }
  })

  b.tryFlush()
  const proof = await p.settle()

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
  const { session, storage } = await create(t, 10)

  const b = storage.read()
  const p = await MerkleTree.proof(session, b, {
    block: { index: 4, nodes: 0 },
    upgrade: { start: 0, length: 8 }
  })

  b.tryFlush()
  const proof = await p.settle()

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
  const { session, storage } = await create(t, 10)

  const b = storage.read()
  const p = await MerkleTree.proof(session, b, {
    block: { index: 1, nodes: 0 },
    upgrade: { start: 1, length: 9 }
  })

  b.tryFlush()
  const proof = await p.settle()

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
  const { session, storage } = await create(t, 10)

  const b = storage.read()
  const p = await MerkleTree.proof(session, b, {
    block: { index: 1, nodes: 0 },
    upgrade: { start: 1, length: 5 }
  })

  b.tryFlush()
  const proof = await p.settle()

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
  const { session, storage } = await create(t, 10)

  const b = storage.read()
  const p = await MerkleTree.proof(session, b, {
    seek: { bytes: 8, padding: 0 },
    block: { index: 4, nodes: 2 }
  })

  b.tryFlush()
  const proof = await p.settle()

  t.is(proof.upgrade, null)
  t.is(proof.seek, null) // seek included in the block
  t.is(proof.block.index, 4)
  t.is(proof.block.nodes.length, 2)
  t.alike(proof.block.nodes.map(n => n.index), [10, 13])
})

test('proof block and seek #2, no upgrade', async function (t) {
  const { session, storage } = await create(t, 10)

  const b = storage.read()
  const p = await MerkleTree.proof(session, b, {
    seek: { bytes: 10, padding: 0 },
    block: { index: 4, nodes: 2 }
  })

  b.tryFlush()
  const proof = await p.settle()

  t.is(proof.upgrade, null)
  t.is(proof.seek, null) // seek included in the block
  t.is(proof.block.index, 4)
  t.is(proof.block.nodes.length, 2)
  t.alike(proof.block.nodes.map(n => n.index), [10, 13])
})

test('proof block and seek #3, no upgrade', async function (t) {
  const { session, storage } = await create(t, 10)

  const b = storage.read()
  const p = await MerkleTree.proof(session, b, {
    seek: { bytes: 13, padding: 0 },
    block: { index: 4, nodes: 2 }
  })

  b.tryFlush()
  const proof = await p.settle()

  t.is(proof.upgrade, null)
  t.alike(proof.seek.nodes.map(n => n.index), [12, 14])
  t.is(proof.block.index, 4)
  t.is(proof.block.nodes.length, 1)
  t.alike(proof.block.nodes.map(n => n.index), [10])
})

test('proof seek with padding, no upgrade', async function (t) {
  const { session, storage } = await create(t, 16)

  const b = storage.read()
  const p = await MerkleTree.proof(session, b, {
    seek: { bytes: 7, padding: 1 },
    block: { index: 0, nodes: 4 }
  })

  b.tryFlush()
  const proof = await p.settle()

  t.is(proof.upgrade, null)
  t.alike(proof.block.nodes.map(n => n.index), [2, 5, 23])
  t.alike(proof.seek.nodes.map(n => n.index), [12, 14, 9])
})

test('proof block and seek that results in tree, no upgrade', async function (t) {
  const { session, storage } = await create(t, 16)

  const b = storage.read()
  const p = await MerkleTree.proof(session, b, {
    seek: { bytes: 26, padding: 0 },
    block: { index: 0, nodes: 4 }
  })

  b.tryFlush()
  const proof = await p.settle()

  t.is(proof.upgrade, null)
  t.alike(proof.block.nodes.map(n => n.index), [2, 5, 11])
  t.alike(proof.seek.nodes.map(n => n.index), [19, 27])
})

test('proof block and seek, with upgrade', async function (t) {
  const { session, storage } = await create(t, 10)

  const b = storage.read()
  const p = await MerkleTree.proof(session, b, {
    seek: { bytes: 13, padding: 0 },
    block: { index: 4, nodes: 2 },
    upgrade: { start: 8, length: 2 }
  })

  b.tryFlush()
  const proof = await p.settle()

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
  const { session, storage } = await create(t, 10)

  const b = storage.read()
  const p = await MerkleTree.proof(session, b, {
    seek: { bytes: 13, padding: 0 },
    upgrade: { start: 0, length: 10 }
  })

  b.tryFlush()
  const proof = await p.settle()

  t.alike(proof.seek.nodes.map(n => n.index), [12, 14, 9, 3])
  t.is(proof.block, null)
  t.is(proof.upgrade.start, 0)
  t.is(proof.upgrade.length, 10)
  t.alike(proof.upgrade.nodes.map(n => n.index), [17])
  t.alike(proof.upgrade.additionalNodes.map(n => n.index), [])
})

test('verify proof #1', async function (t) {
  const { session, storage } = await create(t, 10)
  const clone = await create(t)

  const batch = storage.read()
  const proof = await MerkleTree.proof(session, batch, {
    hash: { index: 6, nodes: 0 },
    upgrade: { start: 0, length: 10 }
  })

  batch.tryFlush()
  const p = await proof.settle()

  const b = await MerkleTree.verify(clone.session, p)
  await flushBatch(clone.session, b)

  t.is(await b.byteOffset(6), await MerkleTree.byteOffset(session, 6))
  t.alike(await MerkleTree.get(clone.session, 6), await MerkleTree.get(session, 6))
})

test('verify proof #2', async function (t) {
  const { session, storage } = await create(t, 10)
  const clone = await create(t)

  const batch = storage.read()
  const proof = await MerkleTree.proof(session, batch, {
    seek: { bytes: 10, padding: 0 },
    upgrade: { start: 0, length: 10 }
  })

  batch.tryFlush()
  const p = await proof.settle()

  const b = await MerkleTree.verify(clone.session, p)
  await flushBatch(clone.session, b)

  t.is(clone.session.length, session.length)
  t.is(clone.session.byteLength, session.byteLength)
  t.alike(await b.byteRange(10), await MerkleTree.byteRange(session, 10))
})

test('upgrade edgecase when no roots need upgrade', async function (t) {
  const { session, storage } = await create(t, 4)
  const clone = await create(t)

  {
    const batch = storage.read()
    const p = await MerkleTree.proof(session, batch, {
      upgrade: { start: 0, length: 4 }
    })

    batch.tryFlush()
    const proof = await p.settle()

    const b = await MerkleTree.verify(clone.session, proof)

    await flushBatch(clone.session, b)
  }

  const b = new MerkleTreeBatch(session)
  b.append(b4a.from('#5'))

  await flushBatch(session, b)

  {
    const batch = storage.read()
    const p = await MerkleTree.proof(session, batch, {
      upgrade: { start: 4, length: 1 }
    })

    batch.tryFlush()
    const proof = await p.settle()

    const b = await MerkleTree.verify(clone.session, proof)
    await flushBatch(clone.session, b)
  }

  t.is(session.length, 5)
  t.is(clone.session.length, 5)
})

test('lowest common ancestor - small gap', async function (t) {
  const core = await create(t, 10)
  const clone = await create(t, 8)
  const ancestors = await reorg(clone, core)

  t.is(ancestors, 8)
  t.is(clone.session.length, core.session.length)
})

test('lowest common ancestor - bigger gap', async function (t) {
  const core = await create(t, 20)
  const clone = await create(t, 1)
  const ancestors = await reorg(clone, core)

  t.is(ancestors, 1)
  t.is(clone.session.length, core.session.length)
})

test('lowest common ancestor - remote is shorter than local', async function (t) {
  const core = await create(t, 5)
  const clone = await create(t, 10)
  const ancestors = await reorg(clone, core)

  t.is(ancestors, 5)
  t.is(clone.session.length, core.session.length)
})

test('lowest common ancestor - simple fork', async function (t) {
  const core = await create(t, 5)
  const clone = await create(t, 5)

  {
    const b = new MerkleTreeBatch(core.session)
    b.append(b4a.from('fork #1'))
    await flushBatch(core.session, b)
  }

  {
    const b = new MerkleTreeBatch(clone.session)
    b.append(b4a.from('fork #2'))
    await flushBatch(clone.session, b)
  }

  const ancestors = await reorg(clone, core)

  t.is(ancestors, 5)
  t.is(clone.session.length, core.session.length)
})

test('lowest common ancestor - long fork', async function (t) {
  const core = await create(t, 5)
  const clone = await create(t, 5)

  {
    const b = new MerkleTreeBatch(core.session)
    b.append(b4a.from('fork #1'))
    await flushBatch(core.session, b)
  }

  {
    const b = new MerkleTreeBatch(clone.session)
    b.append(b4a.from('fork #2'))
    await flushBatch(clone.session, b)
  }

  {
    const b = new MerkleTreeBatch(core.session)
    for (let i = 0; i < 100; i++) b.append(b4a.from('#' + i))
    await flushBatch(core.session, b)
  }

  {
    const b = new MerkleTreeBatch(clone.session)
    for (let i = 0; i < 100; i++) b.append(b4a.from('#' + i))
    await flushBatch(clone.session, b)
  }

  const ancestors = await reorg(clone, core)

  t.is(ancestors, 5)
  t.is(clone.session.length, core.session.length)

  t.ok(await audit(core))
  t.ok(await audit(clone))
})

test('tree hash', async function (t) {
  const a = await create(t, 5)
  const b = await create(t, 5)

  t.alike(MerkleTree.hash(a.session), MerkleTree.hash(b.session))

  {
    const ab = new MerkleTreeBatch(a.session)
    ab.append(b4a.from('hi'))
    await flushBatch(a.session, ab)
  }

  {
    const bb = new MerkleTreeBatch(b.session)
    bb.append(b4a.from('hi'))
    await flushBatch(b.session, bb)
  }

  t.alike(MerkleTree.hash(a.session), MerkleTree.hash(b.session))
})

test('basic tree seeks', async function (t) {
  const a = await create(t, 5)

  {
    const b = new MerkleTreeBatch(a.session)
    b.append(b4a.from('bigger'))
    b.append(b4a.from('block'))
    b.append(b4a.from('tiny'))
    b.append(b4a.from('s'))
    b.append(b4a.from('another'))

    await flushBatch(a.session, b)
  }

  t.is(a.session.length, 10)
  t.is(a.session.byteLength, 33)

  for (let i = 0; i < a.session.byteLength; i++) {
    const s = MerkleTree.seek(a.session, i)

    const actual = await s.update()
    const expected = await linearSeek(a.session, i)

    if (actual[0] !== expected[0] || actual[1] !== expected[1]) {
      t.is(actual, expected, 'bad seek at ' + i)
      return
    }
  }

  t.pass('checked all byte seeks')

  async function linearSeek (session, bytes) {
    for (let i = 0; i < session.length * 2; i += 2) {
      const node = await MerkleTree.get(session, i)
      if (node.size > bytes) return [i / 2, bytes]
      bytes -= node.size
    }
    return [session.length, bytes]
  }
})

test('clear full tree', async function (t) {
  const a = await create(t, 5)

  const tx = a.storage.write()
  tx.deleteTreeNodeRange(0, -1)
  await tx.flush()

  for (let i = 0; i < 5; i++) {
    t.is(await MerkleTree.get(a.session, i), null)
  }
})

test('get older roots', async function (t) {
  const a = await create(t, 5)

  const roots = await MerkleTree.getRoots(a.session, 5)
  t.alike(roots, a.session.roots, 'same roots')

  {
    const b = new MerkleTreeBatch(a.session)
    b.append(b4a.from('next'))
    b.append(b4a.from('next'))
    b.append(b4a.from('next'))

    await flushBatch(a.session, b)
  }

  const oldRoots = await MerkleTree.getRoots(a.session, 5)
  t.alike(oldRoots, roots, 'same old roots')

  const expected = []
  const len = a.session.length

  for (let i = 0; i < 40; i++) {
    expected.push(await MerkleTree.getRoots(a.session, len + i))
    {
      const b = new MerkleTreeBatch(a.session)
      b.append(b4a.from('tick'))

      await flushBatch(a.session, b)
    }
  }

  const actual = []

  for (let i = 0; i < 40; i++) {
    actual.push(await MerkleTree.getRoots(a.session, len + i))
  }

  t.alike(actual, expected, 'check a bunch of different roots')
})

test('check if a length is upgradeable', async function (t) {
  const { session, storage } = await create(t, 5)
  const clone = await create(t)

  // Full clone, has it all

  t.is(await MerkleTree.upgradeable(session, 0), true)
  t.is(await MerkleTree.upgradeable(session, 1), true)
  t.is(await MerkleTree.upgradeable(session, 2), true)
  t.is(await MerkleTree.upgradeable(session, 3), true)
  t.is(await MerkleTree.upgradeable(session, 4), true)
  t.is(await MerkleTree.upgradeable(session, 5), true)

  const batch = storage.read()
  const proof = await MerkleTree.proof(session, batch, {
    upgrade: { start: 0, length: 5 }
  })

  batch.tryFlush()
  const p = await proof.settle()

  const b = await MerkleTree.verify(clone.session, p, clone.session)
  await flushBatch(clone.session, b)

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

  t.is(await MerkleTree.upgradeable(clone.session, 0), true)
  t.is(await MerkleTree.upgradeable(clone.session, 1), false)
  t.is(await MerkleTree.upgradeable(clone.session, 2), false)
  t.is(await MerkleTree.upgradeable(clone.session, 3), false)
  t.is(await MerkleTree.upgradeable(clone.session, 4), true)
  t.is(await MerkleTree.upgradeable(clone.session, 5), true)
})

test('clone a batch', async t => {
  const a = await create(t, 5)

  const b = new MerkleTreeBatch(a.session)
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

  await flushBatch(a.session, b)

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
  const b = new MerkleTreeBatch(a.session)

  for (let i = 0; i < 16; i++) {
    b.append(b4a.from('tick tock'))
  }

  b.prune(15)

  const nodes = b.nodes.sort((a, b) => a.index - b.index).map(n => n.index)

  t.alike(nodes, [15, 23, 27, 29, 30])
})

test('checkout nodes in a batch', async t => {
  const a = await create(t, 0)
  const b = new MerkleTreeBatch(a.session)

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
  const { session } = await create(t)

  const b = new MerkleTreeBatch(session)

  for (let i = 0; i < 100; i++) {
    b.append(b4a.from([i]))
  }

  await flushBatch(session, b)

  const roots = b.roots
  t.is(roots.length > 1, true, 'sanity check')

  const rootByteLength = 32
  const buffer = roots[0].hash.buffer

  t.is(
    buffer.byteLength,
    rootByteLength,
    'unslabbed the first root'
  )
  t.is(
    roots[1].hash.buffer.byteLength,
    rootByteLength,
    'unslabbed the second root'
  )
  t.is(
    roots[2].hash.buffer.byteLength,
    rootByteLength,
    'unslabbed the third root'
  )
})

test.skip('buffer of cached nodes is copied to small slab', async function (t) {
  // RAM does not use slab-allocated memory,
  // so we need to us random-access-file to reproduce this issue
  const { session } = await create(t)

  const b = new MerkleTreeBatch(session)
  b.append(b4a.from('tree-entry'))

  await flushBatch(session, b)

  const node = await MerkleTree.get(session, 0)
  t.is(node.hash.buffer.byteLength, 32, 'created a new memory slab of the correct (small) size')
})

test('reopen a tree', async t => {
  const dir = await t.tmp()

  const a = await create(t, 16, dir)
  const b = new MerkleTreeBatch(a.session)

  for (let i = 0; i < 16; i++) {
    b.append(b4a.from('#' + i))
  }

  await flushBatch(a.session, b)

  t.alike(b.length, 32)

  const byteLength = MerkleTree.size(b.roots)

  t.alike(b.roots.map(n => n.index), [31])

  // fully close db
  await a.storage.db.close({ force: true })

  const a1 = await create(t, 0, dir, b.length)
  const roots = await MerkleTree.getRoots(a1.session, b.length)

  t.alike(MerkleTree.span(roots) / 2, 32)
  t.alike(MerkleTree.size(roots), byteLength)
  t.alike(roots.map(n => n.index), [31])
})

async function audit (core) {
  const flat = require('flat-tree')
  const expectedRoots = flat.fullRoots(core.session.length * 2)

  for (const root of core.session.roots) {
    if (expectedRoots.shift() !== root.index) return false
    if (!(await check(root))) return false
  }

  if (expectedRoots.length) return false

  return true

  async function check (node) {
    if ((node.index & 1) === 0) return true

    const [l, r] = flat.children(node.index)
    const nl = await MerkleTree.get(core.session, l, false)
    const nr = await MerkleTree.get(core.session, r, false)

    if (!nl && !nr) return true

    return b4a.equals(crypto.parent(nl, nr), node.hash) && await check(nl) && await check(nr)
  }
}

async function reorg (local, remote) {
  const upgrade = { start: 0, length: remote.session.length }

  const batch = remote.storage.read()
  const proof = await MerkleTree.proof(remote.session, batch, { upgrade })

  batch.tryFlush()
  const localBatch = new ReorgBatch(local.session)
  const r = await MerkleTree.reorg(local.session, await proof.settle(), localBatch)

  while (!r.finished) {
    const index = 2 * (r.want.end - 1)
    const nodes = r.want.nodes

    const batch = remote.storage.read()
    const proof = await MerkleTree.proof(remote.session, batch, { hash: { index, nodes } })

    batch.tryFlush()

    await r.update(await proof.settle())
  }

  await flushBatch(local.session, r)

  return r.ancestors
}

async function create (t, length = 0, dir, resume = 0) {
  if (!dir) dir = await t.tmp()

  const db = new CoreStorage(dir)

  t.teardown(() => db.close())

  const dkey = b4a.alloc(32)

  const storage = (await db.resume(dkey)) || (await db.create({ key: dkey, discoveryKey: dkey }))

  const session = createSession(storage)

  if (!length) return { session, storage }

  const b = new MerkleTreeBatch(session)
  for (let i = 0; i < length; i++) {
    b.append(b4a.from('#' + i))
  }

  await flushBatch(session, b)

  return { session, storage }
}

function createSession (storage) {
  return {
    storage,
    fork: 0,
    roots: [],
    length: 0,
    byteLength: 0,
    signature: null
  }
}

async function flushBatch (session, batch) {
  const tx = session.storage.write()
  batch.commit(tx)
  await tx.flush()

  session.fork = batch.fork
  session.length = batch.length
  session.byteLength = batch.byteLength
  session.roots = [...batch.roots]

  if (batch.signature) session.signature = batch.signature
}
