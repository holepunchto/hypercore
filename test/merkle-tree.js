const tape = require('tape')
const Tree = require('../lib/merkle-tree')
const ram = require('random-access-memory')

tape('nodes', async function (t) {
  const tree = await create()

  const b = tree.batch()

  for (let i = 0; i < 8; i++) {
    b.append(Buffer.from([i]))
  }

  b.commit()

  t.same(await tree.nodes(0), 0)
  t.end()
})

tape('proof only block', async function (t) {
  const tree = await create(10)

  const proof = await tree.proof({
    block: { index: 4, nodes: 2, value: true }
  })

  t.same(proof.upgrade, null)
  t.same(proof.seek, null)
  t.same(proof.block.index, 4)
  t.same(proof.block.nodes.length, 2)
  t.same(proof.block.nodes.map(n => n.index), [10, 13])

  t.end()
})

tape('proof with upgrade', async function (t) {
  const tree = await create(10)

  const proof = await tree.proof({
    block: { index: 4, nodes: 0, value: true },
    upgrade: { start: 0, length: 10 }
  })

  t.same(proof.seek, null)
  t.same(proof.block.index, 4)
  t.same(proof.block.nodes.length, 3)
  t.same(proof.block.nodes.map(n => n.index), [10, 13, 3])
  t.same(proof.upgrade.start, 0)
  t.same(proof.upgrade.length, 10)
  t.same(proof.upgrade.nodes.map(n => n.index), [17])
  t.same(proof.upgrade.additionalNodes.map(n => n.index), [])

  t.end()
})

tape('proof with upgrade + additional', async function (t) {
  const tree = await create(10)

  const proof = await tree.proof({
    block: { index: 4, nodes: 0, value: true },
    upgrade: { start: 0, length: 8 }
  })

  t.same(proof.seek, null)
  t.same(proof.block.index, 4)
  t.same(proof.block.nodes.length, 3)
  t.same(proof.block.nodes.map(n => n.index), [10, 13, 3])
  t.same(proof.upgrade.start, 0)
  t.same(proof.upgrade.length, 8)
  t.same(proof.upgrade.nodes.map(n => n.index), [])
  t.same(proof.upgrade.additionalNodes.map(n => n.index), [17])

  t.end()
})

tape('proof with upgrade from existing state', async function (t) {
  const tree = await create(10)

  const proof = await tree.proof({
    block: { index: 1, nodes: 0, value: true },
    upgrade: { start: 1, length: 9 }
  })

  t.same(proof.seek, null)
  t.same(proof.block.index, 1)
  t.same(proof.block.nodes.length, 0)
  t.same(proof.block.nodes.map(n => n.index), [])
  t.same(proof.upgrade.start, 1)
  t.same(proof.upgrade.length, 9)
  t.same(proof.upgrade.nodes.map(n => n.index), [5, 11, 17])
  t.same(proof.upgrade.additionalNodes.map(n => n.index), [])

  t.end()
})

tape('proof with upgrade from existing state + additional', async function (t) {
  const tree = await create(10)

  const proof = await tree.proof({
    block: { index: 1, nodes: 0, value: true },
    upgrade: { start: 1, length: 5 }
  })

  t.same(proof.seek, null)
  t.same(proof.block.index, 1)
  t.same(proof.block.nodes.length, 0)
  t.same(proof.block.nodes.map(n => n.index), [])
  t.same(proof.upgrade.start, 1)
  t.same(proof.upgrade.length, 5)
  t.same(proof.upgrade.nodes.map(n => n.index), [5, 9])
  t.same(proof.upgrade.additionalNodes.map(n => n.index), [13, 17])

  t.end()
})

tape('proof block and seek, no upgrade', async function (t) {
  const tree = await create(10)

  const proof = await tree.proof({
    seek: { bytes: 8 },
    block: { index: 4, nodes: 2, value: true }
  })

  t.same(proof.upgrade, null)
  t.same(proof.seek, null) // seek included in the block
  t.same(proof.block.index, 4)
  t.same(proof.block.nodes.length, 2)
  t.same(proof.block.nodes.map(n => n.index), [10, 13])

  t.end()
})

tape('proof block and seek #2, no upgrade', async function (t) {
  const tree = await create(10)

  const proof = await tree.proof({
    seek: { bytes: 10 },
    block: { index: 4, nodes: 2, value: true }
  })

  t.same(proof.upgrade, null)
  t.same(proof.seek, null) // seek included in the block
  t.same(proof.block.index, 4)
  t.same(proof.block.nodes.length, 2)
  t.same(proof.block.nodes.map(n => n.index), [10, 13])

  t.end()
})

tape('proof block and seek #3, no upgrade', async function (t) {
  const tree = await create(10)

  const proof = await tree.proof({
    seek: { bytes: 13 },
    block: { index: 4, nodes: 2, value: true }
  })

  t.same(proof.upgrade, null)
  t.same(proof.seek.nodes.map(n => n.index), [12, 14])
  t.same(proof.block.index, 4)
  t.same(proof.block.nodes.length, 1)
  t.same(proof.block.nodes.map(n => n.index), [10])

  t.end()
})

tape('proof block and seek that results in tree, no upgrade', async function (t) {
  const tree = await create(16)

  const proof = await tree.proof({
    seek: { bytes: 26 },
    block: { index: 0, nodes: 4, value: true }
  })

  t.same(proof.upgrade, null)
  t.same(proof.block.nodes.map(n => n.index), [2, 5, 11])
  t.same(proof.seek.nodes.map(n => n.index), [19, 27])

  t.end()
})

tape('proof block and seek, with upgrade', async function (t) {
  const tree = await create(10)

  const proof = await tree.proof({
    seek: { bytes: 13 },
    block: { index: 4, nodes: 2, value: true },
    upgrade: { start: 8, length: 2 }
  })

  t.same(proof.seek.nodes.map(n => n.index), [12, 14])
  t.same(proof.block.index, 4)
  t.same(proof.block.nodes.length, 1)
  t.same(proof.block.nodes.map(n => n.index), [10])
  t.same(proof.upgrade.start, 8)
  t.same(proof.upgrade.length, 2)
  t.same(proof.upgrade.nodes.map(n => n.index), [17])
  t.same(proof.upgrade.additionalNodes.map(n => n.index), [])

  t.end()
})

tape('proof seek with upgrade', async function (t) {
  const tree = await create(10)

  const proof = await tree.proof({
    seek: { bytes: 13 },
    upgrade: { start: 0, length: 10 }
  })

  t.same(proof.seek.nodes.map(n => n.index), [12, 14, 9, 3])
  t.same(proof.block, null)
  t.same(proof.upgrade.start, 0)
  t.same(proof.upgrade.length, 10)
  t.same(proof.upgrade.nodes.map(n => n.index), [17])
  t.same(proof.upgrade.additionalNodes.map(n => n.index), [])

  t.end()
})

tape('verify proof #1', async function (t) {
  const tree = await create(10)
  const clone = await create()

  const p = await tree.proof({
    block: { index: 3 },
    upgrade: { start: 0, length: 10 }
  })

  const b = await clone.verify(p)
  b.commit()

  t.same(clone.length, tree.length)
  t.same(clone.byteLength, tree.byteLength)
  t.same(await clone.byteOffset(6), await tree.byteOffset(6))
  t.same(await clone.get(6), await tree.get(6))

  t.end()
})

tape('verify proof #2', async function (t) {
  const tree = await create(10)
  const clone = await create()

  const p = await tree.proof({
    seek: { bytes: 10 },
    upgrade: { start: 0, length: 10 }
  })

  const b = await clone.verify(p)
  b.commit()

  t.same(clone.length, tree.length)
  t.same(clone.byteLength, tree.byteLength)
  t.same(await clone.byteRange(10), await tree.byteRange(10))

  t.end()
})

tape('upgrade edgecase when no roots need upgrade', async function (t) {
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
  b.append(Buffer.from('#5'))
  b.commit()

  {
    const proof = await tree.proof({
      upgrade: { start: 4, length: 1 }
    })

    const b = await clone.verify(proof)
    b.commit()
  }

  t.same(tree.length, 5)
  t.end()
})

tape('lowest common ancestor - small gap', async function (t) {
  const tree = await create(10)
  const clone = await create(8)
  const ancestors = await reorg(clone, tree)

  t.same(ancestors, 8)
  t.same(clone.length, tree.length)
  t.end()
})

tape('lowest common ancestor - bigger gap', async function (t) {
  const tree = await create(20)
  const clone = await create(1)
  const ancestors = await reorg(clone, tree)

  t.same(ancestors, 1)
  t.same(clone.length, tree.length)
  t.end()
})

tape('lowest common ancestor - remote is shorter than local', async function (t) {
  const tree = await create(5)
  const clone = await create(10)
  const ancestors = await reorg(clone, tree)

  t.same(ancestors, 5)
  t.same(clone.length, tree.length)
  t.end()
})

tape('lowest common ancestor - simple fork', async function (t) {
  const tree = await create(5)
  const clone = await create(5)

  {
    const b = tree.batch()
    b.append(Buffer.from('fork #1'))
    b.commit()
  }

  {
    const b = clone.batch()
    b.append(Buffer.from('fork #2'))
    b.commit()
  }

  const ancestors = await reorg(clone, tree)

  t.same(ancestors, 5)
  t.same(clone.length, tree.length)
  t.end()
})

tape('lowest common ancestor - long fork', async function (t) {
  const tree = await create(5)
  const clone = await create(5)

  {
    const b = tree.batch()
    b.append(Buffer.from('fork #1'))
    b.commit()
  }

  {
    const b = clone.batch()
    b.append(Buffer.from('fork #2'))
    b.commit()
  }

  {
    const b = tree.batch()
    for (let i = 0; i < 100; i++) b.append(Buffer.from('#' + i))
    b.commit()
  }

  {
    const b = clone.batch()
    for (let i = 0; i < 100; i++) b.append(Buffer.from('#' + i))
    b.commit()
  }

  const ancestors = await reorg(clone, tree)

  t.same(ancestors, 5)
  t.same(clone.length, tree.length)

  t.ok(await audit(tree))
  await tree.flush()
  t.ok(await audit(tree))

  t.end()
})

tape('tree hash', async function (t) {
  const a = await create(5)
  const b = await create(5)

  t.same(a.hash(), b.hash())

  {
    const b = a.batch()
    t.same(b.hash(), a.hash())
    b.append(Buffer.from('hi'))
    const h = b.hash()
    t.notEqual(h, a.hash())
    b.commit()
    t.same(h, a.hash())
  }

  {
    const ba = b.batch()
    ba.append(Buffer.from('hi'))
    const h = ba.hash()
    t.notEqual(h, b.hash())
    t.same(h, a.hash())
    ba.commit()
    t.same(h, b.hash())
  }

  t.end()
})

tape('basic tree seeks', async function (t) {
  const a = await create(5)

  {
    const b = a.batch()
    b.append(Buffer.from('bigger'))
    b.append(Buffer.from('block'))
    b.append(Buffer.from('tiny'))
    b.append(Buffer.from('s'))
    b.append(Buffer.from('another'))
    b.commit()
  }

  t.same(a.length, 10)
  t.same(a.byteLength, 33)

  for (let i = 0; i < a.byteLength; i++) {
    const s = a.seek(i)

    const actual = await s.update()
    const expected = await linearSeek(a, i)

    if (actual[0] !== expected[0] || actual[1] !== expected[1]) {
      t.same(actual, expected, 'bad seek at ' + i)
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

    return tree.crypto.parent(nl, nr).equals(node.hash) && await check(nl) && await check(nr)
  }
}

async function reorg (local, remote) {
  const upgrade = { start: 0, length: remote.length }
  const r = await local.reorg(await remote.proof({ upgrade }))

  while (!r.finished) {
    const index = r.want.end - 1
    const nodes = r.want.nodes

    await r.update(await remote.proof({ block: { index, nodes } }))
  }

  r.commit()
  return r.ancestors
}

async function create (length = 0) {
  const tree = await Tree.open(ram())
  const b = tree.batch()
  for (let i = 0; i < length; i++) {
    b.append(Buffer.from('#' + i))
  }
  b.commit()
  return tree
}
