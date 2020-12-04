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

  const b = clone.batch()
  await b.verify(p)
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

  const b = clone.batch()
  await b.verify(p)
  b.commit()

  t.same(clone.length, tree.length)
  t.same(clone.byteLength, tree.byteLength)
  t.same(await clone.byteRange(10), await tree.byteRange(10))

  t.end()
})

async function create (length = 0) {
  const tree = await Tree.open(ram())
  const b = tree.batch()
  for (let i = 0; i < length; i++) {
    b.append(Buffer.from('#' + i))
  }
  b.commit()
  return tree
}
