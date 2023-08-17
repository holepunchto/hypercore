const test = require('brittle')
const b4a = require('b4a')

const { create } = require('./helpers')

test('batch append', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  const info = await b.append(['de', 'fg'])

  t.is(core.length, 3)
  t.alike(info, { length: 5, byteLength: 7 })

  t.alike(await b.get(3), b4a.from('de'))
  t.alike(await b.get(4), b4a.from('fg'))

  await b.flush()
  t.is(core.length, 5)
})

test('append to core during batch', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  await t.exception(core.append('d'))
  await b.flush()

  await core.append('d')
  t.is(core.length, 4)
})

test('append to session during batch, create before batch', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const s = core.session()
  const b = core.batch()
  await t.exception(s.append('d'))
  await b.flush()

  await s.append('d')
  t.is(s.length, 4)
})

test('append to session during batch, create after batch', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  const s = core.session()
  await t.exception(s.append('d'))
  await b.flush()

  await s.append('d')
  t.is(s.length, 4)
})

test('batch truncate', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  await b.append(['de', 'fg'])
  await b.truncate(4)

  t.alike(await b.get(3), b4a.from('de'))
  await t.exception(b.get(4))

  await b.flush()
  t.is(core.length, 4)
})

test('truncate core during batch', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  await t.exception(core.truncate(2))
  await b.flush()

  await core.truncate(2)
  t.is(core.length, 2)
})

test('batch truncate committed', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  await b.append(['de', 'fg'])
  await t.exception(b.truncate(2))
})

test('batch close', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  await b.append(['de', 'fg'])
  await b.close()
  t.is(core.length, 3)

  await core.append(['d', 'e'])
  t.is(core.length, 5)
})

test('batch close after flush', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  await b.flush()
  await b.close()
})

test('batch flush after close', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  await b.close()
  await t.exception(b.flush())
})

test('batch info', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  await b.append(['de', 'fg'])

  const info = await b.info()
  t.is(info.length, 5)
  t.is(info.contiguousLength, 5)
  t.is(info.byteLength, 7)
  t.unlike(await core.info(), info)

  await b.flush()
  t.alike(await core.info(), info)
})

test('simultaneous batches', async function (t) {
  const core = await create()

  const b = core.batch()
  await t.exception(() => core.batch())
  await b.flush()
})

test('multiple batches', async function (t) {
  const core = await create()
  const session = core.session()

  const b = core.batch()
  await b.append('a')
  await b.flush()

  const b2 = session.batch()
  await b2.append('b')
  await b2.flush()

  t.is(core.length, 2)
})

test('partial flush', async function (t) {
  const core = await create()

  const b = core.batch({ autoClose: false })

  await b.append(['a', 'b', 'c', 'd'])

  await b.flush(2)

  t.is(core.length, 2)
  t.is(b.length, 4)
  t.is(b.byteLength, 4)

  await b.flush(1)

  t.is(core.length, 3)
  t.is(b.length, 4)
  t.is(b.byteLength, 4)

  await b.flush(1)

  t.is(core.length, 4)
  t.is(b.length, 4)
  t.is(b.byteLength, 4)

  await b.flush(1)

  t.is(core.length, 4)
  t.is(b.length, 4)
  t.is(b.byteLength, 4)

  await b.close()
})

test('can make a tree batch', async function (t) {
  const core = await create()

  const b = core.batch()

  await b.append('a')

  const batchTreeBatch = b.createTreeBatch()
  const batchHash = batchTreeBatch.hash()

  await b.flush()

  const treeBatch = core.createTreeBatch()
  const hash = treeBatch.hash()

  t.alike(hash, batchHash)
})

test('batched tree batch contains new nodes', async function (t) {
  const core = await create()

  const b = core.batch()

  await b.append('a')

  const batchTreeBatch = b.createTreeBatch()
  const batchNode = await batchTreeBatch.get(0)

  await b.flush()

  const treeBatch = core.createTreeBatch()
  const node = await treeBatch.get(0)

  t.alike(node, batchNode)
})

test('batched tree batch proofs are equivalent', async function (t) {
  const core = await create()

  const b = core.batch()

  await b.append(['a', 'b', 'c'])

  const batchTreeBatch = b.createTreeBatch()
  const batchProof = await batchTreeBatch.proof({ upgrade: { start: 0, length: 2 } })

  await b.flush()

  const treeBatch = core.createTreeBatch()
  const proof = await treeBatch.proof({ upgrade: { start: 0, length: 2 } })
  const treeProof = await core.core.tree.proof({ upgrade: { start: 0, length: 2 } })

  treeProof.upgrade.signature = null

  t.alike(proof, batchProof)
  t.alike(treeProof, batchProof)
})

test('create tree batches', async function (t) {
  const core = await create()

  const b = core.batch()

  await b.append('a')
  await b.append('b')
  await b.append('c')

  const blocks = [
    b4a.from('d'),
    b4a.from('e'),
    b4a.from('f'),
    b4a.from('g')
  ]

  const t1 = b.createTreeBatch(null, 1)
  const t2 = b.createTreeBatch(null, 2)
  const t3 = b.createTreeBatch(null, 3)
  const t4 = b.createTreeBatch(blocks, 4)
  const t5 = b.createTreeBatch(blocks, 5)

  t.is(t1.length, 1)
  t.is(t2.length, 2)
  t.is(t3.length, 3)
  t.is(t4.length, 4)
  t.is(t5.length, 5)

  t2.append(b4a.from('c'))

  t.alike(t3.signable(), t2.signable())

  const t4s = t4.signable()

  await b.append('d')
  t.alike(b.createTreeBatch().signable(), t4s)

  await b.append('e')
  t.alike(b.createTreeBatch().signable(), t5.signable())

  // remove appended values
  blocks.shift()
  blocks.shift()

  t.absent(b.createTreeBatch(null, 6))
  t.absent(b.createTreeBatch(blocks, 8))

  await b.flush()

  t.is(core.length, 5)

  const b2 = core.batch()
  await b2.ready()

  t.absent(b2.createTreeBatch(null, 3))
  t.alike(t4.signable(), t4s)

  const t6 = b2.createTreeBatch(blocks, 6)
  const t7 = b2.createTreeBatch(blocks, 7)

  t.is(t6.length, 6)
  t.is(t7.length, 7)

  await b2.append('f')
  t.alike(b2.createTreeBatch().signable(), t6.signable())

  await b2.append('g')
  t.alike(b2.createTreeBatch().signable(), t7.signable())
})
