const test = require('brittle')
const b4a = require('b4a')

const NS = b4a.alloc(32)
const { create, replicate, eventFlush } = require('./helpers')

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

test('batch has', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  await b.append(['de', 'fg'])

  for (let i = 0; i < b.length; i++) {
    t.ok(await b.has(i))
  }
})

test('append to core during batch', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  await core.append('d')
  await b.append('e')
  t.absent(await b.flush())

  t.is(core.length, 4)
})

test('append to session during batch, create before batch', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const s = core.session()
  const b = core.batch()
  await b.append('d')
  await s.append('d')

  t.ok(await b.flush())
  t.is(s.length, 4)
})

test('append to session during batch, create after batch', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  await b.append('d')
  const s = core.session()
  await s.append('d')

  t.ok(await b.flush())
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
  await b.append('a')
  await core.truncate(2)
  t.absent(await b.flush())
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
  const c = core.batch()
  const d = core.batch()

  await b.append('a')
  await c.append(['a', 'c'])
  await d.append('c')

  t.ok(await b.flush())
  t.ok(await c.flush())
  t.absent(await d.flush())
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

  await b.flush({ length: 2 })

  t.is(core.length, 2)
  t.is(b.length, 4)
  t.is(b.byteLength, 4)

  await b.flush({ length: 3 })

  t.is(core.length, 3)
  t.is(b.length, 4)
  t.is(b.byteLength, 4)

  await b.flush({ length: 4 })

  t.is(core.length, 4)
  t.is(b.length, 4)
  t.is(b.byteLength, 4)

  await b.flush({ length: 4 })

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

  const t1 = b.createTreeBatch(1)
  const t2 = b.createTreeBatch(2)
  const t3 = b.createTreeBatch(3)
  const t4 = b.createTreeBatch(4, blocks)
  const t5 = b.createTreeBatch(5, blocks)

  t.is(t1.length, 1)
  t.is(t2.length, 2)
  t.is(t3.length, 3)
  t.is(t4.length, 4)
  t.is(t5.length, 5)

  t2.append(b4a.from('c'))

  t.alike(t3.signable(NS), t2.signable(NS))

  const t4s = t4.signable(NS)

  await b.append('d')
  t.alike(b.createTreeBatch().signable(NS), t4s)

  await b.append('e')
  t.alike(b.createTreeBatch().signable(NS), t5.signable(NS))

  // remove appended values
  blocks.shift()
  blocks.shift()

  t.absent(b.createTreeBatch(6))
  t.absent(b.createTreeBatch(8, blocks))

  await b.flush()

  t.is(core.length, 5)

  const b2 = core.batch()
  await b2.ready()

  t.absent(b2.createTreeBatch(3))
  t.alike(t4.signable(NS), t4s)

  const t6 = b2.createTreeBatch(6, blocks)
  const t7 = b2.createTreeBatch(7, blocks)

  t.is(t6.length, 6)
  t.is(t7.length, 7)

  await b2.append('f')
  t.alike(b2.createTreeBatch().signable(NS), t6.signable(NS))

  await b2.append('g')
  t.alike(b2.createTreeBatch().signable(NS), t7.signable(NS))
})

test('flush with bg activity', async function (t) {
  const core = await create()
  const clone = await create(core.key)

  replicate(core, clone, t)

  await core.append('a')
  await clone.get(0)

  const b = clone.batch({ autoClose: false })

  // bg
  await core.append('b')
  await clone.get(1)

  await core.append('c')
  await clone.get(2)

  await b.append('b')

  t.absent(await b.flush(), 'core is ahead, not flushing')

  await b.append('c')

  t.ok(await b.flush(), 'flushed!')
})

test('flush with bg activity persists non conflicting values', async function (t) {
  const core = await create()
  const clone = await create(core.key)

  replicate(core, clone, t)

  await core.append('a')
  await clone.get(0)

  const b = clone.batch()

  // bg
  await core.append('b')
  await core.append('c')

  await b.append('b')
  await b.append('c')

  await eventFlush()

  t.ok(await b.flush(), 'flushed!')

  t.alike(await clone.get(0, { wait: false }), b4a.from('a'))
  t.alike(await clone.get(1, { wait: false }), b4a.from('b'))
  t.alike(await clone.get(2, { wait: false }), b4a.from('c'))

  t.is(b.byteLength, clone.byteLength)
  t.is(b.indexedLength, b.length, 'nothing buffered')
})

test('flush with conflicting bg activity', async function (t) {
  const core = await create()
  const clone = await create(core.key)

  replicate(core, clone, t)

  await core.append('a')
  await clone.get(0)

  const b = clone.batch({ autoClose: false })

  // bg
  await core.append('b')
  await clone.get(1)

  await core.append('c')
  await clone.get(2)

  await b.append('c')
  await b.append('c')

  t.absent(await b.flush(), 'cannot flush a batch with conflicts')
})

test('checkout batch', async function (t) {
  const core = await create()

  await core.append(['a', 'b'])
  const hash = core.createTreeBatch().hash()
  await core.append(['c', 'd'])

  const b = core.batch({ checkout: 2, autoClose: false })

  await b.ready()

  t.is(b.length, 2)
  t.is(b.byteLength, 2)

  const batch = b.createTreeBatch()
  t.alike(batch.hash(), hash)

  await b.append(['c', 'z'])
  t.absent(await b.flush())

  await b.truncate(3, b.fork)
  await b.append('d')
  t.ok(await b.flush())
})

test('encryption and batches', async function (t) {
  const core = await create({ encryptionKey: b4a.alloc(32) })

  await core.append(['a', 'b'])
  const batch = core.batch()

  t.alike(await batch.get(0), b4a.from('a'))
  t.alike(await batch.get(1), b4a.from('b'))

  const pre = batch.createTreeBatch(3, [b4a.from('c')])
  await batch.append('c')
  const post = batch.createTreeBatch(3)

  t.is(batch.byteLength, 3)
  t.alike(await batch.get(2), b4a.from('c'))

  await batch.flush()

  t.is(core.byteLength, 3)
  t.is(core.length, 3)

  t.alike(await core.get(2), b4a.from('c'))

  const final = core.createTreeBatch()

  t.alike(pre.hash(), final.hash())
  t.alike(post.hash(), final.hash())
})

test('encryption and bigger batches', async function (t) {
  const core = await create({ encryptionKey: b4a.alloc(32) })

  await core.append(['a', 'b'])
  const batch = core.batch()

  t.alike(await batch.get(0), b4a.from('a'))
  t.alike(await batch.get(1), b4a.from('b'))

  const pre = batch.createTreeBatch(5, [b4a.from('c'), b4a.from('d'), b4a.from('e')])
  await batch.append(['c', 'd', 'e'])
  const post = batch.createTreeBatch(5)

  t.is(batch.byteLength, 5)
  t.alike(await batch.get(2), b4a.from('c'))
  t.alike(await batch.get(3), b4a.from('d'))
  t.alike(await batch.get(4), b4a.from('e'))

  await batch.flush()

  t.is(core.byteLength, 5)
  t.is(core.length, 5)

  t.alike(await core.get(2), b4a.from('c'))
  t.alike(await core.get(3), b4a.from('d'))
  t.alike(await core.get(4), b4a.from('e'))

  const final = core.createTreeBatch()

  t.alike(pre.hash(), final.hash())
  t.alike(post.hash(), final.hash())
})

test('persistent batch', async function (t) {
  const core = await create()

  await core.append(['a', 'b', 'c'])

  let batch = core.batch()

  await batch.ready()
  await batch.append(['d', 'e', 'f'])
  await batch.flush({ pending: true })

  batch = core.batch({ restore: true, autoClose: false })

  await batch.ready()

  t.is(batch.length, 6)
  t.is(batch.byteLength, 6)
  t.is(batch.indexedLength, 3)
  t.alike(await batch.seek(4), [4, 0])

  const clone = await create(core.key)

  replicate(core, clone, t)

  clone.download()

  await t.test('download', async function (sub) {
    const downloaded = []
    clone.on('download', function (index) {
      downloaded.push(index)
    })
    await eventFlush()
    sub.alike(downloaded.sort(), [0, 1, 2], 'got non pending blocks')
  })

  await batch.flush({ length: 5 })

  t.is(core.length, 5)

  await t.test('download', async function (sub) {
    const downloaded = []
    clone.on('download', function (index) {
      downloaded.push(index)
    })
    await eventFlush()
    sub.alike(downloaded.sort(), [3, 4], 'got non pending blocks')
  })

  await batch.flush({ length: 6 })

  t.is(core.length, 6)

  await t.test('download', async function (sub) {
    const downloaded = []
    clone.on('download', function (index) {
      downloaded.push(index)
    })
    await eventFlush()
    sub.alike(downloaded.sort(), [5], 'got non pending blocks')
  })

  await batch.append('g')

  t.is(batch.length, 7)

  await batch.flush({ pending: true })

  t.is(core.length, 6)

  await batch.append('h')

  t.is(batch.length, 8)

  await batch.flush({ pending: true })

  t.is(batch.length, 8)

  t.is(core.length, 6)

  await batch.flush()

  t.is(batch.length, 8)
  t.is(core.length, 8)

  await t.test('download', async function (sub) {
    const downloaded = []
    clone.on('download', function (index) {
      downloaded.push(index)
    })
    await eventFlush()
    sub.alike(downloaded.sort(), [6, 7], 'got non pending blocks')
  })
})

test('clear', async function (t) {
  const core = await create()

  await core.append('hello')

  const clone = await create(core.key)

  const b = clone.batch()

  await b.append('hello')
  await b.flush()
  await b.close()

  const [s1, s2] = replicate(core, clone, t)

  await eventFlush()
  t.ok(!!(await clone.get(0)), 'got block 0 proof')

  s1.destroy()
  s2.destroy()

  const b1 = clone.batch()
  await b1.ready()
  await b1.append('foo')
  await b1.flush()
  await b1.close()

  t.is(clone.length, 1, 'clone length is still 1')

  const b2 = clone.batch({ clear: true })
  await b2.ready()

  t.is(b2.length, 1, 'reset the batch')
})

test('copy from with encrypted batch', async function (t) {
  const encryptionKey = b4a.alloc(32, 2)

  const core = await create({ encryptionKey })

  const blocks = 290

  const b = core.batch({ autoClose: false })

  for (let i = 0; i < blocks; i++) {
    await b.append('block' + i)
  }

  await b.flush({ keyPair: null })

  t.is(core.length, 0)
  t.is(b._sessionLength, blocks)

  const manifest = {
    prologue: {
      length: b._sessionLength,
      hash: b.createTreeBatch().hash()
    },
    encryptionKey
  }

  const clone = await create({
    manifest,
    encryptionKey
  })

  const tree = clone.core.tree.batch()

  for (let i = 0; i < blocks; i++) {
    await tree.append(await b.get(i, { raw: true }))
  }

  t.alike(tree.hash(), manifest.prologue.hash)
})
