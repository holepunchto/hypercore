const test = require('brittle')
const createTempDir = require('test-tmp')
const b4a = require('b4a')

const Hypercore = require('../')
const { create, createStorage, replicate } = require('./helpers')

const NS = b4a.alloc(32)

test('batch append', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c'])

  const b = core.session({ name: 'batch' })
  await b.ready() // todo: we shouldn't have to wait for ready

  t.unlike(b.state, core.state)

  const info = await b.append(['de', 'fg'])

  t.is(core.length, 3)

  t.is(b.length, 5)
  t.alike(info, { length: 5, byteLength: 7 })

  t.alike(await b.get(3), b4a.from('de'))
  t.alike(await b.get(4), b4a.from('fg'))

  t.is(core.length, 3)

  await core.commit(b.state)

  t.is(core.length, 5)

  await b.close()
})

test('batch has', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c'])

  const b = core.session({ name: 'batch' })
  await b.append(['de', 'fg'])

  for (let i = 0; i < b.length; i++) {
    t.ok(await b.has(i))
  }

  await b.close()
})

test.skip('append to core during batch', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c'])

  const b = core.session({ name: 'batch' })
  await core.append('d')
  await b.append('e')
  t.absent(await b.flush())

  t.is(core.length, 4)

  await b.close()
})

test('append to session during batch, create before batch', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c'])

  const s = core.session()
  const b = core.session({ name: 'batch' })
  await b.append('d')
  await s.append('d')

  t.ok(await core.commit(b.state))
  t.is(s.length, 4)

  await b.close()
  await s.close()
})

test('append to session during batch, create after batch', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c'])

  const b = core.session({ name: 'batch' })
  await b.append('d')
  const s = core.session()
  await s.append('d')

  t.ok(await core.commit(b.state))
  t.is(s.length, 4)

  await s.close()
  await b.close()
})

test('batch truncate', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c'])

  const b = core.session({ name: 'batch' })
  await b.append(['de', 'fg'])
  await b.truncate(4, { fork: 0 })

  t.alike(await b.get(3), b4a.from('de'))
  t.alike(await b.get(4, { wait: false }), null)

  await core.commit(b.state)
  t.is(core.length, 4)

  await b.close()
})

test('truncate core during batch', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c'])

  const b = core.session({ name: 'batch' })
  await b.append('a')
  await core.truncate(2)
  await t.exception(core.commit(b.state))
  t.is(core.length, 2)

  await b.close()
})

test.skip('batch truncate committed', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c'])

  const b = core.session({ name: 'batch' })
  await b.append(['de', 'fg'])
  await t.exception(b.truncate(2))

  await b.close()
})

test('batch close', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c'])

  const b = core.session({ name: 'batch' })
  await b.append(['de', 'fg'])
  await b.close()
  t.is(core.length, 3)

  await core.append(['d', 'e'])
  t.is(core.length, 5)
})

test('batch close after flush', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c'])

  const b = core.session({ name: 'batch' })
  await b.ready()

  await core.commit(b.state)
  await b.close()
})

test.skip('batch flush after close', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c'])

  const b = core.session({ name: 'batch' })
  await b.ready()

  await b.close()
  await t.exception(core.commit(b.state))
})

test.skip('batch info', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c'])

  const b = core.session({ name: 'batch' })
  await b.append(['de', 'fg'])

  const info = await b.info()
  t.is(info.length, 5)
  t.is(info.contiguousLength, 5)
  t.is(info.byteLength, 7)
  t.unlike(await core.info(), info)

  await core.commit(b.state)
  t.alike(await core.info(), info)

  await b.close()
})

test('simultaneous batches', async function (t) {
  const core = await create(t)

  const b = core.session({ name: '1' })
  const c = core.session({ name: '2' })
  const d = core.session({ name: '3' })

  await b.append('a')
  await c.append(['a', 'c'])
  await d.append('c')

  t.ok(await core.commit(b.state))
  t.ok(await core.commit(c.state))
  await t.exception(core.commit(d.state))

  await b.close()
  await c.close()
  await d.close()
})

test('multiple batches', async function (t) {
  const core = await create(t)
  const session = core.session()

  const b = core.session({ name: 'batch1' })
  await b.append('a')
  await core.commit(b.state)

  const b2 = session.session({ name: 'batch2' })
  await b2.append('b')
  await core.commit(b2.state)

  t.is(core.length, 2)

  await session.close()
  await b.close()
  await b2.close()
})

test.skip('partial flush', async function (t) {
  const core = await create(t)

  const b = core.session({ name: 'batch' })

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
  const core = await create(t)

  const b = core.session({ name: 'batch' })

  await b.append('a')

  const batchTreeBatch = b.createTreeBatch()
  const batchHash = batchTreeBatch.hash()

  await core.commit(b.state)

  const treeBatch = core.createTreeBatch()
  const hash = treeBatch.hash()

  t.alike(hash, batchHash)

  await b.close()
})

test('batched tree batch contains new nodes', async function (t) {
  const core = await create(t)

  const b = core.session({ name: 'batch' })

  await b.append('a')

  const batchTreeBatch = b.createTreeBatch()
  const batchNode = await batchTreeBatch.get(0)

  await core.commit(b.state)

  const treeBatch = core.createTreeBatch()
  const node = await treeBatch.get(0)

  t.alike(node, batchNode)

  await b.close()
})

test('batched tree batch proofs are equivalent', async function (t) {
  const core = await create(t)

  const b = core.session({ name: 'batch' })

  await b.append(['a', 'b', 'c'])

  const reader = b.state.storage.read()
  const batchTreeBatch = b.createTreeBatch()
  const batchProofIntermediate = await batchTreeBatch.proof(reader, { upgrade: { start: 0, length: 2 } })

  reader.tryFlush()

  const batchProof = await batchProofIntermediate.settle()

  await core.commit(b.state)

  const reader1 = core.state.storage.read()
  const treeBatch = core.createTreeBatch()
  const proofIntermediate = await treeBatch.proof(reader, { upgrade: { start: 0, length: 2 } })
  const treeProofIntermediate = await core.core.tree.proof(reader1, core.state.createTreeBatch(), { upgrade: { start: 0, length: 2 } })

  reader1.tryFlush()

  const proof = await proofIntermediate.settle()
  const treeProof = await treeProofIntermediate.settle()

  t.alike(proof, treeProof)

  treeProof.upgrade.signature = null

  t.alike(treeProof, batchProof)

  await b.close()
})

test.skip('create tree batches', async function (t) {
  const core = await create(t)

  const b = core.session({ name: 'batch' })

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

  await core.commit(b.state)

  t.is(core.length, 5)

  const b2 = core.session({ name: 'batch2' })
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

  await b.close()
  await b2.close()
})

test('flush with bg activity', async function (t) {
  const core = await create(t)
  const clone = await create(t, { keyPair: core.core.header.keyPair })

  replicate(core, clone, t)

  await core.append('a')
  await clone.get(0)

  const b = clone.session({ name: 'batch' })

  // bg
  await core.append('b')
  await clone.get(1)

  await core.append('c')
  await clone.get(2)

  await b.append('b')

  await t.exception(core.commit(b.state)) // core is ahead, not flushing

  await b.append('c')

  t.ok(await core.commit(b.state), 'flushed!')

  await b.close()
})

test('flush with bg activity persists non conflicting values', async function (t) {
  const core = await create(t)
  const clone = await create(t, core.key)

  replicate(core, clone, t)

  await core.append('a')
  await clone.get(0)

  const b = clone.session({ name: 'batch' })

  // bg
  const promise = new Promise(resolve => clone.on('append', resolve))

  await core.append('b')
  await core.append('c')

  await b.append('b')
  await b.append('c')

  await promise

  t.is(clone.length, 3)
  t.ok(await clone.commit(b.state), 'flushed!')

  t.alike(await clone.get(0, { wait: false }), b4a.from('a'))
  t.alike(await clone.get(1, { wait: false }), b4a.from('b'))
  t.alike(await clone.get(2, { wait: false }), b4a.from('c'))

  t.is(b.byteLength, clone.byteLength)
  t.is(b.signedLength, b.length, 'nothing buffered')

  await b.close()
})

test('flush with conflicting bg activity', async function (t) {
  const core = await create(t)
  const clone = await create(t, core.key)

  replicate(core, clone, t)

  await core.append('a')
  await clone.get(0)

  const b = clone.session({ name: 'batch' })

  // bg
  await core.append('b')
  await clone.get(1)

  await core.append('c')
  await clone.get(2)

  await b.append('c')
  await b.append('c')

  await t.exception(clone.commit(b.state)) // cannot flush a batch with conflicts

  await b.close()
})

test.skip('checkout batch', async function (t) {
  const core = await create(t)

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

  await b.close()
})

test('encryption and batches', async function (t) {
  const core = await create(t, { encryptionKey: b4a.alloc(32) })

  await core.append(['a', 'b'])
  const batch = core.session({ name: 'batch' })

  await batch.ready()

  t.alike(await batch.get(0), b4a.from('a'))
  t.alike(await batch.get(1), b4a.from('b'))

  // const pre = batch.createTreeBatch(3, [b4a.from('c')])
  await batch.append('c')
  const post = batch.createTreeBatch(3)

  t.is(batch.byteLength, 3)
  t.alike(await batch.get(2), b4a.from('c'))

  await core.commit(batch.state)

  t.is(core.byteLength, 3)
  t.is(core.length, 3)

  t.alike(await core.get(2), b4a.from('c'))

  const final = core.createTreeBatch()

  // t.alike(pre.hash(), final.hash())
  t.alike(post.hash(), final.hash())

  await batch.close()
})

test('encryption and bigger batches', async function (t) {
  const core = await create(t, { encryptionKey: b4a.alloc(32) })

  await core.append(['a', 'b'])
  const batch = core.session({ name: 'batch' })

  t.alike(await batch.get(0), b4a.from('a'))
  t.alike(await batch.get(1), b4a.from('b'))

  // const pre = batch.createTreeBatch(5, [b4a.from('c'), b4a.from('d'), b4a.from('e')])
  await batch.append(['c', 'd', 'e'])
  const post = batch.createTreeBatch(5)

  t.is(batch.byteLength, 5)
  t.alike(await batch.get(2), b4a.from('c'))
  t.alike(await batch.get(3), b4a.from('d'))
  t.alike(await batch.get(4), b4a.from('e'))

  await core.commit(batch.state)

  t.is(core.byteLength, 5)
  t.is(core.length, 5)

  t.alike(await core.get(2), b4a.from('c'))
  t.alike(await core.get(3), b4a.from('d'))
  t.alike(await core.get(4), b4a.from('e'))

  const final = core.createTreeBatch()

  // t.alike(pre.hash(), final.hash())
  t.alike(post.hash(), final.hash())

  await batch.close()
})

// test('persistent batch', async function (t) {
//   const core = await create(t)

//   await core.append(['a', 'b', 'c'])

//   let batch = core.session({ name: 'batch' })

//   await batch.ready()
//   await batch.append(['d', 'e', 'f'])
//   await batch.flush({ pending: true })

//   batch = core.batch({ restore: true, autoClose: false })

//   await batch.ready()

//   t.is(batch.length, 6)
//   t.is(batch.byteLength, 6)
//   t.is(batch.signedLength, 3)
//   t.alike(await batch.seek(4), [4, 0])

//   const clone = await create(t, core.key)

//   replicate(core, clone, t)

//   clone.download()

//   await t.test('download', async function (sub) {
//     const downloaded = []
//     clone.on('download', function (index) {
//       downloaded.push(index)
//     })
//     await eventFlush()
//     sub.alike(downloaded.sort(), [0, 1, 2], 'got non pending blocks')
//   })

//   await batch.flush({ length: 5 })

//   t.is(core.length, 5)

//   await t.test('download', async function (sub) {
//     const downloaded = []
//     clone.on('download', function (index) {
//       downloaded.push(index)
//     })
//     await eventFlush()
//     sub.alike(downloaded.sort(), [3, 4], 'got non pending blocks')
//   })

//   await batch.flush({ length: 6 })

//   t.is(core.length, 6)

//   await t.test('download', async function (sub) {
//     const downloaded = []
//     clone.on('download', function (index) {
//       downloaded.push(index)
//     })
//     await eventFlush()
//     sub.alike(downloaded.sort(), [5], 'got non pending blocks')
//   })

//   await batch.append('g')

//   t.is(batch.length, 7)

//   await batch.flush({ pending: true })

//   t.is(core.length, 6)

//   await batch.append('h')

//   t.is(batch.length, 8)

//   await batch.flush({ pending: true })

//   t.is(batch.length, 8)

//   t.is(core.length, 6)

//   await batch.flush()

//   t.is(batch.length, 8)
//   t.is(core.length, 8)

//   await t.test('download', async function (sub) {
//     const downloaded = []
//     clone.on('download', function (index) {
//       downloaded.push(index)
//     })
//     await eventFlush()
//     sub.alike(downloaded.sort(), [6, 7], 'got non pending blocks')
//   })
// })

test('persistent batch', async function (t) {
  const dir = await createTempDir()
  let storage = null

  const core = new Hypercore(await open())
  await core.ready()

  await core.append(['a', 'b', 'c'])

  const batch = core.session({ name: 'batch' })
  await batch.ready()

  await batch.append(['d', 'e', 'f'])

  t.is(batch.length, 6)
  t.is(batch.byteLength, 6)
  // t.is(batch.signedLength, 3)
  // t.alike(await batch.seek(4), [4, 0])

  await core.close()

  const reopen = new Hypercore(await open())
  await reopen.ready()

  const reopened = reopen.session({ name: 'batch' })
  await reopened.ready()

  t.is(reopened.length, 6)
  t.is(reopened.byteLength, 6)
  // t.is(batch.signedLength, 3)
  // t.alike(await batch.seek(4), [4, 0])

  await reopened.close()
  await reopen.close()

  async function open () {
    if (storage) await storage.close()
    storage = await createStorage(t, dir)
    return storage
  }
})

test('clear', async function (t) {
  const core = await create(t)

  await core.append('hello')

  const clone = await create(t, core.key)

  const b = clone.session({ name: 'b' })

  await b.append('hello')

  const [s1, s2] = replicate(core, clone, t)

  await new Promise(resolve => clone.on('append', resolve))

  await clone.commit(b.state)
  await b.close()

  t.ok(!!(await clone.get(0)), 'got block 0 proof')

  s1.destroy()
  s2.destroy()

  const b1 = clone.session({ name: 'b1' })
  await b1.ready()
  await b1.append('foo')
  await t.exception(clone.commit(b1.state))
  await b1.close()

  t.is(clone.length, 1, 'clone length is still 1')

  const b2 = clone.session({ name: 'b2' })
  await b2.ready()

  t.is(b2.length, 1, 'reset the batch')

  await b2.close()
})

test('copy from with encrypted batch', async function (t) {
  const encryptionKey = b4a.alloc(32, 2)

  const core = await create(t, { encryptionKey })

  const blocks = 290

  const b = core.session({ name: 'batch' })

  for (let i = 0; i < blocks; i++) {
    await b.append('block' + i)
  }

  t.is(core.length, 0)
  t.is(b.length, blocks)

  const manifest = {
    prologue: {
      length: b.length,
      hash: b.createTreeBatch().hash()
    },
    encryptionKey
  }

  const clone = await create(t, {
    manifest,
    encryptionKey
  })

  const tree = clone.core.state.createTreeBatch()

  for (let i = 0; i < blocks; i++) {
    await tree.append(await b.get(i, { raw: true }))
  }

  t.alike(tree.hash(), manifest.prologue.hash)

  await b.close()
})

test('batch append with huge batch', async function (t) {
  // Context: array.append(...otherArray) stops working after a certain amount of entries
  // due to a limit on the amount of function args
  // This caused a bug on large batches
  const core = await create(t)
  const bigBatch = (new Array(200_000)).fill('o')

  const b = core.session({ name: 'batch' })
  await b.append(bigBatch)

  // Actually flushing such a big batch takes multiple minutes
  // so we only ensure that nothing crashed while appending
  t.pass('Can append a big batch')

  await b.close()
})

test('batch does not append but reopens', async function (t) {
  const dir = await createTempDir(t)

  let core = new Hypercore(dir)

  await core.append('hello')

  let batch = core.session({ name: 'hello' })

  // open and close
  await batch.ready()
  await batch.close()

  await core.close()

  core = new Hypercore(dir)

  await core.append('hello')

  batch = core.session({ name: 'hello' })
  await batch.ready()

  t.is(core.length, 2)
  t.is(batch.length, 1)

  await core.close()
  await batch.close()
})
