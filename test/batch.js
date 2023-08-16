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
