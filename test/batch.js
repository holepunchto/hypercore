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

test('batch truncate', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  await b.append(['de', 'fg'])
  await b.truncate(4)

  t.alike(await b.get(3), b4a.from('de'))
  t.alike(await b.get(4), null)

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
  t.exception(b.truncate(2))
})

test('batch destroy', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  await b.append(['de', 'fg'])
  await b.destroy()
  t.is(core.length, 3)

  await core.append(['d', 'e'])
  t.is(core.length, 4)
})

test('batch destroy after flush', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  await b.flush()
  t.exception(b.destroy())
})

test('batch flush after destroy', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c'])

  const b = core.batch()
  await b.destroy()
  t.exception(b.flush())
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
