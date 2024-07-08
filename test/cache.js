const test = require('brittle')
const Xache = require('xache')
const Rache = require('rache')
const b4a = require('b4a')
const { create, replicate } = require('./helpers')

test('cache', async function (t) {
  const a = await create({ cache: true })
  await a.append(['a', 'b', 'c'])

  const p = a.get(0)
  const q = a.get(0)

  t.is(await p, await q, 'blocks are identical')
})

test('session cache inheritance', async function (t) {
  const a = await create({ cache: true })
  await a.append(['a', 'b', 'c'])

  const s = a.session()

  const p = a.get(0)
  const q = s.get(0)

  t.is(await p, await q, 'blocks are identical')
})

test('session cache opt-out', async function (t) {
  const a = await create({ cache: true })
  await a.append(['a', 'b', 'c'])

  const s = a.session({ cache: false })

  const p = a.get(0)
  const q = s.get(0)

  t.not(await p, await q, 'blocks are not identical')
})

test('session cache override', async function (t) {
  const a = await create({ cache: true })
  await a.append(['a', 'b', 'c'])

  const s = a.session({ cache: new Xache({ maxSize: 64, maxAge: 0 }) })

  const p = a.get(0)
  const q = s.get(0)
  const r = s.get(0)

  t.not(await p, await q, 'blocks are not identical')
  t.is(await q, await r, 'blocks are identical')
})

test('clear cache on truncate', async function (t) {
  const a = await create({ cache: true })
  await a.append(['a', 'b', 'c'])

  const p = a.get(0)

  await a.truncate(0)
  await a.append('d')

  const q = a.get(0)

  t.alike(await p, b4a.from('a'))
  t.alike(await q, b4a.from('d'))
})

test('cache on replicate', async function (t) {
  const a = await create()
  await a.append(['a', 'b', 'c'])

  const b = await create(a.key, { cache: true })

  replicate(a, b, t)

  // These will issue a replicator request
  const p = b.get(0)
  const q = b.get(0)

  t.is(await p, await q, 'blocks are identical')

  // This should use the cache
  const r = b.get(0)

  t.is(await p, await r, 'blocks are identical')
})

test('session cache with different encodings', async function (t) {
  const a = await create({ cache: true })
  await a.append(['a', 'b', 'c'])

  const s = a.session({ valueEncoding: 'utf-8' })

  const p = a.get(0)
  const q = s.get(0)

  t.alike(await p, b4a.from('a'))
  t.is(await q, 'a')
})

test('cache is set through preload', async function (t) {
  const a = await create({ async preload () { return { cache: true } } })

  t.ok(a.cache)
})

test('null default for globalCache', async function (t) {
  const a = await create()
  t.is(a.globalCache, null)
})

test('globalCache set if passed in, and shared among sessions', async function (t) {
  const globalCache = new Rache()
  const a = await create({ globalCache })
  t.is(a.globalCache, globalCache, 'cache is stored in hypercore')

  const session = a.session()
  t.is(session.globalCache, globalCache, 'passed on to sessions')
})
