const test = require('brittle')
const Xache = require('xache')
const Rache = require('rache')
const b4a = require('b4a')
const { create, replicate } = require('./helpers')

test('cache', async function (t) {
  const a = await create(t, { cache: true })
  await a.append(['a', 'b', 'c'])

  const p = await a.get(0)
  const q = await a.get(0)

  t.is(p, q, 'blocks are identical')
})

test('session cache inheritance', async function (t) {
  const a = await create(t, { cache: true })
  await a.append(['a', 'b', 'c'])

  const s = a.session()

  const p = await a.get(0)
  const q = await s.get(0)

  t.is(p, q, 'blocks are identical')

  await s.close()
})

test('session cache opt-out', async function (t) {
  const a = await create(t, { cache: true })
  await a.append(['a', 'b', 'c'])

  const s = a.session({ cache: false })

  const p = await a.get(0)
  const q = await s.get(0)

  t.not(p, q, 'blocks are not identical')

  await s.close()
})

test('session cache override', async function (t) {
  const a = await create(t, { cache: true })
  await a.append(['a', 'b', 'c'])

  const s = a.session({ cache: new Xache({ maxSize: 64, maxAge: 0 }) })

  const p = await a.get(0)
  const q = await s.get(0)
  const r = await s.get(0)

  t.not(p, q, 'blocks are not identical')
  t.is(q, r, 'blocks are identical')

  await s.close()
})

test('clear cache on truncate', async function (t) {
  const a = await create(t, { cache: true })
  await a.append(['a', 'b', 'c'])

  const p = a.get(0)

  await a.truncate(0)
  await a.append('d')

  const q = a.get(0)

  t.alike(await p, b4a.from('a'))
  t.alike(await q, b4a.from('d'))
})

test('cache on replicate', async function (t) {
  const a = await create(t)
  await a.append(['a', 'b', 'c'])

  const b = await create(t, a.key, { cache: true })

  replicate(a, b, t)

  // These will issue a replicator request
  const p = await b.get(0)
  const q = await b.get(0)

  t.is(p, q, 'blocks are identical')

  // This should use the cache
  const r = await b.get(0)

  t.is(p, r, 'blocks are identical')
})

test('session cache with different encodings', async function (t) {
  const a = await create(t, { cache: true })
  await a.append(['a', 'b', 'c'])

  const s = a.session({ valueEncoding: 'utf-8' })

  const p = a.get(0)
  const q = s.get(0)

  t.alike(await p, b4a.from('a'))
  t.is(await q, 'a')

  await s.close()
})

test('cache is set through preload', async function (t) {
  const a = await create(t, { async preload () { return { cache: true } } })

  t.ok(a.cache)
})

test('null default for globalCache', async function (t) {
  const a = await create(t)
  t.is(a.globalCache, null)
})

test('globalCache set if passed in, and shared among sessions', async function (t) {
  const globalCache = new Rache()
  const a = await create(t, { globalCache })
  t.is(a.globalCache, globalCache, 'cache is stored in hypercore')

  const session = a.session()
  t.is(session.globalCache, globalCache, 'passed on to sessions')

  await session.close()
})
