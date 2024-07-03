const test = require('brittle')
const Rache = require('rache')
const b4a = require('b4a')
const { create, replicate } = require('./helpers')

test('cache', async function (t) {
  const a = await create({ cache: new Rache() })
  await a.append(['a', 'b', 'c'])

  const p = a.get(0)
  const q = a.get(0)

  t.is(await p, await q, 'blocks are identical')
})

test('session cache inheritance', async function (t) {
  const a = await create({ cache: new Rache() })
  await a.append(['a', 'b', 'c'])

  const s = a.session()

  const p = a.get(0)
  const q = s.get(0)

  t.is(await p, await q, 'blocks are identical')
})

test('session cache opt-out', async function (t) {
  const a = await create({ cache: new Rache() })
  await a.append(['a', 'b', 'c'])

  const s = a.session({ cache: false })

  const p = a.get(0)
  const q = s.get(0)

  t.not(await p, await q, 'blocks are not identical')
})

test('session cache override', async function (t) {
  const cache = new Rache()
  const a = await create({ cache })
  await a.append(['a', 'b', 'c'])

  const s = a.session({ cache: cache.sub() })

  const p = a.get(0)
  const q = s.get(0)
  const r = s.get(0)

  t.not(await p, await q, 'blocks are not identical')
  t.is(await q, await r, 'blocks are identical')
})

test('clear cache on truncate', async function (t) {
  const a = await create({ cache: new Rache() })
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

  const b = await create(a.key, { cache: new Rache() })

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
  // TODO: investigate why this is working

  const a = await create({ cache: new Rache() })
  await a.append(['a', 'b', 'c'])

  const s = a.session({ valueEncoding: 'utf-8' })

  const p = a.get(0)
  const q = s.get(0)

  t.alike(await p, b4a.from('a'))
  t.is(await q, 'a')
})

test('cache is set through preload', async function (t) {
  const a = await create({ async preload () { return { cache: new Rache() } } })

  t.ok(a.cache)
})
