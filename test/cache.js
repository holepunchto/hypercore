const test = require('brittle')
const Xache = require('xache')
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

  t.alike(await p, Buffer.from('a'))
  t.alike(await q, Buffer.from('d'))
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

  t.alike(await p, Buffer.from('a'))
  t.is(await q, 'a')
})
