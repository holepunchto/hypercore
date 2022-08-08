const test = require('brittle')
const b4a = require('b4a')
const { create, replicate, eventFlush } = require('./helpers')

test('clear', async function (t) {
  const a = await create()
  await a.append(['a', 'b', 'c'])

  t.is(a.contiguousLength, 3)

  await a.clear(1)

  t.is(a.contiguousLength, 1, 'contig updated')

  t.ok(await a.has(0), 'has 0')
  t.absent(await a.has(1), 'has not 1')
  t.ok(await a.has(2), 'has 2')
})

test('clear + replication', async function (t) {
  const a = await create()
  const b = await create(a.key)

  replicate(a, b, t)

  await a.append(['a', 'b', 'c'])
  await b.download({ start: 0, end: 3 }).downloaded()

  await a.clear(1)

  t.absent(await a.has(1), 'a cleared')
  t.ok(await b.has(1), 'b not cleared')

  t.alike(await a.get(1), b4a.from('b'), 'a downloaded from b')
})

test('clear + replication, gossip', async function (t) {
  const a = await create()
  const b = await create(a.key)
  const c = await create(a.key)

  replicate(a, b, t)
  replicate(b, c, t)

  await a.append(['a', 'b', 'c'])
  await b.download({ start: 0, end: 3 }).downloaded()
  await c.update()

  await b.clear(1)

  t.ok(await a.has(1), 'a not cleared')
  t.absent(await b.has(1), 'b cleared')

  let resolved = false

  const req = c.get(1)
  req.then(() => (resolved = true))

  await eventFlush()
  t.absent(resolved, 'c not downloaded')

  t.alike(await b.get(1), b4a.from('b'), 'b downloaded from a')
  t.alike(await req, b4a.from('b'), 'c downloaded from b')
})
