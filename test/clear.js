const test = require('brittle')
const tmp = require('test-tmp')
const b4a = require('b4a')
const { create, createStorage, replicate, eventFlush } = require('./helpers')

const Hypercore = require('../')

test('clear', async function (t) {
  const a = await create(t)
  await a.append(['a', 'b', 'c'])

  t.is(a.contiguousLength, 3)

  await a.clear(1)

  t.is(a.contiguousLength, 1, 'contig updated')

  t.ok(await a.has(0), 'has 0')
  t.absent(await a.has(1), 'has not 1')
  t.ok(await a.has(2), 'has 2')

  await a.close()
})

test('clear + replication', async function (t) {
  const a = await create(t)
  const b = await create(t, a.key)

  replicate(a, b, t)

  await a.append(['a', 'b', 'c'])
  await b.download({ start: 0, end: 3 }).done()

  await a.clear(1)

  t.absent(await a.has(1), 'a cleared')
  t.ok(await b.has(1), 'b not cleared')

  t.alike(await a.get(1), b4a.from('b'), 'a downloaded from b')

  await a.close()
  await b.close()
})

test('clear + replication, gossip', async function (t) {
  const a = await create(t)
  const b = await create(t, a.key)
  const c = await create(t, a.key)

  replicate(a, b, t)
  replicate(b, c, t)

  await a.append(['a', 'b', 'c'])
  await b.download({ start: 0, end: 3 }).done()
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

test.skip('incorrect clear', async function (t) {
  const core = await create(t)

  const blocks = []
  while (blocks.length < 129) {
    blocks.push(b4a.from('tick'))
  }

  await core.append(blocks)
  await core.clear(127, 128)

  t.absent(await core.has(127))
  t.ok(await core.has(128))
  t.alike(await core.get(128), b4a.from('tick'))
})

test.skip('clear blocks with diff option', async function (t) {
  const storage = await createStorage(t)
  const core = new Hypercore(storage)
  await core.append(b4a.alloc(128))

  const cleared = await core.clear(1337)
  t.is(cleared, null)

  // todo: reenable bytes use api

  // const cleared2 = await core.clear(0, { diff: true })
  // t.ok(cleared2.blocks > 0)

  // const cleared3 = await core.clear(0, { diff: true })
  // t.is(cleared3.blocks, 0)

  await core.close()
})

test.skip('clear - no side effect from clearing unknown nodes', async function (t) {
  const storageWriter = await tmp(t)
  const storageReader = await tmp(t)

  const writer1 = new Hypercore(storageWriter)
  await writer1.append(['a', 'b', 'c', 'd']) // => 'Error: Could not load node: 1'

  const clone = new Hypercore(storageReader, writer1.key)
  await clone.ready()

  // Needs replicate and the three clears for error to happen
  replicate(writer1, clone, t)
  await clone.clear(0)
  await clone.clear(1)
  await clone.clear(2)

  await writer1.close()
  await clone.close()

  t.pass('did not crash')
})
