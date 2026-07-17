const test = require('brittle')
const CoreStorage = require('hypercore-storage')
const { create, replicate, eventFlush } = require('./helpers')
const Hypercore = require('../')

test('suspended replication stops downloading, catches up on resume', async function (t) {
  const controller = new Hypercore.SuspendController()

  const a = await create(t)
  await a.append(['a', 'b', 'c'])

  const b = await create(t, a.key, { suspendSignal: controller.signal })

  replicate(a, b, t)

  await b.get(0)

  controller.suspend()
  await drainReplication(b)

  await a.append(['d', 'e'])
  await eventFlush()
  await sleep(200)

  t.is(b.length, 3, 'no upgrade while suspended')

  controller.resume()

  t.alike(await b.get(4), Buffer.from('e'), 'caught up after resume')
})

test('suspended replication queues incoming requests, serves them on resume', async function (t) {
  const controller = new Hypercore.SuspendController()
  controller.suspend()

  const a = await create(t, null, { suspendSignal: controller.signal })
  await a.append(['a', 'b', 'c'])

  const b = await create(t, a.key)

  replicate(a, b, t)

  const get = b.get(2)
  const early = await Promise.race([get.then(() => 'served'), sleep(300).then(() => 'pending')])
  t.is(early, 'pending', 'request not served while suspended')

  controller.resume()

  t.alike(await get, Buffer.from('c'), 'queued request served after resume')
})

test('no storage io while replication and storage are suspended', async function (t) {
  const controller = new Hypercore.SuspendController()

  const dir = await t.tmp()
  const db = new CoreStorage(dir)

  const a = new Hypercore(db, null, { suspendSignal: controller.signal })
  await a.ready()
  t.teardown(() => a.close())

  await a.append(['a', 'b', 'c', 'd', 'e'])

  const b = await create(t, a.key)

  replicate(a, b, t)
  await b.get(0)

  controller.suspend()
  await drainReplication(a)
  await db.suspend()

  // incoming request while fully suspended must queue, not park on storage
  const get = b.get(3)
  await eventFlush()
  await sleep(300)

  t.is(db.rocks.diagnostics().io, 0, 'no parked storage io while suspended')

  await db.resume()
  controller.resume()

  t.alike(await get, Buffer.from('d'), 'served after resume')
})

test('core opened while the signal is suspended is born suspended', async function (t) {
  const controller = new Hypercore.SuspendController()
  controller.suspend()

  const a = await create(t, null, { suspendSignal: controller.signal })
  await a.append(['a', 'b', 'c'])

  const b = await create(t, a.key)

  replicate(a, b, t)

  const get = b.get(1)
  const early = await Promise.race([get.then(() => 'served'), sleep(300).then(() => 'pending')])
  t.is(early, 'pending', 'core born suspended serves nothing')

  controller.resume()

  t.alike(await get, Buffer.from('b'), 'served after the shared signal resumed')
})

async function drainReplication(core) {
  while (core.core.replicator._replicationBusy()) await sleep(10)
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
