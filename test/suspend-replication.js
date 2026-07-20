const test = require('brittle')
const CoreStorage = require('hypercore-storage')
const { create, replicate, eventFlush } = require('./helpers')
const Hypercore = require('../')
const { once } = require('events')

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

test('suspended replication queues incoming requests, clear receiverBusy after handling w/ error', async function (t) {
  const controller = new Hypercore.SuspendController()

  const a = await create(t, null, { suspendSignal: controller.signal })
  const b = await create(t, a.key)
  await a.append(['a', 'b', 'c'])

  const bAppended = once(b, 'append')
  replicate(a, b, t)
  await bAppended

  controller.suspend()

  // Artificial setup to right max invalid requests to force error when handling an invalid request
  a.peers[0].stats.invalidRequests = 63

  const peerForB = b.replicator.peers[0]
  const invalidReq = {
    peer: peerForB,
    rt: 0,
    id: 1,
    fork: 0,
    block: { index: 0, nodes: 2 },
    hash: null,
    seek: { bytes: 1, padding: 1 }, // invalid to both seek and block when upgrading
    upgrade: { start: 0, length: 2 },
    manifest: false,
    priority: 1,
    timestamp: 1754412092523,
    elapsed: 0
  }

  b.replicator._inflight.add(invalidReq)
  peerForB.wireRequest.send(invalidReq)

  await eventFlush() // allow it to be sent

  controller.resume()
  t.ok(a.peers[0].receiverBusy, 'set to busy')
  await eventFlush()

  t.absent(a.peers[0].receiverBusy, 'no longer set to busy')
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

test('core push while suspending', async function (t) {
  const controller = new Hypercore.SuspendController()

  const a = await create(t, null, { suspendSignal: controller.signal  })
  const b = await create(t, a.key, { allowPush: true, pushOnly: true })

  b.replicator.setPushOnly(true)
  t.is(b.replicator.pushOnly, true, 'b is push only')

  replicate(a, b, t)

  await a.append(['a', 'b', 'c'])

  t.ok(a.peers[0].remoteAllowPush, 'a sees b as push only')
  t.is(a.peers[0].pushProcessing, 0, 'a sees b w/ no pushes initially')
  t.absent(a.replicator.busy, 'isnt busy initially')

  const wireDataTxBefore = a.replicator.stats.wireData.tx

  const pushP = a.replicator.push(0)
  t.absent(a.replicator.suspended, 'isnt suspended yet')
  controller.suspend()

  await eventFlush()
  t.ok(a.replicator.busy, 'pushing makes replicator busy')
  t.ok(a.replicator.suspended, 'now suspended')
  t.is(a.peers[0].pushProcessing, 1, 'a sees b w/ a push')
  await pushP

  t.absent(await b.has(0), 'b doesnt have block')
  controller.resume()

  t.absent(await b.has(0), 'block still absent')
  t.is(a.replicator.stats.wireData.tx, wireDataTxBefore, 'block not sent because !isActive()')
})

async function drainReplication(core) {
  while (core.core.replicator.busy) await sleep(10)
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
