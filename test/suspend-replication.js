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

  const a = await create(t, null, { suspendSignal: controller.signal })
  const b = await create(t, a.key)
  await a.append(['a', 'b', 'c'])

  // sync before suspending
  const bAppended = once(b, 'append')
  replicate(a, b, t)
  await bAppended

  controller.suspend()

  const get = b.get(2)
  const early = await Promise.race([get.then(() => 'served'), sleep(300).then(() => 'pending')])
  t.is(early, 'pending', 'request not served while suspended')
  t.is(a.peers[0].receiverQueue.length, 1, 'request queued while suspended')

  controller.resume()

  t.alike(await get, Buffer.from('c'), 'queued request served after resume')
})

test('suspended replication doesnt deadlock when suspended again mid-flight after resume', async function (t) {
  const controller = new Hypercore.SuspendController()

  const a = await create(t, null, { suspendSignal: controller.signal })
  const b = await create(t, a.key)
  await a.append(['a', 'b', 'c'])

  const bAppended = once(b, 'append')
  replicate(a, b, t)
  await bAppended

  controller.suspend()
  const get = b.get(0)
  const early = await Promise.race([get.then(() => 'served'), sleep(300).then(() => 'pending')])
  t.is(early, 'pending', 'request not served while suspended')

  // land the flutter suspend deterministically inside _fulfillRequest's one
  // pending await, instead of guessing how many ticks req.fulfill() takes
  const fired = fulfillMidflightSuspend(a.peers[0], controller)

  controller.resume()
  await fired

  t.alike(await get, Buffer.from('a'), 'served once its in-flight read completed')
})

test('suspended-again request is served exactly once, not duplicated', async function (t) {
  const controller = new Hypercore.SuspendController()

  const a = await create(t, null, { suspendSignal: controller.signal })
  const b = await create(t, a.key)
  await a.append(['a'])

  const bAppended = once(b, 'append')
  replicate(a, b, t)
  await bAppended

  controller.suspend()
  const get = b.get(0)
  await eventFlush()

  const wireDataTxBefore = a.replicator.stats.wireData.tx
  const fired = fulfillMidflightSuspend(a.peers[0], controller)

  controller.resume()
  await fired

  t.alike(await get, Buffer.from('a'), 'served once its in-flight read completed')

  // suspending again after it was sent
  controller.suspend()
  // resume to check for duplicate sends
  controller.resume()
  await eventFlush()

  t.is(
    a.replicator.stats.wireData.tx,
    wireDataTxBefore + 1,
    'served exactly once, no duplicate send'
  )
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

  const a = await create(t, null, { suspendSignal: controller.signal })
  const b = await create(t, a.key, { allowPush: true })

  await a.append(['a', 'b', 'c'])

  replicate(a, b, t)
  await b.get(2) // fully synced before switching to push-only

  b.replicator.setPushOnly(true)
  t.is(b.replicator.pushOnly, true, 'b is push only')

  await a.append(['d'])
  await eventFlush()

  t.ok(a.peers[0].remoteAllowPush, 'a sees b as push only')
  t.is(a.peers[0].pushProcessing, 0, 'a sees b w/ no pushes initially')
  t.absent(a.replicator.busy, 'isnt busy initially')

  const wireDataTxBefore = a.replicator.stats.wireData.tx

  const fired = fulfillMidflightSuspend(a.peers[0], controller)
  const pushP = a.replicator.push(3)
  t.absent(a.replicator.suspended, 'isnt suspended yet')

  await fired
  t.ok(a.replicator.busy, 'pushing makes replicator busy')
  t.ok(a.replicator.suspended, 'now suspended mid push')
  t.is(a.peers[0].pushProcessing, 1, 'a sees b w/ a push')
  await pushP

  await waitForBlock(b, 3)
  t.ok(await b.has(3), 'block sent despite suspend, its read was already in flight')
  t.is(
    a.replicator.stats.wireData.tx,
    wireDataTxBefore + 1,
    'in-flight push send goes through once its read completes'
  )

  // post-suspend so pushes should not read or send
  await a.append(['e'])
  await eventFlush()

  const wireDataTxAfterFirst = a.replicator.stats.wireData.tx
  await a.replicator.push(4)

  t.is(a.peers[0].pushProcessing, 0, 'push started while suspended never begins reading')
  t.absent(await b.has(4), 'no push while suspended')
  t.is(
    a.replicator.stats.wireData.tx,
    wireDataTxAfterFirst,
    'no send for a push that started while suspended'
  )

  controller.resume()
  await a.replicator.push(4)

  await waitForBlock(b, 4)
  t.ok(await b.has(4), 'push works again after resume')
})

// patches peer._fulfillRequest for exactly one call, firing controller.suspend()
// synchronously right after it starts (while its one internal await is pending)
// so the flutter lands inside the race window instead of at a guessed tick count
function fulfillMidflightSuspend(peer, controller) {
  const orig = peer._fulfillRequest.bind(peer)
  let resolve
  const fired = new Promise((r) => {
    resolve = r
  })

  peer._fulfillRequest = function (req, pushing) {
    peer._fulfillRequest = orig
    const p = orig(req, pushing)
    controller.suspend()
    resolve()
    return p
  }

  return fired
}

async function drainReplication(core) {
  while (core.core.replicator.busy) await sleep(10)
}

async function waitForBlock(core, index) {
  while (!(await core.has(index))) await sleep(10)
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
