const test = require('brittle')
const b4a = require('b4a')
const RAM = require('random-access-memory')
const NoiseSecretStream = require('@hyperswarm/secret-stream')
const { create, replicate, unreplicate, eventFlush } = require('./helpers')
const { makeStreamPair } = require('./helpers/networking.js')
const Hypercore = require('../')

test('basic replication', async function (t) {
  const a = await create()

  await a.append(['a', 'b', 'c', 'd', 'e'])

  const b = await create(a.key)

  let d = 0
  b.on('download', () => d++)

  replicate(a, b, t)

  const r = b.download({ start: 0, end: a.length })

  await r.done()

  t.is(d, 5)
})

test('basic replication stats', async function (t) {
  const a = await create()

  await a.append(['a', 'b', 'c', 'd', 'e'])

  const b = await create(a.key)

  const aStats = a.replicator.stats
  const bStats = b.replicator.stats

  t.is(aStats.wireSyncReceived, 0, 'wireSync init 0')
  t.is(aStats.wireSyncTransmitted, 0, 'wireSync init 0')
  t.is(aStats.wireRequestReceived, 0, 'wireRequests init 0')
  t.is(aStats.wireRequestTransmitted, 0, 'wireRequests init 0')
  t.is(aStats.wireDataReceived, 0, 'wireData init 0')
  t.is(aStats.wireDataTransmitted, 0, 'wireData init 0')
  t.is(aStats.wireWantReceived, 0, 'wireWant init 0')
  t.is(aStats.wireWantTransmitted, 0, 'wireWant init 0')
  t.is(aStats.wireBitfieldReceived, 0, 'wireBitfield init 0')
  t.is(aStats.wireBitfieldTransmitted, 0, 'wireBitfield init 0')
  t.is(aStats.wireRangeReceived, 0, 'wireRange init 0')
  t.is(aStats.wireRangeTransmitted, 0, 'wireRange init 0')
  t.is(aStats.wireExtensionReceived, 0, 'wireExtension init 0')
  t.is(aStats.wireExtensionTransmitted, 0, 'wireExtension init 0')
  t.is(aStats.wireCancelReceived, 0, 'wireCancel init 0')
  t.is(aStats.wireCancelTransmitted, 0, 'wireCancel init 0')

  const initStatsLength = [...Object.keys(aStats)].length
  t.is(initStatsLength, 16, 'Expected amount of stats')

  replicate(a, b, t)

  b.get(10).catch(() => {}) // does not exist (for want messages0)
  const r = b.download({ start: 0, end: a.length })

  await r.done()

  const aPeerStats = a.replicator.peers[0].stats
  t.alike(aPeerStats, aStats, 'same stats for peer as entire replicator (when there is only 1 peer)')

  t.ok(aStats.wireSyncReceived > 0, 'wiresync incremented')
  t.is(aStats.wireSyncReceived, bStats.wireSyncTransmitted, 'wireSync received == transmitted')

  t.ok(aStats.wireRequestReceived > 0, 'wireRequests incremented')
  t.is(aStats.wireRequestReceived, bStats.wireRequestTransmitted, 'wireRequests received == transmitted')

  t.ok(bStats.wireDataReceived > 0, 'wireRequests incremented')
  t.is(aStats.wireDataTransmitted, bStats.wireDataReceived, 'wireData received == transmitted')

  t.ok(aStats.wireWantReceived > 0, 'wireWant incremented')
  t.is(bStats.wireWantTransmitted, aStats.wireWantReceived, 'wireWant received == transmitted')

  t.ok(bStats.wireRangeReceived > 0, 'wireRange incremented')
  t.is(aStats.wireRangeTransmitted, bStats.wireRangeReceived, 'wireRange received == transmitted')

  // extension messages
  const aExt = a.registerExtension('test-extension', {
    encoding: 'utf-8'
  })
  aExt.send('hello', a.peers[0])
  await new Promise(resolve => setImmediate(resolve))
  t.ok(bStats.wireExtensionReceived > 0, 'extension incremented')
  t.is(aStats.wireExtensionTransmitted, bStats.wireExtensionReceived, 'extension received == transmitted')

  // bitfield messages
  await b.clear(1)
  const c = await create(a.key)
  replicate(c, b, t)
  c.get(1).catch(() => {})
  await new Promise(resolve => setImmediate(resolve))
  const cStats = c.replicator.stats
  t.ok(cStats.wireBitfieldReceived > 0, 'bitfield incremented')
  t.is(bStats.wireBitfieldTransmitted, cStats.wireBitfieldReceived, 'bitfield received == transmitted')

  t.is(initStatsLength, [...Object.keys(aStats)].length, 'No stats were dynamically added')
})

test('basic downloading is set immediately after ready', function (t) {
  t.plan(2)

  const a = new Hypercore(RAM)

  a.on('ready', function () {
    t.ok(a.replicator.downloading)
  })

  const b = new Hypercore(RAM, { active: false })

  b.on('ready', function () {
    t.absent(b.replicator.downloading)
  })

  t.teardown(async () => {
    await a.close()
    await b.close()
  })
})

test('basic replication from fork', async function (t) {
  const a = await create()

  await a.append(['a', 'b', 'c', 'd', 'e'])
  await a.truncate(4)
  await a.append('e')

  t.is(a.fork, 1)

  const b = await create(a.key)

  replicate(a, b, t)

  let d = 0
  b.on('download', () => d++)

  const r = b.download({ start: 0, end: a.length })

  await r.done()

  t.is(d, 5)
  t.is(a.fork, b.fork)
})

test('eager replication from bigger fork', async function (t) {
  const a = await create()
  const b = await create(a.key)

  replicate(a, b, t)

  await a.append(['a', 'b', 'c', 'd', 'e', 'g', 'h', 'i', 'j', 'k'])
  await a.truncate(4)
  await a.append(['FORKED', 'g', 'h', 'i', 'j', 'k'])

  t.is(a.fork, 1)

  let d = 0
  b.on('download', (index) => {
    d++
  })

  const r = b.download({ start: 0, end: a.length })
  await r.done()

  t.is(d, a.length)
  t.is(a.fork, b.fork)
})

test('eager replication of updates per default', async function (t) {
  const a = await create()
  const b = await create(a.key)

  replicate(a, b, t)

  const appended = new Promise(resolve => {
    b.on('append', function () {
      t.pass('appended')
      resolve()
    })
  })

  await a.append(['a', 'b', 'c', 'd', 'e', 'g', 'h', 'i', 'j', 'k'])
  await appended
})

test('bigger download range', async function (t) {
  const a = await create()
  const b = await create(a.key)

  replicate(a, b, t)

  for (let i = 0; i < 20; i++) await a.append('data')

  const downloaded = new Set()

  b.on('download', function (index) {
    downloaded.add(index)
  })

  const r = b.download({ start: 0, end: a.length })
  await r.done()

  t.is(b.length, a.length, 'same length')
  t.is(downloaded.size, a.length, 'downloaded all')
})

test('high latency reorg', async function (t) {
  const a = await create()
  const b = await create(a.key)

  const s = replicate(a, b, t, { teardown: false })

  for (let i = 0; i < 50; i++) await a.append('data')

  {
    const r = b.download({ start: 0, end: a.length })
    await r.done()
  }

  s[0].destroy()
  s[1].destroy()

  await a.truncate(30)

  for (let i = 0; i < 50; i++) await a.append('fork')

  replicate(a, b, t)

  {
    const r = b.download({ start: 0, end: a.length })
    await r.done()
  }

  let same = 0

  for (let i = 0; i < a.length; i++) {
    const ba = await a.get(i)
    const bb = await b.get(i)
    if (b4a.equals(ba, bb)) same++
  }

  t.is(a.fork, 1)
  t.is(a.fork, b.fork)
  t.is(same, 80)
})

test('invalid signature fails', async function (t) {
  t.plan(1)

  const a = await create(null)
  const b = await create(a.key)

  a.core.verifier = {
    sign () {
      return b4a.alloc(64)
    },
    verify (s, sig) {
      return false
    }
  }

  b.on('verification-error', function (err) {
    t.is(err.code, 'INVALID_SIGNATURE')
  })

  await a.append(['a', 'b', 'c', 'd', 'e'])

  replicate(a, b, t)
})

test('more invalid signatures fails', async function (t) {
  const a = await create(null)

  await a.append(['a', 'b'], { signature: b4a.alloc(64) })

  await t.test('replication fails after bad append', async function (sub) {
    sub.plan(1)

    const b = await create(a.key)
    replicate(a, b, sub)

    b.on('verification-error', function (err) {
      sub.is(err.code, 'INVALID_SIGNATURE')
    })

    b.get(0).then(() => sub.fail('should not get block'), () => {})
    sub.teardown(() => b.close())
  })

  await a.truncate(1, { signature: b4a.alloc(64) })

  await t.test('replication fails after bad truncate', async function (sub) {
    sub.plan(1)

    const b = await create(a.key)
    replicate(a, b, sub)

    b.on('verification-error', function (err) {
      sub.is(err.code, 'INVALID_SIGNATURE')
    })

    b.get(0).then(() => sub.fail('should not get block'), () => {})
    sub.teardown(() => b.close())
  })

  await a.append('good')

  await t.test('replication works again', async function (sub) {
    const b = await create(a.key)
    replicate(a, b, sub)

    await new Promise(resolve => setImmediate(resolve))

    sub.alike(await b.get(0), b4a.from('a'), 'got block')

    sub.teardown(() => b.close())
  })
})

test('invalid capability fails', async function (t) {
  t.plan(2)

  const a = await create()
  const b = await create()

  b.replicator.discoveryKey = a.discoveryKey

  await a.append(['a', 'b', 'c', 'd', 'e'])

  const [s1, s2] = replicate(a, b, t)

  s1.on('error', (err) => {
    t.ok(err, 'stream closed')
  })

  // TODO: move this to the verification-error handler like above...
  s2.on('error', (err) => {
    t.is(err.code, 'INVALID_CAPABILITY')
  })

  return new Promise((resolve) => {
    let missing = 2

    s1.on('close', onclose)
    s2.on('close', onclose)

    function onclose () {
      if (--missing === 0) resolve()
    }
  })
})

test('update with zero length', async function (t) {
  const a = await create()
  const b = await create(a.key)

  replicate(a, b, t)

  await b.update() // should not hang
  t.is(b.length, 0)
})

test('basic multiplexing', async function (t) {
  const a1 = await create()
  const a2 = await create()

  const b1 = await create(a1.key)
  const b2 = await create(a2.key)

  const a = a1.replicate(a2.replicate(true, { keepAlive: false }))
  const b = b1.replicate(b2.replicate(false, { keepAlive: false }))

  a.pipe(b).pipe(a)

  await a1.append('hi')
  t.alike(await b1.get(0), b4a.from('hi'))

  await a2.append('ho')
  t.alike(await b2.get(0), b4a.from('ho'))

  a.destroy()
  b.destroy()
})

test('async multiplexing', async function (t) {
  const a1 = await create()
  const b1 = await create(a1.key)

  const a = a1.replicate(true, { keepAlive: false })
  const b = b1.replicate(false, { keepAlive: false })

  a.pipe(b).pipe(a)

  const a2 = await create()
  await a2.append('ho')

  const b2 = await create(a2.key)

  // b2 doesn't replicate immediately.
  a2.replicate(a)
  await eventFlush()
  b2.replicate(b)

  await new Promise(resolve => b2.once('peer-add', resolve))

  t.is(b2.peers.length, 1)
  t.alike(await b2.get(0), b4a.from('ho'))

  a.destroy()
  b.destroy()
})

test('multiplexing with external noise stream', async function (t) {
  const a1 = await create()
  const a2 = await create()

  const b1 = await create(a1.key)
  const b2 = await create(a2.key)

  const n1 = new NoiseSecretStream(true)
  const n2 = new NoiseSecretStream(false)
  n1.rawStream.pipe(n2.rawStream).pipe(n1.rawStream)

  const s1 = a1.replicate(n1, { keepAlive: false })
  const s2 = a2.replicate(n1, { keepAlive: false })
  const s3 = b1.replicate(n2, { keepAlive: false })
  const s4 = b2.replicate(n2, { keepAlive: false })

  await a1.append('hi')
  t.alike(await b1.get(0), b4a.from('hi'))

  await a2.append('ho')
  t.alike(await b2.get(0), b4a.from('ho'))

  s1.destroy()
  s2.destroy()
  s3.destroy()
  s4.destroy()
})

test('multiplexing with createProtocolStream (ondiscoverykey is not called)', async function (t) {
  t.plan(2)

  const a1 = await create()
  const a2 = await create()

  const b1 = await create(a1.key)
  const b2 = await create(a2.key)

  const n1 = new NoiseSecretStream(true)
  const n2 = new NoiseSecretStream(false)
  n1.rawStream.pipe(n2.rawStream).pipe(n1.rawStream)

  const stream1 = Hypercore.createProtocolStream(n1, {
    ondiscoverykey: function (discoveryKey) {
      t.fail()
    }
  })
  const stream2 = Hypercore.createProtocolStream(n2, {
    ondiscoverykey: function (discoveryKey) {
      t.fail()
    }
  })

  const s1 = a1.replicate(stream1, { keepAlive: false })
  const s2 = a2.replicate(stream1, { keepAlive: false })
  const s3 = b1.replicate(stream2, { keepAlive: false })
  const s4 = b2.replicate(stream2, { keepAlive: false })

  await a1.append('hi')
  t.alike(await b1.get(0), b4a.from('hi'))

  await a2.append('ho')
  t.alike(await b2.get(0), b4a.from('ho'))

  s1.destroy()
  s2.destroy()
  s3.destroy()
  s4.destroy()
})

test('multiplexing with createProtocolStream (ondiscoverykey is called)', async function (t) {
  t.plan(4)

  const a1 = await create()
  const a2 = await create()

  const b1 = await create(a1.key)
  const b2 = await create(a2.key)

  const n1 = new NoiseSecretStream(true)
  const n2 = new NoiseSecretStream(false)
  n1.rawStream.pipe(n2.rawStream).pipe(n1.rawStream)

  const stream1 = Hypercore.createProtocolStream(n1, {
    ondiscoverykey: function (discoveryKey) {
      if (b4a.equals(a1.discoveryKey, discoveryKey)) {
        a1.replicate(stream1, { keepAlive: false })
        t.pass()
      }

      if (b4a.equals(a2.discoveryKey, discoveryKey)) {
        a2.replicate(stream1, { keepAlive: false })
        t.pass()
      }
    }
  })
  const stream2 = Hypercore.createProtocolStream(n2, {
    ondiscoverykey: function (discoveryKey) {
      t.fail()
    }
  })

  const s1 = b1.replicate(stream2, { keepAlive: false })
  const s2 = b2.replicate(stream2, { keepAlive: false })

  await a1.append('hi')
  t.alike(await b1.get(0), b4a.from('hi'))

  await a2.append('ho')
  t.alike(await b2.get(0), b4a.from('ho'))

  s1.destroy()
  s2.destroy()
})

test('seeking while replicating', async function (t) {
  const a = await create()
  const b = await create(a.key)

  replicate(a, b, t)

  await a.append(['hello', 'this', 'is', 'test', 'data'])

  t.alike(await b.seek(6), [1, 1])
})

test('seek with no wait', async function (t) {
  t.plan(2)

  const a = await create()
  const b = await create(a.key)

  replicate(a, b, t)

  t.is(await a.seek(6, { wait: false }), null)

  await a.append(['hello', 'this', 'is', 'test', 'data'])

  t.alike(await a.seek(6, { wait: false }), [1, 1])
})

test('seek with timeout', async function (t) {
  t.plan(1)

  const a = await create()

  try {
    await a.seek(6, { timeout: 1 })
    t.fail('should have timeout')
  } catch (err) {
    t.is(err.code, 'REQUEST_TIMEOUT')
  }
})

test('seek with session options', async function (t) {
  t.plan(3)

  const a = await create()

  const s1 = a.session({ wait: false })

  t.is(await s1.seek(6), null)
  await s1.append(['hello', 'this', 'is', 'test', 'data'])
  t.alike(await s1.seek(6, { wait: false }), [1, 1])

  await s1.close()

  const s2 = a.session({ timeout: 1 })

  try {
    await s2.seek(100)
    t.fail('should have timeout')
  } catch (err) {
    t.is(err.code, 'REQUEST_TIMEOUT')
  }

  await s2.close()
})

test('multiplexing multiple times over the same stream', async function (t) {
  const a1 = await create()

  await a1.append('hi')

  const b1 = await create(a1.key)

  const n1 = new NoiseSecretStream(true)
  const n2 = new NoiseSecretStream(false)

  n1.rawStream.pipe(n2.rawStream).pipe(n1.rawStream)

  const s1 = a1.replicate(n1, { keepAlive: false })
  const s2 = b1.replicate(n2, { keepAlive: false })
  const s3 = b1.replicate(n2, { keepAlive: false })

  t.ok(await b1.update({ wait: true }), 'update once')
  t.absent(await a1.update({ wait: true }), 'writer up to date')
  t.absent(await b1.update({ wait: true }), 'update again')

  t.is(b1.length, a1.length, 'same length')

  s1.destroy()
  s2.destroy()
  s3.destroy()
})

test('destroying a stream and re-replicating works', async function (t) {
  const core = await create()

  while (core.length < 33) await core.append(b4a.from('#' + core.length))

  const clone = await create(core.key)

  let s1 = core.replicate(true, { keepAlive: false })
  let s2 = clone.replicate(false, { keepAlive: false })

  s1.pipe(s2).pipe(s1)

  await s2.opened

  const all = []
  for (let i = 0; i < 33; i++) {
    all.push(clone.get(i))
  }

  clone.once('download', function () {
    // simulate stream failure in the middle of bulk downloading
    s1.destroy()
  })

  await new Promise((resolve) => s1.once('close', resolve))

  // retry
  s1 = core.replicate(true, { keepAlive: false })
  s2 = clone.replicate(false, { keepAlive: false })

  s1.pipe(s2).pipe(s1)

  const blocks = await Promise.all(all)

  t.is(blocks.length, 33, 'downloaded 33 blocks')

  s1.destroy()
  s2.destroy()
})

// TODO: wireWant here
test('replicate discrete range', async function (t) {
  const a = await create()

  await a.append(['a', 'b', 'c', 'd', 'e'])

  const b = await create(a.key)

  let d = 0
  b.on('download', () => d++)

  replicate(a, b, t)

  const r = b.download({ blocks: [0, 2, 3] })
  await r.done()

  t.is(d, 3)
  t.alike(await b.get(0), b4a.from('a'))
  t.alike(await b.get(2), b4a.from('c'))
  t.alike(await b.get(3), b4a.from('d'))
})

test('replicate discrete empty range', async function (t) {
  const a = await create()

  await a.append(['a', 'b', 'c', 'd', 'e'])

  const b = await create(a.key)

  let d = 0
  b.on('download', () => d++)

  replicate(a, b, t)

  const r = b.download({ blocks: [] })

  await r.done()

  t.is(d, 0)
})

test('get with { wait: false } returns null if block is not available', async function (t) {
  const a = await create()

  await a.append('a')

  const b = await create(a.key, { valueEncoding: 'utf-8' })

  replicate(a, b, t)

  t.is(await b.get(0, { wait: false }), null)
  t.is(await b.get(0), 'a')
})

test('request cancellation regression', async function (t) {
  t.plan(2)

  const a = await create()
  const b = await create(a.key)

  let errored = 0

  // do not connect the two

  b.get(0).catch(onerror)
  b.get(1).catch(onerror)
  b.get(2).catch(onerror)

  // No explict api to trigger this (maybe we add a cancel signal / abort controller?) but cancel get(1)
  b.activeRequests[1].context.detach(b.activeRequests[1])

  await b.close()

  t.is(b.activeRequests.length, 0)
  t.is(errored, 3)

  function onerror () {
    errored++
  }
})

test('findingPeers makes update wait for first peer', async function (t) {
  t.plan(2)

  const a = await create()
  const b = await create(a.key)

  await a.append('hi')

  t.is(await b.update(), false)

  const done = b.findingPeers()

  const u = b.update()
  await eventFlush()

  replicate(a, b, t)

  t.is(await u, true)
  done()
})

test('findingPeers + done makes update return false if no peers', async function (t) {
  t.plan(2)

  const a = await create()
  const b = await create(a.key)

  await a.append('hi')

  t.is(await b.update(), false)

  const done = b.findingPeers()

  const u = b.update()
  await eventFlush()

  done()
  t.is(await u, false)
})

test.skip('can disable downloading from a peer', async function (t) {
  const a = await create()

  await a.append(['a', 'b', 'c', 'd', 'e'])

  const b = await create(a.key, { valueEncoding: 'utf-8' })
  const c = await create(a.key, { valueEncoding: 'utf-8' })

  const [aStream] = replicate(b, a, t)
  replicate(b, c, t)
  replicate(a, c, t)

  {
    const r = c.download({ start: 0, end: a.length })
    await r.done()
  }

  const aPeer = b.peers[0].stream.rawStream === aStream
    ? b.peers[0]
    : b.peers[1]

  aPeer.setDownloading(false)

  let aUploads = 0
  let cUploads = 0

  c.on('upload', function () {
    cUploads++
  })
  a.on('upload', function () {
    aUploads++
  })

  {
    const r = b.download({ start: 0, end: a.length })
    await r.done()
  }

  t.is(aUploads, 0)
  t.is(cUploads, a.length)
})

test('contiguous length', async function (t) {
  const a = await create()

  await a.append(['a', 'b', 'c', 'd', 'e'])
  t.is(a.contiguousLength, 5, 'a has all blocks')

  const b = await create(a.key)
  t.is(b.contiguousLength, 0)

  replicate(a, b, t)

  await b.download({ blocks: [0, 2, 4] }).done()
  t.is(b.contiguousLength, 1, 'b has 0 through 1')

  await b.download({ blocks: [1] }).done()
  t.is(b.contiguousLength, 3, 'b has 0 through 2')

  await b.download({ blocks: [3] }).done()
  t.is(b.contiguousLength, 5, 'b has all blocks')
})

test('contiguous length after fork', async function (t) {
  const a = await create()
  const b = await create(a.key)

  const s = replicate(a, b, t, { teardown: false })

  await a.append(['a', 'b', 'c', 'd', 'e'])

  await unreplicate(s)

  await a.truncate(2)
  await a.append('f')
  t.is(a.contiguousLength, 3, 'a has all blocks after fork')

  replicate(a, b, t)

  await b.download({ start: 0, end: a.length }).done()
  t.is(b.contiguousLength, 3, 'b has all blocks after fork')
})

test('one inflight request to a peer per block', async function (t) {
  const a = await create()
  const b = await create(a.key)

  let uploads = 0
  a.on('upload', function (index) {
    if (index === 2) uploads++
  })

  await a.append(['a', 'b', 'c', 'd', 'e'])

  replicate(a, b, t)

  await eventFlush()

  const r1 = b.get(2)
  await Promise.resolve()
  const r2 = b.get(2)

  await r1
  await r2

  t.is(uploads, 1)
})

test('non-sparse replication', async function (t) {
  const a = await create()
  const b = await create(a.key, { sparse: false })

  await a.append(['a', 'b', 'c', 'd', 'e'])

  replicate(a, b, t)

  const download = t.test('download')

  download.plan(6)

  let contiguousLength = 0

  b
    // The tree length should be updated to the full length when the first block
    // is downloaded.
    .once('download', () => download.is(b.core.tree.length, 5))

    // When blocks are downloaded, the reported length should always match the
    // contiguous length.
    .on('download', (i) => {
      download.is(b.length, b.contiguousLength, `block ${i}`)
    })

    // Appends should only be emitted when the contiguous length is updated and
    // never when it's zero.
    .on('append', () => {
      if (contiguousLength >= b.contiguousLength) {
        download.fail('append emitted before contiguous length updated')
      }

      contiguousLength = b.contiguousLength
    })

  await download

  t.is(contiguousLength, b.length)
})

test('download blocks if available', async function (t) {
  const a = await create()
  const b = await create(a.key)

  replicate(a, b, t)

  await a.append(['a', 'b', 'c', 'd', 'e'])
  await eventFlush()

  let d = 0
  b.on('download', () => d++)

  const r = b.download({ blocks: [1, 3, 6], ifAvailable: true })
  await r.done()

  t.is(d, 2)
})

test('download range if available', async function (t) {
  const a = await create()
  const b = await create(a.key)

  replicate(a, b, t)

  await a.append(['a', 'b', 'c', 'd', 'e'])
  await eventFlush()

  let d = 0
  b.on('download', () => d++)

  const r = b.download({ start: 2, end: 6, ifAvailable: true })
  await r.done()

  t.is(d, 3)
})

test('download blocks if available, destroy midway', async function (t) {
  const a = await create()
  const b = await create(a.key)

  const s = replicate(a, b, t, { teardown: false })

  await a.append(['a', 'b', 'c', 'd', 'e'])
  await eventFlush()

  let d = 0
  b.on('download', () => {
    if (d++ === 0) unreplicate(s)
  })

  const r = b.download({ blocks: [1, 3, 6], ifAvailable: true })
  await r.done()

  t.pass('range resolved')
})

test('download blocks available from when only a partial set is available', async function (t) {
  const a = await create()
  const b = await create(a.key)
  const c = await create(a.key)

  replicate(a, b, t)
  replicate(b, c, t)

  await a.append(['a', 'b', 'c', 'd', 'e'])
  await eventFlush()

  await b.get(2)
  await b.get(3)

  const r = c.download({ start: 0, end: -1, ifAvailable: true })
  await r.done()

  t.ok(!(await c.has(0)))
  t.ok(!(await c.has(1)))
  t.ok(await c.has(2))
  t.ok(await c.has(3))
  t.ok(!(await c.has(4)))
})

test('download range resolves immediately if no peers', async function (t) {
  const a = await create()
  const b = await create(a.key)

  // no replication

  const r = b.download({ start: 0, end: 5, ifAvailable: true })
  await r.done()

  t.pass('range resolved')
})

test('download available blocks on non-sparse update', async function (t) {
  const a = await create()
  const b = await create(a.key, { sparse: false })

  replicate(a, b, t)

  await a.append(['a', 'b', 'c', 'd', 'e'])
  await b.update()

  t.is(b.contiguousLength, b.length)
})

test('downloaded blocks are unslabbed if small', async function (t) {
  const a = await create()

  await a.append(Buffer.alloc(1))

  const b = await create(a.key)

  replicate(a, b, t)

  t.is(b.contiguousLength, 0, 'sanity check: we want to receive the downloaded buffer (not from fs)')
  const block = await b.get(0)

  t.is(block.buffer.byteLength, 1, 'unslabbed block')
})

test('downloaded blocks are not unslabbed if bigger than half of slab size', async function (t) {
  const a = await create()

  await a.append(Buffer.alloc(5000))
  t.is(
    Buffer.poolSize < 5000 * 2,
    true,
    'Sanity check (adapt test if fails)'
  )

  const b = await create(a.key)

  replicate(a, b, t)

  t.is(b.contiguousLength, 0, 'sanity check: we want to receive the downloaded buffer (not from fs)')
  const block = await b.get(0)

  t.is(
    block.buffer.byteLength !== block.byteLength,
    true,
    'No unslab if big block' // slab includes the protomux frame
  )
})

test('sparse replication without gossiping', async function (t) {
  t.plan(4)

  const a = await create()
  const b = await create(a.key)

  await a.append(['a', 'b', 'c'])

  let s

  s = replicate(a, b, t, { teardown: false })
  await b.download({ start: 0, end: 3 }).done()
  await unreplicate(s)

  await a.append(['d', 'e', 'f', 'd'])

  s = replicate(a, b, t, { teardown: false })
  await b.download({ start: 4, end: 7 }).done()
  await unreplicate(s)

  await t.test('block', async function (t) {
    const c = await create(a.key)

    s = replicate(b, c, t, { teardown: false })
    t.teardown(() => unreplicate(s))

    t.alike(await c.get(4), b4a.from('e'))
  })

  await t.test('range', async function (t) {
    const c = await create(a.key)

    replicate(b, c, t)

    await c.download({ start: 4, end: 6 }).done()
    t.pass('resolved')
  })

  await t.test('discrete range', async function (t) {
    const c = await create(a.key)

    replicate(b, c, t)

    await c.download({ blocks: [4, 6] }).done()

    t.pass('resolved')
  })

  await t.test('seek', async function (t) {
    const c = await create(a.key)

    replicate(b, c, t)

    t.alike(await c.seek(4), [4, 0])
  })
})

test('force update writable cores', async function (t) {
  const a = await create()
  const b = await create(a.key, { header: a.core.header.manifest })

  await a.append(['a', 'b', 'c', 'd', 'e'])

  replicate(a, b, t)

  await b.update()

  t.is(a.length, 5)
  t.is(b.length, 0, "new device didn't bootstrap its state from the network")

  await b.update({ force: true, wait: true })

  t.is(
    b.length,
    a.length,
    'new device did bootstrap its state from the network'
  )
})

test('replicate to writable cores after clearing', async function (t) {
  const a = await create()
  const b = await create(a.key)

  await a.append(['a', 'b', 'c', 'd', 'e'])

  replicate(a, b, t)
  await b.download({ start: 0, end: 5 }).downloaded()

  await a.clear(0, 5) // clear all data

  t.not(await a.has(2)) // make sure a does not have it
  t.ok(await b.has(2)) // make sure b has it

  const c = await a.get(2)

  t.alike(c, b4a.from('c'))
})

test('large linear download', async function (t) {
  const n = 1000

  const a = await create()

  for (let i = 0; i < n; i++) await a.append(i.toString())

  const b = await create(a.key)

  let d = 0
  b.on('download', () => d++)

  replicate(a, b, t)

  const r = b.download({ start: 0, end: n, linear: true })

  await r.done()

  t.is(d, 1000)
})

test('replication session', async function (t) {
  const a = await create()
  const b = await create(a.key)

  await a.append(['a', 'b', 'c', 'd', 'e'])

  const [s1, s2] = replicate(a, b, t, { session: true, teardown: false })

  t.is(a.sessions.length, 1)
  t.is(b.sessions.length, 1)
  t.is(a.core.active, 2)
  t.is(b.core.active, 2)

  s1.destroy()
  s2.destroy()

  await Promise.all([new Promise(resolve => s1.on('close', resolve)), new Promise(resolve => s2.on('close', resolve))])

  t.is(a.sessions.length, 1)
  t.is(b.sessions.length, 1)
  t.is(a.core.active, 1)
  t.is(b.core.active, 1)
})

test('replication session after stream opened', async function (t) {
  const a = await create()
  const b = await create(a.key)

  await a.append(['a', 'b', 'c', 'd', 'e'])

  const [s1, s2] = replicate(a, b, t, { session: true, teardown: false })

  await s1.noiseStream.opened
  await s2.noiseStream.opened

  t.is(a.sessions.length, 1)
  t.is(b.sessions.length, 1)
  t.is(a.core.active, 2)
  t.is(b.core.active, 2)

  s1.destroy()
  s2.destroy()

  await Promise.all([new Promise(resolve => s1.on('close', resolve)), new Promise(resolve => s2.on('close', resolve))])

  t.is(a.sessions.length, 1)
  t.is(b.sessions.length, 1)
  t.is(a.core.active, 1)
  t.is(b.core.active, 1)
})

test('replication session keeps the core open', async function (t) {
  const a = await create()
  const b = await create(a.key)

  await a.append(['a', 'b', 'c', 'd', 'e'])

  replicate(a, b, t, { session: true })

  await a.close()
  await eventFlush()

  const blk = await b.get(2)

  t.alike(blk, b4a.from('c'), 'still replicating due to session')
})

test('replicate range that fills initial size of bitfield page', async function (t) {
  const a = await create()
  await a.append(new Array(2 ** 15).fill('a'))

  const b = await create(a.key)

  let d = 0
  b.on('download', () => d++)

  replicate(a, b, t)

  const r = b.download({ start: 0, end: a.length })
  await r.done()

  t.is(d, a.length)
})

test('replicate range that overflows initial size of bitfield page', async function (t) {
  const a = await create()
  await a.append(new Array(2 ** 15 + 1).fill('a'))

  const b = await create(a.key)

  let d = 0
  b.on('download', () => d++)

  replicate(a, b, t)

  const r = b.download({ start: 0, end: a.length })
  await r.done()

  t.is(d, a.length)
})

test('replicate ranges in reverse order', async function (t) {
  const a = await create()
  await a.append(['a', 'b'])

  const b = await create(a.key)

  let d = 0
  b.on('download', () => d++)

  replicate(a, b, t)

  const ranges = [[1, 1], [0, 1]] // Order is important

  for (const [start, length] of ranges) {
    const r = b.download({ start, length })
    await r.done()
  }

  t.is(d, a.length)
})

test('cancel block', async function (t) {
  t.plan(4)

  const a = await create()
  const b = await create(a.key)

  await a.append(['a', 'b', 'c'])

  const [n1, n2] = makeStreamPair(t, { latency: [50, 50] })
  a.replicate(n1)
  b.replicate(n2)

  const session = b.session()
  const cancelling = waitForRequestBlock(session).then(() => session.close())
  try {
    await session.get(0)
    t.fail('Should have failed')
  } catch (err) {
    t.is(err.code, 'REQUEST_CANCELLED')
  }
  await cancelling

  t.alike(await b.get(1), b4a.from('b'))

  await a.close()
  await b.close()

  t.ok(a.replicator.stats.wireCancelReceived > 0, 'wireCancel stats incremented')
  t.is(a.replicator.stats.wireCancelReceived, b.replicator.stats.wireCancelTransmitted, 'wireCancel stats consistent')
})

test('try cancel block from a different session', async function (t) {
  t.plan(3)

  const a = await create()
  const b = await create(a.key)

  await a.append(['a', 'b', 'c'])

  const [n1, n2] = makeStreamPair(t, { latency: [50, 50] })
  a.replicate(n1)
  b.replicate(n2)

  const s1 = b.session()
  const s2 = b.session()

  const cancelling = waitForRequestBlock(s1).then(() => s1.close())

  const b1 = s1.get(0)
  const b2 = s2.get(0)

  try {
    await b1
    t.fail('Should have failed')
  } catch (err) {
    t.is(err.code, 'REQUEST_CANCELLED')
  }

  await cancelling

  t.alike(await b2, b4a.from('a'))
  t.alike(await s2.get(1), b4a.from('b'))
  await s2.close()

  await a.close()
  await b.close()
})

test('retry failed block requests to another peer', async function (t) {
  t.plan(6)

  const a = await create()
  const b = await create(a.key)
  const c = await create(a.key)

  await a.append(['1', '2', '3'])

  const [n1, n2] = makeStreamPair(t, { latency: [50, 50] })
  a.replicate(n1)
  b.replicate(n2)

  const [n3, n4] = makeStreamPair(t, { latency: [50, 50] })
  a.replicate(n3)
  c.replicate(n4)

  const [n5, n6] = makeStreamPair(t, { latency: [50, 50] })
  b.replicate(n5)
  c.replicate(n6)

  await b.download({ start: 0, end: a.length }).done()

  t.is(a.contiguousLength, 3)
  t.is(b.contiguousLength, 3)
  t.is(c.contiguousLength, 0)

  let once = false

  // "c" will make a block request, then whoever gets the request first "a" or "b" we destroy that replication stream
  a.once('upload', onupload.bind(null, a, 'a'))
  b.once('upload', onupload.bind(null, b, 'b'))

  t.alike(await c.get(0), b4a.from('1'))

  async function onupload (core, name) {
    t.pass('onupload: ' + name + ' (' + (once ? 'allow' : 'deny') + ')')

    if (once) return
    once = true

    if (name === 'a') {
      await unreplicate([n1, n2, n3, n4])
    } else {
      await unreplicate([n1, n2, n5, n6])
    }
  }
})

test('idle replication sessions auto gc', async function (t) {
  const a = await create({ active: false, notDownloadingLinger: 50 })
  const b = await create(a.key, { autoClose: true, active: false, notDownloadingLinger: 50 })

  await a.append('test')
  const s = b.session()

  replicate(a, b, t, { session: true })

  t.alike(await s.get(0), b4a.from('test'), 'replicates')

  let closed = false
  b.on('close', function () {
    closed = true
  })

  await s.close()

  await pollUntil(() => closed, 75)

  t.ok(closed, 'replication session gced')
})

test('manifests eagerly sync', async function (t) {
  t.plan(1)

  const a = await create({ compat: false })
  const b = await create(a.key)

  replicate(a, b, t)

  b.on('manifest', function () {
    t.alike(b.manifest, a.manifest)
  })
})

test('manifests gossip eagerly sync', async function (t) {
  t.plan(2)

  const a = await create({ compat: false })
  const b = await create(a.key)
  const c = await create(a.key)

  replicate(a, b, t)
  replicate(b, c, t)

  b.on('manifest', function () {
    t.alike(b.manifest, a.manifest)
  })

  c.on('manifest', function () {
    t.alike(b.manifest, a.manifest)
  })
})

test('remote has larger tree', async function (t) {
  const a = await create()
  const b = await create(a.key)
  const c = await create(a.key)

  await a.append(['a', 'b', 'c', 'd', 'e'])

  {
    const [s1, s2] = replicate(a, b, t, { teardown: false })
    await b.get(2)
    await b.get(3)
    await b.get(4)
    s1.destroy()
    s2.destroy()
  }

  await a.append('f')

  {
    const [s1, s2] = replicate(a, c, t, { teardown: false })
    await eventFlush()
    s1.destroy()
    s2.destroy()
  }

  replicate(b, c, t)
  c.get(4) // unresolvable but should not crash anything
  await eventFlush()
  t.ok(!!(await c.get(2)), 'got block #2')
  t.ok(!!(await c.get(3)), 'got block #3')
})

test('range download, single block missing', async function (t) {
  const a = await create()
  const b = await create(a.key)

  const n = 100

  for (let i = 0; i < n; i++) await a.append(b4a.from([0]))

  replicate(a, b, t)

  await b.download({ start: 0, end: n }).done()
  await b.clear(n - 1)

  await b.download({ start: 0, end: n }).done()
})

test('range download, repeated', async function (t) {
  const a = await create()
  const b = await create(a.key)

  const n = 100

  for (let i = 0; i < n; i++) await a.append(b4a.from([0]))

  replicate(a, b, t)

  await b.download({ start: 0, end: n }).done()

  for (let i = 0; i < 1000; i++) {
    await b.download({ start: 0, end: n }).done()
  }
})

test('replication updates on core copy', async function (t) {
  const a = await create()

  const n = 100

  for (let i = 0; i < n; i++) await a.append(b4a.from([0]))

  const manifest = { prologue: { hash: await a.treeHash(), length: a.length } }
  const b = await create({ manifest })
  const c = await create({ manifest })

  replicate(b, c, t)

  const promise = c.get(50)

  await b.core.copyPrologue(a.core)

  await t.execution(promise)
})

test('can define default max-inflight blocks for replicator peers', async function (t) {
  const a = new Hypercore(RAM, { inflightRange: [123, 123] })
  await a.append('some block')

  const b = await create(a.key)
  replicate(a, b, t)
  await b.get(0)

  t.alike(
    a.replicator.peers[0].inflightRange,
    [123, 123],
    'Uses the custom inflight range'
  )
  t.alike(
    b.replicator.peers[0].inflightRange,
    [16, 512],
    'Uses default if no inflight range specified'
  )
})

test('session id reuse does not stall', async function (t) {
  t.plan(2)
  t.timeout(90_000)

  const a = await create()
  const b = await create(a.key)

  const n = 500

  const batch = Array(n).fill().map(e => b4a.from([0]))
  for (let i = 0; i < n; i++) await a.append(batch)

  const [n1, n2] = makeStreamPair(t, { latency: [50, 50] })
  a.replicate(n1)
  b.replicate(n2)

  let downloaded = 0
  b.on('download', function () {
    downloaded++
  })

  while (true) {
    const session = b.session()
    await session.ready()
    const all = []
    for (let i = 0; i < 100; i++) {
      if (!session.core.bitfield.get(i)) {
        all.push(session.get(i).catch(noop))
      }
    }
    if (all.length) await Promise.race(all)
    await session.close()
    if (all.length === 0) break
  }

  t.pass('All blocks downloaded')
  t.is(downloaded, 100, 'Downloaded all blocks exactly once')
})

test('restore after cancelled block request', async function (t) {
  t.plan(2)

  const a = await create()
  const b = await create(a.key)

  for (let i = 0; i < 4; i++) await a.append(b4a.from([i]))

  const [n1, n2] = makeStreamPair(t, { latency: [0, 0] })

  a.replicate(n1)
  b.replicate(n2)

  await new Promise(resolve => b.on('append', resolve))

  const session = b.session()
  t.exception(session.get(a.length)) // async

  a.on('upload', () => session.close()) // close before processing

  // trigger upgrade
  a.append([b4a.from([4]), b4a.from([5])])

  await new Promise(resolve => b.on('append', resolve))

  t.is(b.length, a.length)
})

test('handshake is unslabbed', async function (t) {
  const a = await create()

  await a.append(['a'])

  const b = await create(a.key)

  replicate(a, b, t)
  const r = b.download({ start: 0, end: a.length })
  await r.done()

  t.is(
    a.replicator.peers[0].channel.handshake.capability.buffer.byteLength,
    32,
    'unslabbed handshake capability buffer'
  )
  t.is(
    b.replicator.peers[0].channel.handshake.capability.buffer.byteLength,
    32,
    'unslabbed handshake capability buffer'
  )
})

test('merkle-tree signature gets unslabbed', async function (t) {
  const a = await create()
  await a.append(['a'])

  const b = await create(a.key)
  replicate(a, b, t)
  await b.get(0)

  t.is(
    b.core.tree.signature.buffer.byteLength,
    64,
    'Signature got unslabbed'
  )
})

test('seek against non sparse peer', async function (t) {
  const a = await create()
  await a.append(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n'])

  const b = await create(a.key)
  replicate(a, b, t)

  await b.get(a.length - 1)

  const [block, offset] = await b.seek(5)

  t.is(block, 5)
  t.is(offset, 0)
})

async function waitForRequestBlock (core) {
  while (true) {
    const reqBlock = core.replicator._inflight._requests.find(req => req && req.block)
    if (reqBlock) break

    await new Promise(resolve => setImmediate(resolve))
  }
}

async function pollUntil (fn, time) {
  for (let i = 0; i < 5; i++) {
    if (fn()) return
    await new Promise(resolve => setTimeout(resolve, time))
  }
}

function noop () {}
