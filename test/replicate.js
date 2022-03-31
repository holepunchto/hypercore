const test = require('brittle')
const NoiseSecretStream = require('@hyperswarm/secret-stream')
const { create, replicate, eventFlush } = require('./helpers')

test('basic replication', async function (t) {
  const a = await create()

  await a.append(['a', 'b', 'c', 'd', 'e'])

  const b = await create(a.key)

  let d = 0
  b.on('download', () => d++)

  replicate(a, b, t)

  const r = b.download({ start: 0, end: a.length })

  await r.downloaded()

  t.is(d, 5)
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

  await r.downloaded()

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
  await r.downloaded()

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
  await r.downloaded()

  t.is(b.length, a.length, 'same length')
  t.is(downloaded.size, a.length, 'downloaded all')

  t.end()
})

test('high latency reorg', async function (t) {
  const a = await create()
  const b = await create(a.key)

  const s = replicate(a, b, t)

  for (let i = 0; i < 50; i++) await a.append('data')

  {
    const r = b.download({ start: 0, end: a.length })
    await r.downloaded()
  }

  s[0].destroy()
  s[1].destroy()

  await a.truncate(30)

  for (let i = 0; i < 50; i++) await a.append('fork')

  replicate(a, b, t)

  {
    const r = b.download({ start: 0, end: a.length })
    await r.downloaded()
  }

  let same = 0

  for (let i = 0; i < a.length; i++) {
    const ba = await a.get(i)
    const bb = await b.get(i)
    if (ba.equals(bb)) same++
  }

  t.is(a.fork, 1)
  t.is(a.fork, b.fork)
  t.is(same, 80)
})

test('invalid signature fails', async function (t) {
  t.plan(2)

  const a = await create(null, {
    auth: {
      sign () {
        return Buffer.alloc(64)
      },
      verify (s, sig) {
        return false
      }
    }
  })

  const b = await create(a.key)

  await a.append(['a', 'b', 'c', 'd', 'e'])

  const [s1, s2] = replicate(a, b, t)

  s1.on('error', (err) => {
    t.ok(err, 'stream closed')
  })

  s2.on('error', (err) => {
    t.is(err.message, 'Remote signature does not match')
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

  s2.on('error', (err) => {
    t.is(err.message, 'Remote sent an invalid capability')
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
  t.alike(await b1.get(0), Buffer.from('hi'))

  await a2.append('ho')
  t.alike(await b2.get(0), Buffer.from('ho'))
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
  t.alike(await b2.get(0), Buffer.from('ho'))
})

test('multiplexing with external noise stream', async function (t) {
  const a1 = await create()
  const a2 = await create()

  const b1 = await create(a1.key)
  const b2 = await create(a2.key)

  const n1 = new NoiseSecretStream(true)
  const n2 = new NoiseSecretStream(false)
  n1.rawStream.pipe(n2.rawStream).pipe(n1.rawStream)

  a1.replicate(n1, { keepAlive: false })
  a2.replicate(n1, { keepAlive: false })
  b1.replicate(n2, { keepAlive: false })
  b2.replicate(n2, { keepAlive: false })

  await a1.append('hi')
  t.alike(await b1.get(0), Buffer.from('hi'))

  await a2.append('ho')
  t.alike(await b2.get(0), Buffer.from('ho'))
})

test('seeking while replicating', async function (t) {
  const a = await create()
  const b = await create(a.key)

  replicate(a, b, t)

  await a.append(['hello', 'this', 'is', 'test', 'data'])

  t.alike(await b.seek(6), [1, 1])
})

test('multiplexing multiple times over the same stream', async function (t) {
  const a1 = await create()

  await a1.append('hi')

  const b1 = await create(a1.key)

  const n1 = new NoiseSecretStream(true)
  const n2 = new NoiseSecretStream(false)

  n1.rawStream.pipe(n2.rawStream).pipe(n1.rawStream)

  a1.replicate(n1, { keepAlive: false })

  b1.replicate(n2, { keepAlive: false })
  b1.replicate(n2, { keepAlive: false })

  t.ok(await b1.update(), 'update once')
  t.absent(await a1.update(), 'writer up to date')
  t.absent(await b1.update(), 'update again')

  t.is(b1.length, a1.length, 'same length')
  t.end()
})

test('destroying a stream and re-replicating works', async function (t) {
  const core = await create()

  while (core.length < 33) await core.append(Buffer.from('#' + core.length))

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
})

test('replicate discrete range', async function (t) {
  const a = await create()

  await a.append(['a', 'b', 'c', 'd', 'e'])

  const b = await create(a.key)

  let d = 0
  b.on('download', () => d++)

  replicate(a, b, t)

  const r = b.download({ blocks: [0, 2, 3] })
  await r.downloaded()

  t.is(d, 3)
  t.alike(await b.get(0), Buffer.from('a'))
  t.alike(await b.get(2), Buffer.from('c'))
  t.alike(await b.get(3), Buffer.from('d'))
})

test('replicate discrete empty range', async function (t) {
  const a = await create()

  await a.append(['a', 'b', 'c', 'd', 'e'])

  const b = await create(a.key)

  let d = 0
  b.on('download', () => d++)

  replicate(a, b, t)

  const r = b.download({ blocks: [] })

  await r.downloaded()

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
    await r.downloaded()
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
    await r.downloaded()
  }

  t.is(aUploads, 0)
  t.is(cUploads, a.length)
})
