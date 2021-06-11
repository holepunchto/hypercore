const tape = require('tape')
const NoiseSecretStream = require('noise-secret-stream')
const { create, replicate } = require('./helpers')

tape('basic replication', async function (t) {
  const a = await create()

  await a.append(['a', 'b', 'c', 'd', 'e'])

  const b = await create(a.key)

  let d = 0
  b.on('download', () => d++)

  replicate(a, b)

  const r = b.download({ start: 0, end: a.length })

  await r.downloaded()

  t.same(d, 5)
})

tape('basic replication from fork', async function (t) {
  const a = await create()

  await a.append(['a', 'b', 'c', 'd', 'e'])
  await a.truncate(4)
  await a.append('e')

  t.same(a.info.fork, 1)

  const b = await create(a.key)

  replicate(a, b)

  let d = 0
  b.on('download', () => d++)

  const r = b.download({ start: 0, end: a.length })

  await r.downloaded()

  t.same(d, 5)
  t.same(a.info.fork, b.info.fork)
})

tape('eager replication from bigger fork', async function (t) {
  const a = await create()
  const b = await create(a.key)

  replicate(a, b)

  await a.append(['a', 'b', 'c', 'd', 'e', 'g', 'h', 'i', 'j', 'k'])
  await a.truncate(4)
  await a.append('FORKED', 'g', 'h', 'i', 'j', 'k')

  t.same(a.info.fork, 1)

  let d = 0
  b.on('download', () => d++)

  const r = b.download({ start: 0, end: a.length })

  await r.downloaded()

  t.same(d, 5)
  t.same(a.info.fork, b.info.fork)
})

tape('eager replication of updates per default', async function (t) {
  const a = await create()
  const b = await create(a.key)

  replicate(a, b)

  await a.append(['a', 'b', 'c', 'd', 'e', 'g', 'h', 'i', 'j', 'k'])

  return new Promise(resolve => {
    b.on('append', function () {
      t.pass('appended')
      resolve()
    })
  })
})

tape('high latency reorg', async function (t) {
  const a = await create()
  const b = await create(a.key)

  const s = replicate(a, b)

  for (let i = 0; i < 50; i++) await a.append('data')

  {
    const r = b.download({ start: 0, end: a.length })
    await r.downloaded()
  }

  s[0].destroy()
  s[1].destroy()

  await a.truncate(30)

  for (let i = 0; i < 50; i++) await a.append('fork')

  replicate(a, b)

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

  t.same(a.fork, 1)
  t.same(a.fork, b.fork)
  t.same(same, 80)
})

tape('invalid signature fails', async function (t) {
  t.plan(2)

  const a = await create()
  const b = await create() // not the same key

  b.discoveryKey = a.discoveryKey // haxx to make them swarm

  await a.append(['a', 'b', 'c', 'd', 'e'])

  const [s1, s2] = replicate(a, b)

  s1.on('error', (err) => {
    t.ok(err, 'stream closed')
  })

  s2.on('error', (err) => {
    t.same(err.message, 'Remote signature does not match')
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

tape('update with zero length', async function (t) {
  const a = await create()
  const b = await create(a.key)

  replicate(a, b)

  await b.update() // should not hang
  t.same(b.length, 0)
})

tape('basic multiplexing', async function (t) {
  const a1 = await create()
  const a2 = await create()

  const b1 = await create(a1.key)
  const b2 = await create(a2.key)

  const a = a1.replicate(a2.replicate(true))
  const b = b1.replicate(b2.replicate(false))

  a.pipe(b).pipe(a)

  await a1.append('hi')
  t.same(await b1.get(0), Buffer.from('hi'))

  await a2.append('ho')
  t.same(await b2.get(0), Buffer.from('ho'))
})

tape('async multiplexing', async function (t) {
  const a1 = await create()
  const b1 = await create(a1.key)

  const a = a1.replicate(true)
  const b = b1.replicate(false)

  a.pipe(b).pipe(a)

  const a2 = await create()
  await a2.append('ho')

  const b2 = await create(a2.key)

  // b2 doesn't replicate immediately.
  a2.replicate(a)
  await new Promise(resolve => setImmediate(resolve))
  b2.replicate(b)

  await new Promise(resolve => b2.once('peer-add', resolve))

  t.same(b2.peers.length, 1)
  t.same(await b2.get(0), Buffer.from('ho'))
})

tape('multiplexing with external noise stream', async function (t) {
  const a1 = await create()
  const a2 = await create()

  const b1 = await create(a1.key)
  const b2 = await create(a2.key)

  const n1 = new NoiseSecretStream(true)
  const n2 = new NoiseSecretStream(false)
  n1.rawStream.pipe(n2.rawStream).pipe(n1.rawStream)

  a1.replicate(n1)
  a2.replicate(n1)
  b1.replicate(n2)
  b2.replicate(n2)

  await a1.append('hi')
  t.same(await b1.get(0), Buffer.from('hi'))

  await a2.append('ho')
  t.same(await b2.get(0), Buffer.from('ho'))
})
