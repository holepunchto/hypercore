const tape = require('tape')
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

  // TODO: this is only needed for testing atm, as the event is triggered in some tick after the above
  await new Promise(resolve => setImmediate(resolve))

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

  setTimeout(async () => {
    t.same(d, 0)
    await b.verify(await a.proof({ upgrade: { start: 0, length: a.length } }))
  }, 10)

  await r.downloaded()

  // TODO: this is only needed for testing atm, as the event is triggered in some tick after the above
  await new Promise(resolve => setImmediate(resolve))

  t.same(d, 5)
  t.same(a.info.fork, b.info.fork)
})

tape('invalid signature fails', async function (t) {
  const a = await create()
  const b = await create() // not the same key

  await a.append(['a', 'b', 'c', 'd', 'e'])

  const [s1, s2] = replicate(a, b)

  s1.on('error', (err) => {
    t.ok(err, 'stream closed')
  })

  s2.on('error', (err) => {
    t.same(err.message, 'Remote signature does not match')
  })

  await b.update()
})
