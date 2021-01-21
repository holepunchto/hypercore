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

  await downloadEventWorkAround()

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

  await downloadEventWorkAround()

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

  await downloadEventWorkAround()

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

tape('update with zero length', async function (t) {
  const a = await create()
  const b = await create(a.key)

  replicate(a, b)

  await b.update() // should not hang
  t.same(b.length, 0)

  t.end()
})

function downloadEventWorkAround () {
  // TODO: this is only needed for testing atm, as the event is triggered in some tick after range.downloaded()
  // this is due to the bitfield being update before it is flushed, this should be fixed and this workaround removed.
  return new Promise(resolve => setImmediate(resolve))
}
