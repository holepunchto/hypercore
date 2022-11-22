const test = require('brittle')
const { create, replicate, unreplicate } = require('./helpers')

test('one forks', async function (t) {
  t.plan(3)

  const a = await create()
  await a.append(['a', 'b', 'c', 'd', 'e'])

  const b = await create(a.key)

  const c = await create({ keyPair: a.core.header.signer })
  await c.append(['a', 'b', 'c', 'd', 'f', 'e'])

  const streams = replicate(a, b, t)

  c.on('conflict', function (length) {
    t.is(length, 5, 'conflict at 5 seen by c')
  })

  b.on('conflict', function (length) {
    t.is(length, 5, 'conflict at 5 seen by b')
  })

  await b.get(2)

  await unreplicate(streams)

  replicate(c, b, t)

  await t.exception(b.get(4))
})
