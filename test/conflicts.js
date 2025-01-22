const test = require('brittle')
const { create, replicate, unreplicate } = require('./helpers')

test('one forks', async function (t) {
  // NOTE: this test occasionally (~1/100) flakes
  // because one of the 'conflict' events never emits
  // due to a lifecycle issue (when closing all sessions
  // on a core in reaction to the conflict)
  t.plan(3)

  const a = await create(t)
  await a.append(['a', 'b', 'c', 'd', 'e'])

  const b = await create(t, a.key)

  const c = await create(t, { keyPair: a.core.header.keyPair })
  await c.append(['a', 'b', 'c', 'd', 'f', 'e'])

  const streams = replicate(a, b, t)

  // Note: 'conflict' can be emitted more than once (no guarantees on that)
  let cSeen = false
  c.on('conflict', function (length) {
    if (!cSeen) t.is(length, 5, 'conflict at 5 seen by c')
    cSeen = true
  })

  let bSeen = false
  b.on('conflict', function (length) {
    if (!bSeen) t.is(length, 5, 'conflict at 5 seen by b')
    bSeen = true
  })

  await b.get(2)

  await unreplicate(streams)

  replicate(c, b, t)

  await t.exception(b.get(4))
})
