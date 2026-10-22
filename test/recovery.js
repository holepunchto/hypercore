const test = require('brittle')
const { create, replicate } = require('./helpers')

test('recover - resolves once a peer has nothing more for us', async function (t) {
  const a = await create(t)
  await a.append(['a', 'b', 'c'])

  const b = await create(t, a.key)
  replicate(a, b, t)
  await b.download({ start: 0, end: 3 }).done()

  await b.recover()
  t.is(b.length, 3)
})

test('recover - rejects when the peer had more', async function (t) {
  const a = await create(t)
  await a.append(['a', 'b', 'c'])

  const b = await create(t, a.key)
  replicate(a, b, t)

  await t.exception(b.recover(), /SESSION_NOT_WRITABLE/)
  t.is(b.length, 3, 'caught up meanwhile')
})

test('recover - rejects when the core closes', async function (t) {
  const core = await create(t)
  await core.append('a')

  const recovering = core.recover()
  await core.close()

  await t.exception(recovering, /SESSION_CLOSED/)
})

test('recover - rejects when the session closes while the core stays open', async function (t) {
  const core = await create(t)
  await core.append('a')

  const session = core.session()
  const recovering = session.recover()
  await session.close()

  await t.exception(recovering, /SESSION_CLOSED/)
  t.absent(core.closing, 'the core is still open')
})
