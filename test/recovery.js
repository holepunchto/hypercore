const test = require('brittle')
const b4a = require('b4a')
const Hypercore = require('../')
const { create, replicate } = require('./helpers')

const ANY = [b4a.alloc(32)]

test('recover - resolves once a peer has nothing more for us', async function (t) {
  const a = await create(t)
  await a.append(['a', 'b', 'c'])

  const b = await create(t, a.key)
  const [s1] = replicate(a, b, t)
  recoveryPeers(t, [s1.noiseStream.publicKey])
  await b.download({ start: 0, end: 3 }).done()

  const started = Date.now()
  await b.recover({ settle: 1000 })
  t.is(b.length, 3)
  t.ok(Date.now() - started < 5000, 'within the settle period')
})

test('recover - resolves when the peer had more', async function (t) {
  const a = await create(t)
  await a.append(['a', 'b', 'c'])

  const b = await create(t, a.key)
  const [s1] = replicate(a, b, t)
  recoveryPeers(t, [s1.noiseStream.publicKey])

  await b.recover({ settle: 1000 })
  t.is(b.length, 3, 'caught up meanwhile')
})

test('recover - only the configured peers count', async function (t) {
  const a = await create(t)
  await a.append(['a', 'b', 'c'])

  const b = await create(t, a.key)
  replicate(a, b, t)
  recoveryPeers(t, ANY)

  await t.exception(b.recover({ timeout: 1500, settle: 100 }), /REQUEST_TIMEOUT/)
})

test('recover - rejects without recovery peers configured', async function (t) {
  const core = await create(t)
  recoveryPeers(t, null)
  await t.exception(core.recover(), /BAD_ARGUMENT/)
})

test('recover - rejects for a snapshot', async function (t) {
  const core = await create(t)
  await core.append('a')
  recoveryPeers(t, ANY)

  const snapshot = core.snapshot()
  await t.exception(snapshot.recover(), /BAD_ARGUMENT/)
  await snapshot.close()
})

test('recover - times out', async function (t) {
  const core = await create(t)
  recoveryPeers(t, ANY)

  const started = Date.now()
  await t.exception(core.recover({ timeout: 500 }), /REQUEST_TIMEOUT/)
  t.ok(Date.now() - started < 2000, 'at the timeout, not at the next retry')
})

test('recover - rejects when the core closes', async function (t) {
  const core = await create(t)
  await core.append('a')
  recoveryPeers(t, ANY)

  const recovering = core.recover()
  const started = Date.now()
  await core.close()

  await t.exception(recovering, /SESSION_CLOSED/)
  t.ok(Date.now() - started < 1000, 'and does so at once')
})

test('recover - rejects when the session closes while the core stays open', async function (t) {
  const core = await create(t)
  await core.append('a')
  recoveryPeers(t, ANY)

  const session = core.session()
  const recovering = session.recover()
  const started = Date.now()
  await session.close()

  await t.exception(recovering, /SESSION_CLOSED/)
  t.ok(Date.now() - started < 1000, 'and does so at once')
  t.absent(core.closing, 'the core is still open')
})

function recoveryPeers(t, keys) {
  Hypercore.setRecoveryPeers(keys)
  t.teardown(() => Hypercore.setRecoveryPeers(null))
}
