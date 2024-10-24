const test = require('brittle')
const crypto = require('hypercore-crypto')
const c = require('compact-encoding')
const b4a = require('b4a')
const { create, createStorage } = require('./helpers')

const Hypercore = require('../')

test('sessions - can create writable sessions from a read-only core', async function (t) {
  t.plan(5)

  const storage = await createStorage(t)
  const keyPair = crypto.keyPair()
  const core = new Hypercore(storage, keyPair.publicKey, {
    valueEncoding: 'utf-8'
  })
  await core.ready()
  t.absent(core.writable)

  const session = core.session({ keyPair })
  await session.ready()

  t.ok(session.writable)

  try {
    await core.append('hello')
    t.fail('should not have appended to the read-only core')
  } catch {
    t.pass('read-only core append threw correctly')
  }

  try {
    await session.append('world')
    t.pass('session append did not throw')
  } catch {
    t.fail('session append should not have thrown')
  }

  t.is(core.length, 1)

  await session.close()
  await core.close()
})

test('sessions - custom valueEncoding on session', async function (t) {
  const storage = await createStorage(t)
  const core1 = new Hypercore(storage)
  await core1.append(c.encode(c.raw.json, { a: 1 }))

  const core2 = core1.session({ valueEncoding: 'json' })
  await core2.append({ b: 2 })

  t.alike(await core2.get(0), { a: 1 })
  t.alike(await core2.get(1), { b: 2 })

  await core2.close()
  await core1.close()
})

test('sessions - custom preload hook on first/later sessions', async function (t) {
  const preloadsTest = t.test('both preload hooks called')
  preloadsTest.plan(2)

  const storage = await createStorage(t)
  const core1 = new Hypercore(storage, {
    preload: () => {
      preloadsTest.pass('first hook called')
      return null
    }
  })
  const core2 = core1.session({
    preload: () => {
      preloadsTest.pass('second hook called')
      return null
    }
  })
  await core2.ready()

  await preloadsTest

  await core2.close()
  await core1.close()
})

test('session inherits non-sparse setting', async function (t) {
  const a = await create(t, { sparse: false })
  const s = a.session()

  t.is(s.sparse, false)

  await s.close()
  await a.close()
})

test('session on a from instance, pre-ready', async function (t) {
  const a = await create(t)

  const b = new Hypercore({ from: a })
  const c = b.session()

  await a.ready()
  await b.ready()
  await c.ready()

  t.is(a.sessions, b.sessions)
  t.is(a.sessions, c.sessions)

  await b.close()
  await c.close()
})

test('session on a from instance does not inject itself to other sessions', async function (t) {
  const a = await create(t, { })

  const b = new Hypercore({ from: a, encryptionKey: null })
  await b.ready()

  const c = new Hypercore({ from: a, encryptionKey: null })
  await c.ready()
  await c.setEncryptionKey(b4a.alloc(32))

  const d = new Hypercore({ from: a, encryptionKey: null })
  await d.ready()

  t.absent(a.encryption)
  t.absent(b.encryption)
  t.ok(c.encryption)
  t.absent(d.encryption)

  await b.close()
  await c.close()
  await d.close()
})
