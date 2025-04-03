const uncaughts = require('uncaughts')
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

test('sessions - truncate a checkout session', async function (t) {
  const storage = await createStorage(t)
  const core = new Hypercore(storage)

  for (let i = 0; i < 10; i++) await core.append(b4a.from([i]))

  const atom = storage.createAtom()

  const session = core.session({ checkout: 7, atom })
  await session.ready()

  t.is(session.length, 7)

  await session.truncate(5, session.fork)

  t.is(session.length, 5)

  await session.append(b4a.from('hello'))

  await session.close()
  await core.close()
})

test.skip('session on a from instance does not inject itself to other sessions', async function (t) {
  const a = await create(t, { })

  const b = new Hypercore({ core: a.core, encryptionKey: null })
  await b.ready()

  const c = new Hypercore({ core: a.core, encryptionKey: null })
  await c.ready()
  await c.setEncryptionKey(b4a.alloc(32))

  const d = new Hypercore({ core: a.core, encryptionKey: null })
  await d.ready()

  t.absent(a.encryption)
  t.absent(b.encryption)
  t.ok(c.encryption)
  t.absent(d.encryption)

  await b.close()
  await c.close()
  await d.close()
})

test('sessions - cannot set checkout if name not set', async function (t) {
  const storage = await createStorage(t)
  const core = new Hypercore(storage)
  await core.append('Block0')

  t.exception(
    () => core.session({ checkout: 0 }),
    /Checkouts are only supported on atoms or named sessions/
  )

  t.execution(() => core.session({ checkout: 0, name: 'named' }), 'sanity check on happy path')

  await core.close()
})

test('sessions - checkout breaks prologue', async function (t) {
  const storage = await createStorage(t)
  const storage2 = await createStorage(t)

  uncaughts.on(noop)

  const core = new Hypercore(storage)

  for (let i = 0; i < 10; i++) await core.append(b4a.from([i]))

  const prologued = new Hypercore(storage2, {
    manifest: {
      ...core.manifest,
      prologue: {
        hash: await core.treeHash(),
        length: core.length
      }
    }
  })

  await prologued.ready()
  await prologued.core.copyPrologue(core.state)

  let session
  try {
    session = prologued.session({ name: 'fail', checkout: 7 })
    await session.ready()
    t.fail()
  } catch (err) {
    t.pass()
  }

  await session.close()
  await prologued.close()
  await core.close()

  uncaughts.off(noop)
})

function noop () {}
