const test = require('brittle')
const RAM = require('random-access-memory')
const crypto = require('hypercore-crypto')
const c = require('compact-encoding')
const { create } = require('./helpers')

const Hypercore = require('../')

test('sessions - can create writable sessions from a read-only core', async function (t) {
  t.plan(5)

  const keyPair = crypto.keyPair()
  const core = new Hypercore(RAM, keyPair.publicKey, {
    valueEncoding: 'utf-8'
  })
  await core.ready()
  t.absent(core.writable)

  const session = core.session({ keyPair: { secretKey: keyPair.secretKey } })
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
})

test('sessions - writable session with custom sign function', async function (t) {
  t.plan(5)

  const keyPair = crypto.keyPair()
  const core = new Hypercore(RAM, keyPair.publicKey, {
    valueEncoding: 'utf-8'
  })
  await core.ready()
  t.absent(core.writable)

  const session = core.session({
    auth: {
      sign: signable => crypto.sign(signable, keyPair.secretKey),
      verify: (signable, signature) => crypto.verify(signable, signature, keyPair.publicKey)
    }
  })

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
})

test('sessions - writable session with invalid keypair throws', async function (t) {
  t.plan(2)

  const keyPair1 = crypto.keyPair()
  const keyPair2 = crypto.keyPair()

  try {
    const core = new Hypercore(RAM, keyPair2.publicKey) // Create a new core in read-only mode.
    const session = core.session({ keyPair: keyPair1 })
    await session.ready()
    t.fail('invalid keypair did not throw')
  } catch {
    t.pass('invalid keypair threw')
  }

  try {
    const core = new Hypercore(RAM, keyPair1.publicKey, { keyPair: keyPair2 }) // eslint-disable-line
    await core.ready()
    t.fail('invalid keypair did not throw')
  } catch {
    t.pass('invalid keypair threw')
  }
})

test('sessions - auto close', async function (t) {
  const core = new Hypercore(RAM, { autoClose: true })

  let closed = false
  core.on('close', function () {
    closed = true
  })

  const a = core.session()
  const b = core.session()

  await a.close()
  t.absent(closed, 'not closed yet')

  await b.close()
  t.ok(closed, 'all closed')
})

test('sessions - auto close different order', async function (t) {
  const core = new Hypercore(RAM, { autoClose: true })

  const a = core.session()
  const b = core.session()

  let closed = false
  a.on('close', function () {
    closed = true
  })

  await core.close()
  t.absent(closed, 'not closed yet')

  await b.close()
  t.ok(closed, 'all closed')
})

test('sessions - auto close with all closing', async function (t) {
  const core = new Hypercore(RAM, { autoClose: true })

  const a = core.session()
  const b = core.session()

  let closed = 0
  a.on('close', () => closed++)
  b.on('close', () => closed++)
  core.on('close', () => closed++)

  await Promise.all([core.close(), a.close(), b.close()])
  t.is(closed, 3, 'all closed')
})

test('sessions - auto close when using from option', async function (t) {
  const core1 = new Hypercore(RAM, {
    autoClose: true
  })
  const core2 = new Hypercore({
    preload: () => {
      return {
        from: core1
      }
    }
  })
  await core2.close()
  t.ok(core1.closed)
})

test('sessions - close with from option', async function (t) {
  const core1 = new Hypercore(RAM)
  await core1.append('hello world')

  const core2 = new Hypercore({
    preload: () => {
      return {
        from: core1
      }
    }
  })
  await core2.close()

  t.absent(core1.closed)
  t.alike(await core1.get(0), Buffer.from('hello world'))
})

test('sessions - custom valueEncoding on session', async function (t) {
  const core1 = new Hypercore(RAM)
  await core1.append(c.encode(c.raw.json, { a: 1 }))

  const core2 = core1.session({ valueEncoding: 'json' })
  await core2.append({ b: 2 })

  t.alike(await core2.get(0), { a: 1 })
  t.alike(await core2.get(1), { b: 2 })
})

test('sessions - custom preload hook on first/later sessions', async function (t) {
  const preloadsTest = t.test('both preload hooks called')
  preloadsTest.plan(2)

  const core1 = new Hypercore(RAM, {
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
})

test('session inherits non-sparse setting', async function (t) {
  const a = await create({ sparse: false })
  const s = a.session()

  t.is(s.sparse, false)
})

test('session on a from instance, pre-ready', async function (t) {
  const a = await create()

  const b = new Hypercore({ from: a })
  const c = b.session()

  await a.ready()
  await b.ready()
  await c.ready()

  t.is(a.sessions, b.sessions)
  t.is(a.sessions, c.sessions)
})
