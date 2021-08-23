const tape = require('tape')
const ram = require('random-access-memory')
const crypto = require('hypercore-crypto')

const Hypercore = require('../')

tape('sessions - can create writable sessions from a read-only core', async function (t) {
  t.plan(5)

  const keyPair = crypto.keyPair()
  const core = new Hypercore(ram, keyPair.publicKey, {
    valueEncoding: 'utf-8'
  })
  await core.ready()
  t.false(core.writable)

  const session = core.session({ keyPair: { secretKey: keyPair.secretKey } })
  t.true(session.writable)

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

  t.same(core.length, 1)
  t.end()
})

tape('sessions - writable session with custom sign function', async function (t) {
  t.plan(5)

  const keyPair = crypto.keyPair()
  const core = new Hypercore(ram, keyPair.publicKey, {
    valueEncoding: 'utf-8'
  })
  await core.ready()
  t.false(core.writable)

  const session = core.session({ sign: signable => crypto.sign(signable, keyPair.secretKey) })
  t.true(session.writable)

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

  t.same(core.length, 1)
  t.end()
})

tape('sessions - writable session with invalid keypair throws', async function (t) {
  t.plan(2)

  const keyPair1 = crypto.keyPair()
  const keyPair2 = crypto.keyPair()

  try {
    const core = new Hypercore(ram, keyPair2.publicKey) // Create a new core in read-only mode.
    core.session({ keyPair: keyPair1 })
    t.fail('invalid keypair did not throw')
  } catch {
    t.pass('invalid keypair threw')
  }

  try {
    const core = new Hypercore(ram, keyPair1.publicKey, { keyPair: keyPair2 }) // eslint-disable-line
    await core.ready()
    t.fail('invalid keypair did not throw')
  } catch {
    t.pass('invalid keypair threw')
  }
})

tape('sessions - auto close', async function (t) {
  const core = new Hypercore(ram, { autoClose: true })

  let closed = false
  core.on('close', function () {
    closed = true
  })

  const a = core.session()
  const b = core.session()

  await a.close()
  t.notOk(closed, 'not closed yet')

  await b.close()
  t.ok(closed, 'all closed')
})

tape('sessions - auto close different order', async function (t) {
  const core = new Hypercore(ram, { autoClose: true })

  const a = core.session()
  const b = core.session()

  let closed = false
  a.on('close', function () {
    closed = true
  })

  await core.close()
  t.notOk(closed, 'not closed yet')

  await b.close()
  t.ok(closed, 'all closed')
})

tape('sessions - auto close with all closing', async function (t) {
  const core = new Hypercore(ram, { autoClose: true })

  const a = core.session()
  const b = core.session()

  let closed = 0
  a.on('close', () => closed++)
  b.on('close', () => closed++)
  core.on('close', () => closed++)

  await Promise.all([core.close(), a.close(), b.close()])
  t.same(closed, 3, 'all closed')
})
