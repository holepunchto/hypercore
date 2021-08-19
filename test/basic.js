const tape = require('tape')
const ram = require('random-access-memory')
const crypto = require('hypercore-crypto')

const Hypercore = require('..')
const { create } = require('./helpers')

tape('basic', async function (t) {
  const core = await create()
  let appends = 0

  t.same(core.length, 0)
  t.same(core.byteLength, 0)
  t.same(core.writable, true)
  t.same(core.readable, true)

  core.on('append', function () {
    appends++
  })

  await core.append('hello')
  await core.append('world')

  t.same(core.length, 2)
  t.same(core.byteLength, 10)
  t.same(appends, 2)

  t.end()
})

tape('session', async function (t) {
  const core = await create()

  const session = core.session()

  await session.append('test')
  t.same(await core.get(0), Buffer.from('test'))
  t.same(await session.get(0), Buffer.from('test'))
  t.end()
})

tape('close', async function (t) {
  const core = await create()
  await core.append('hello world')

  await core.close()

  try {
    await core.get(0)
    t.fail('core should be closed')
  } catch {
    t.pass('get threw correctly when core was closed')
  }
})

tape('storage options', async function (t) {
  const core = new Hypercore({ storage: ram })
  await core.append('hello')
  t.same(await core.get(0), Buffer.from('hello'))
  t.end()
})

tape('preload - storage', async function (t) {
  const core = new Hypercore(null, {
    preload: () => {
      return { storage: ram }
    }
  })
  await core.ready()

  await core.append('hello world')
  t.same(core.length, 1)
  t.same(await core.get(0), Buffer.from('hello world'))

  t.end()
})

tape('preload - from another core', async function (t) {
  t.plan(2)

  const first = new Hypercore(ram)
  await first.ready()

  const second = new Hypercore(null, {
    preload: () => {
      return { from: first }
    }
  })
  await second.ready()

  t.same(first.key, second.key)
  t.same(first.sessions, second.sessions)
})

tape('preload - custom keypair', async function (t) {
  const keyPair = crypto.keyPair()
  const core = new Hypercore(ram, keyPair.publicKey, {
    preload: () => {
      return { keyPair }
    }
  })
  await core.ready()

  t.true(core.writable)
  t.same(core.key, keyPair.publicKey)

  t.end()
})

tape('preload - sign/storage', async function (t) {
  const keyPair = crypto.keyPair()
  const core = new Hypercore(null, keyPair.publicKey, {
    valueEncoding: 'utf-8',
    preload: () => {
      return {
        storage: ram,
        sign: signable => crypto.sign(signable, keyPair.secretKey)
      }
    }
  })
  await core.ready()

  t.true(core.writable)
  await core.append('hello world')
  t.same(core.length, 1)
  t.same(await core.get(0), 'hello world')

  t.end()
})
