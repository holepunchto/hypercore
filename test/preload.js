const crypto = require('hypercore-crypto')
const test = require('brittle')
const RAM = require('random-access-memory')
const Hypercore = require('../')

test('preload - storage', async function (t) {
  const core = new Hypercore(null, {
    preload: () => {
      return { storage: RAM }
    }
  })
  await core.ready()

  await core.append('hello world')
  t.is(core.length, 1)
  t.alike(await core.get(0), Buffer.from('hello world'))

  t.end()
})

test('preload - from another core', async function (t) {
  t.plan(2)

  const first = new Hypercore(RAM)
  await first.ready()

  const second = new Hypercore(null, {
    preload: () => {
      return { from: first }
    }
  })
  await second.ready()

  t.is(first.key, second.key)
  t.is(first.sessions, second.sessions)
})

test('preload - custom keypair', async function (t) {
  const keyPair = crypto.keyPair()
  const core = new Hypercore(RAM, keyPair.publicKey, {
    preload: () => {
      return { keyPair }
    }
  })
  await core.ready()

  t.ok(core.writable)
  t.is(core.key, keyPair.publicKey)

  t.end()
})

test('preload - sign/storage', async function (t) {
  const keyPair = crypto.keyPair()
  const core = new Hypercore(null, keyPair.publicKey, {
    valueEncoding: 'utf-8',
    preload: () => {
      return {
        storage: RAM,
        auth: {
          sign: signable => crypto.sign(signable, keyPair.secretKey),
          verify: (signable, signature) => crypto.verify(signable, signature, keyPair.publicKey)
        }
      }
    }
  })
  await core.ready()

  t.ok(core.writable)
  await core.append('hello world')
  t.is(core.length, 1)
  t.is(await core.get(0), 'hello world')

  t.end()
})
