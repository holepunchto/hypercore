const crypto = require('hypercore-crypto')
const test = require('brittle')
const RAM = require('random-access-memory')
const b4a = require('b4a')
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
  t.alike(await core.get(0), b4a.from('hello world'))
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

  t.alike(first.key, second.key)
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
  t.alike(core.key, keyPair.publicKey)
})

test('preload - sign/storage', async function (t) {
  const keyPair = crypto.keyPair()
  const core = new Hypercore(null, keyPair.publicKey, {
    valueEncoding: 'utf-8',
    preload: () => {
      return {
        storage: RAM,
        keyPair
      }
    }
  })
  await core.ready()

  t.ok(core.writable)
  await core.append('hello world')
  t.is(core.length, 1)
  t.is(await core.get(0), 'hello world')
})
