const crypto = require('hypercore-crypto')
const test = require('brittle')
const b4a = require('b4a')
const Hypercore = require('../')
const { create, createStorage } = require('./helpers')

test('preload - storage', async function (t) {
  const storage = await createStorage(t)

  const core = new Hypercore(null, {
    preload: () => {
      return { storage }
    }
  })
  await core.ready()

  await core.append('hello world')
  t.is(core.length, 1)
  t.alike(await core.get(0), b4a.from('hello world'))
})

test('preload - from another core', async function (t) {
  t.plan(2)

  const first = await create(t)

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
  const storage = await createStorage(t)

  const core = new Hypercore(storage, keyPair.publicKey, {
    preload: () => {
      return { keyPair }
    }
  })
  await core.ready()

  t.ok(core.writable)
  t.is(core.key, keyPair.publicKey)
})

test('preload - sign/storage', async function (t) {
  const keyPair = crypto.keyPair()
  const storage = await createStorage(t)
  const core = new Hypercore(null, keyPair.publicKey, {
    valueEncoding: 'utf-8',
    preload: () => {
      return {
        storage,
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
