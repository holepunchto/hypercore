const crypto = require('hypercore-crypto')
const tape = require('tape')
const ram = require('random-access-memory')
const Hypercore = require('../')

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
