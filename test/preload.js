const crypto = require('hypercore-crypto')
const test = require('brittle')
const Hypercore = require('../')
const { createStorage } = require('./helpers')

test('preload - custom keypair', async function (t) {
  const keyPair = crypto.keyPair()
  const storage = await createStorage(t)

  let done = null
  const preload = new Promise((resolve) => {
    done = resolve
  })

  const opts = {
    preload,
    keyPair: null
  }

  const core = new Hypercore(storage, keyPair.publicKey, opts)

  await Promise.resolve()
  opts.keyPair = keyPair
  done()

  await core.ready()

  t.ok(core.writable)
  t.alike(core.key, keyPair.publicKey)

  await core.close()
})
