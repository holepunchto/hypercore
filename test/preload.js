const crypto = require('hypercore-crypto')
const test = require('brittle')
const Hypercore = require('../')
const { createStorage } = require('./helpers')

test('preload - custom keypair', async function (t) {
  const keyPair = crypto.keyPair()
  const storage = await createStorage(t)

  const preload = new Promise((resolve) => {
    resolve({ keyPair })
  })

  const core = new Hypercore(storage, keyPair.publicKey, { preload })
  await core.ready()

  t.ok(core.writable)
  t.alike(core.key, keyPair.publicKey)

  await core.close()
})
