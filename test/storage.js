const test = require('brittle')
const sodium = require('sodium-universal')
const crypto = require('hypercore-crypto')
const RAM = require('random-access-memory')
const b4a = require('b4a')
const Hypercore = require('..')

const keyPair = crypto.keyPair(b4a.alloc(sodium.crypto_sign_SEEDBYTES, 'seed'))

const encryptionKey = b4a.alloc(sodium.crypto_stream_KEYBYTES, 'encryption key')

test('storage layout', async function (t) {
  const core = new Hypercore(RAM, { keyPair })

  for (let i = 0; i < 10000; i++) {
    await core.append(b4a.from([i]))
  }

  snapshot(t, core)

  await core.close()
})

test('encrypted storage layout', async function (t) {
  const core = new Hypercore(RAM, { keyPair, encryptionKey })

  for (let i = 0; i < 10000; i++) {
    await core.append(b4a.from([i]))
  }

  snapshot(t, core)

  await core.close()
})

function snapshot (t, core) {
  t.snapshot(b4a.toString(core.core.blocks.storage.toBuffer(), 'base64'), 'blocks')
  t.snapshot(b4a.toString(core.core.tree.storage.toBuffer(), 'base64'), 'tree')
  t.snapshot(b4a.toString(core.core.oplog.storage.toBuffer(), 'base64'), 'oplog')
}
