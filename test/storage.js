const test = require('brittle')
const sodium = require('sodium-universal')
const crypto = require('hypercore-crypto')
const RAM = require('random-access-memory')
const Hypercore = require('..')

const keyPair = crypto.keyPair(Buffer.alloc(sodium.crypto_sign_SEEDBYTES, 'seed'))

const encryptionKey = Buffer.alloc(sodium.crypto_stream_KEYBYTES, 'encryption key')

test('storage layout', async function (t) {
  const core = new Hypercore(RAM, { keyPair })

  for (let i = 0; i < 10000; i++) {
    await core.append(Buffer.from([i]))
  }

  snapshot(t, core)
})

test('encrypted storage layout', async function (t) {
  const core = new Hypercore(RAM, { keyPair, encryptionKey })

  for (let i = 0; i < 10000; i++) {
    await core.append(Buffer.from([i]))
  }

  snapshot(t, core)
})

function snapshot (t, core) {
  t.snapshot(core.core.blocks.storage.toBuffer().toString('base64'), 'blocks')
  t.snapshot(core.core.tree.storage.toBuffer().toString('base64'), 'tree')
  t.snapshot(core.core.oplog.storage.toBuffer().toString('base64'), 'oplog')
}
