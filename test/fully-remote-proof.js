const test = require('brittle')
const b4a = require('b4a')
const c = require('compact-encoding')
const remote = require('../lib/fully-remote-proof.js')
const messages = require('../lib/messages.js')

const { create } = require('./helpers')

test('fully remote proof - proof and verify', async function (t) {
  const core = await create(t)

  await core.append('hello')
  await core.append('world')

  {
    const proof = await remote.proof(core)
    t.ok(await remote.verify(core.state.storage.store, proof))
  }

  {
    const proof = await remote.proof(core, { index: 0, block: b4a.from('hello') })
    const p = await remote.verify(core.state.storage.store, proof)
    t.is(p.block.index, 0)
    t.alike(p.block.value, b4a.from('hello'))
  }

  {
    const proof = await remote.proof(core, { index: 0, block: b4a.from('hello') })
    const p = await remote.verify(core.state.storage.store, proof, { referrer: b4a.alloc(32) })
    t.is(p, null)
  }

  {
    const proof = await remote.proof(core, { index: 0, upgrade: { start: 0, length: 1 } })
    const p = await remote.verify(core.state.storage.store, proof)
    t.is(p.proof.upgrade.length, 1, 'reflects upgrade arg')
  }
})

test('fully remote proof - block, hash or seek not covered by the upgrade', async function (t) {
  const core = await create(t)
  const store = core.state.storage.store

  await core.append(['a', 'b', 'c', 'd'])

  const proof = c.decode(messages.wire.data, (await remote.proof(core)).subarray(32))
  const encode = (data) => b4a.concat([core.discoveryKey, c.encode(messages.wire.data, data)])

  const uncovered = [
    { block: { index: 1, value: b4a.from('x'), nodes: [] } },
    { hash: { index: 5, nodes: [{ index: 5, hash: b4a.alloc(32), size: 2 }] } },
    { seek: { bytes: 3, nodes: [{ index: 1, hash: b4a.alloc(32), size: 3 }] } }
  ]

  for (const u of uncovered) {
    t.is(await remote.verify(store, encode({ ...proof, ...u })), null)
  }

  const opts = { index: 3, block: b4a.from('d'), upgrade: { start: 0, length: 2 } }
  const p = await remote.verify(store, await remote.proof(core, opts))
  t.alike(p.block.value, b4a.from('d'), 'block past the upgrade still verifies')
})
