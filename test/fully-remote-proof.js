const test = require('brittle')
const b4a = require('b4a')
const remote = require('../lib/fully-remote-proof.js')
const { MerkleTree } = require('../lib/merkle-tree.js')

const { create, replicate, unreplicate } = require('./helpers')

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

test('fully remote proof - stale length check during concurrent update', async function (t) {
  const writer = await create(t)
  await writer.append('hello')

  const reader = await create(t, writer.key)
  const streams = replicate(writer, reader, t, { teardown: false })
  await reader.get(0)
  await unreplicate(streams)
  await writer.append('world')

  const proofBuf = await remote.proof(writer)

  // monkey-patch to simulate a concurrent storage update
  const origFn = MerkleTree.verifyFullyRemote
  t.teardown(() => {
    MerkleTree.verifyFullyRemote = origFn
  })

  MerkleTree.verifyFullyRemote = function (...args) {
    const result = origFn.apply(this, args)
    return (async () => {
      const s = replicate(writer, reader, t, { teardown: false })
      await reader.get(1)
      await unreplicate(s)
      return result
    })()
  }

  const verifyResult = await remote.verify(reader.state.storage.store, proofBuf)
  MerkleTree.verifyFullyRemote = origFn

  t.is(reader.length, 2, 'reader was updated during verification')

  t.is(
    verifyResult.newer,
    false,
    'should not be newer since storage head was updated during verification'
  )
})
