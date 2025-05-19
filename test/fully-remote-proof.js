const test = require('brittle')
const b4a = require('b4a')
const remote = require('../lib/fully-remote-proof.js')

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
})
