const test = require('brittle')
const b4a = require('b4a')
const c = require('compact-encoding')
const remote = require('../lib/fully-remote-proof.js')
const messages = require('../lib/messages.js')

const { create, replicate } = require('./helpers')

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

// Simulates the race from #791: the first head read inside verify() returns a
// stale head, as if a concurrent notification advanced storage right after it
// was read.
function wrapStoreWithStaleHead(store, staleLength) {
  return new Proxy(store, {
    get(target, prop, receiver) {
      if (prop !== 'resumeCore') return Reflect.get(target, prop, receiver)
      return async function (discoveryKey) {
        const core = await target.resumeCore(discoveryKey)
        if (core === null) return null
        let first = true
        const read = core.read.bind(core)
        core.read = function (...args) {
          const rx = read(...args)
          if (first) {
            first = false
            const getHead = rx.getHead.bind(rx)
            rx.getHead = async function (...headArgs) {
              const head = await getHead(...headArgs)
              if (head === null) return null
              return { ...head, length: staleLength }
            }
          }
          return rx
        }
        return core
      }
    }
  })
}

test('fully remote proof - newer uses freshest head', async function (t) {
  const a = await create(t)
  await a.append(['a', 'b', 'c'])

  const b = await create(t, a.key)
  replicate(a, b, t)
  await b.get(2)

  const proof = await remote.proof(a)

  const store = wrapStoreWithStaleHead(b.state.storage.store, 2)
  const p = await remote.verify(store, proof)

  t.ok(p !== null, 'proof verifies')
  t.is(p.length, 3, 'proof length reported')
  t.is(p.newer, false, 'storage already has the proof length, so it is not newer')
})
