const test = require('brittle')
const crypto = require('hypercore-crypto')
const tmpDir = require('test-tmp')
const Hypercore = require('../')
const RAM = require('random-access-memory')

const { create, replicate } = require('./helpers')

test('clone', async function (t) {
  const core = await create()

  await core.append('hello')
  await core.append('world')

  const clone = core.clone(crypto.keyPair(), RAM)
  await clone.ready()

  const info = await clone.info()

  t.is(clone.length, 2)
  t.is(info.byteLength, 10)
  t.is(clone.writable, true)
  t.is(clone.readable, true)
})

test('clone - append after clone', async function (t) {
  const core = await create()

  await core.append('hello')
  await core.append('world')

  const clone = core.clone(crypto.keyPair(), RAM)
  await clone.ready()

  const hash = clone.core.tree.hash()

  await t.execution(clone.append('extra'))

  t.is(clone.length, 3)

  t.not(clone.length, core.length)
  t.unlike(clone.core.tree.hash(), hash)
  t.unlike(clone.core.tree.hash(), core.core.tree.hash())
})

test('clone - src appends after clone', async function (t) {
  const core = await create()

  await core.append('hello')
  await core.append('world')

  const clone = core.clone(crypto.keyPair(), RAM)
  await clone.ready()

  const hash = clone.core.tree.hash()

  await core.append('extra')

  t.is(clone.length, 2)

  t.not(clone.length, core.length)
  t.alike(clone.core.tree.hash(), hash)
  t.unlike(clone.core.tree.hash(), core.core.tree.hash())
})

test('clone - truncate src after', async function (t) {
  const core = await create()

  await core.append('hello')
  await core.append('world')
  await core.append('goodbye')
  await core.append('home')

  const clone = core.clone(crypto.keyPair(), RAM)
  await clone.ready()

  const hash = clone.core.tree.hash()

  await core.truncate(2)

  t.is(clone.length, 4)

  t.not(clone.length, core.length)
  t.alike(clone.core.tree.hash(), hash)
  t.unlike(clone.core.tree.hash(), core.core.tree.hash())
})

test('clone - pass new keypair', async function (t) {
  const core = await create()

  await core.append('hello')
  await core.append('world')
  await core.append('goodbye')
  await core.append('home')

  const keyPair = crypto.keyPair()

  const batch = core.core.tree.batch()
  const signature = crypto.sign(batch.signable(), keyPair.secretKey)

  const clone = core.clone(keyPair, RAM, { signature })
  await clone.ready()

  t.is(clone.length, 4)
  t.not(clone.key, core.key)

  await t.execution(clone.append('final'))
})

test('clone - sparse', async function (t) {
  const core = await create()
  const replica = await create(core.key, { manifest: core.manifest, sparse: true })

  await core.append('hello')
  await core.append('world')
  await core.append('goodbye')
  await core.append('home')

  replicate(core, replica, t)

  await replica.get(0)
  await replica.get(3)

  t.is(replica.length, 4)

  const clone = replica.clone(crypto.keyPair(), RAM)
  await clone.ready()

  t.is(clone.length, 4)

  t.alike(await clone.get(0), await core.get(0))
  t.alike(await clone.get(3), await core.get(3))

  const proof = await core.core.tree.proof({
    block: { index: 3, nodes: 2 }
  })

  // need to populate proof, see lib/replicator
  proof.block.value = await core.core.blocks.get(proof.block.index)

  await t.execution(clone.core.tree.verify(proof))
  await t.execution(clone.append('final'))
})

test('clone - replicate clones new key', async function (t) {
  const core = await create()

  const keyPair = crypto.keyPair()

  const clone = core.clone(keyPair, RAM)
  await clone.ready()

  await core.append('hello')
  await core.append('world')
  await core.append('goodbye')
  await core.append('home')

  const batch = core.core.tree.batch()
  const signature = crypto.sign(batch.signable(), keyPair.secretKey)

  const full = core.clone(keyPair, RAM, { signature })
  await full.ready()

  replicate(clone, full)

  await clone.get(0)
  await clone.get(3)

  t.is(core.length, 4)
  t.is(clone.length, 4)

  t.alike(await clone.get(0), await core.get(0))
  t.alike(await clone.get(3), await core.get(3))

  const proof = await core.core.tree.proof({
    block: { index: 3, nodes: 2 }
  })

  // need to populate proof, see lib/replicator
  proof.block.value = await core.core.blocks.get(proof.block.index)

  await t.execution(clone.core.tree.verify(proof))
  await t.execution(clone.append('final'))
})

test('clone - replicate sparse clone with new key', async function (t) {
  const core = await create()
  const replica = await create(core.key, { manifest: core.core.header.manifest, sparse: true })

  const keyPair = crypto.keyPair()

  await core.append('hello')
  await core.append('world')
  await core.append('goodbye')
  await core.append('home')

  const batch = core.core.tree.batch()
  const signature = crypto.sign(batch.signable(), keyPair.secretKey)

  const full = core.clone(keyPair, RAM, { signature })
  await full.ready()

  replicate(core, replica)

  await replica.get(0)
  await replica.get(3)

  t.is(replica.length, 4)

  const clone = replica.clone(keyPair, RAM, { signature })
  await clone.ready()

  t.is(clone.length, 4)

  replicate(clone, full)

  t.alike(await clone.get(1), await core.get(1))
  t.alike(await clone.get(2), await core.get(2))
})

test('clone - persist clone to disk', async function (t) {
  const core = await create()
  const storage = await tmpDir(t)

  const keyPair = crypto.keyPair()

  await core.append('hello')
  await core.append('world')
  await core.append('goodbye')
  await core.append('home')

  const batch = core.core.tree.batch()
  const signature = crypto.sign(batch.signable(), keyPair.secretKey)

  const clone = core.clone(keyPair, storage, { signature })
  await clone.ready()

  t.is(clone.length, 4)

  await clone.close()

  const reopened = new Hypercore(storage)
  await reopened.ready()

  t.alike(await reopened.get(0), await core.get(0))
  t.alike(await reopened.get(1), await core.get(1))
  t.alike(await reopened.get(2), await core.get(2))
  t.alike(await reopened.get(3), await core.get(3))

  await reopened.close()
})

test('clone - persisted clone with new key can replicate', async function (t) {
  const core = await create()
  const storage = await tmpDir(t)

  const keyPair = crypto.keyPair()

  await core.append('hello')
  await core.append('world')

  let batch = core.core.tree.batch()
  let signature = crypto.sign(batch.signable(), keyPair.secretKey)

  const clone = core.clone(keyPair, storage, { signature })
  await clone.ready()

  await core.append('goodbye')
  await core.append('home')

  batch = core.core.tree.batch()
  signature = crypto.sign(batch.signable(), keyPair.secretKey)

  const fullClone = core.clone(keyPair, RAM, { signature, compat: true })
  await fullClone.ready()

  t.is(clone.length, 2)
  t.is(fullClone.length, 4)

  await clone.close()

  const reopened = new Hypercore(storage)
  await reopened.ready()

  t.is(reopened.length, 2)
  replicate(reopened, fullClone, t)

  t.alike(await reopened.get(0), await core.get(0))
  t.alike(await reopened.get(1), await core.get(1))
  t.alike(await reopened.get(2), await core.get(2))
  t.alike(await reopened.get(3), await core.get(3))

  t.is(reopened.length, 4)

  await reopened.close()
})
