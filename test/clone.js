const test = require('brittle')
const crypto = require('hypercore-crypto')
const RAM = require('random-access-memory')

const { create, replicate } = require('./helpers')

test('clone', async function (t) {
  const core = await create()

  await core.append('hello')
  await core.append('world')

  const clone = await core.clone(RAM)
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

  const clone = await core.clone(RAM)
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

  const clone = await core.clone(RAM)
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

  const clone = await core.clone(RAM)
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

  const batch = await core.core.tree.batch()
  const signature = crypto.sign(batch.signable(), keyPair.secretKey)

  const clone = await core.clone(RAM, { keyPair, signature })
  await clone.ready()

  t.is(clone.length, 4)
  t.not(clone.key, core.key)

  await t.execution(clone.append('final'))
})

test('clone - sparse', async function (t) {
  const core = await create()
  const replica = await create(core.key, { auth: core.core.defaultAuth, sparse: true })

  await core.append('hello')
  await core.append('world')
  await core.append('goodbye')
  await core.append('home')

  replicate(core, replica)

  await replica.get(0)
  await replica.get(3)

  t.is(replica.length, 4)

  const clone = await replica.clone(RAM)
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
