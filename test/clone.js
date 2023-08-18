const test = require('brittle')
const crypto = require('hypercore-crypto')
const RAM = require('random-access-memory')

const { create } = require('./helpers')

test('clone', async function (t) {
  const core = await create()

  await core.append('hello')
  await core.append('world')

  const clone = await core.clone({ storage: RAM })
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

  const clone = await core.clone({ storage: RAM })
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

  const clone = await core.clone({ storage: RAM })
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

  const clone = await core.clone({ storage: RAM })
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
  batch.signature = crypto.sign(batch.signable(), keyPair.secretKey)
  const upgrade = await batch.proof({ upgrade: { start: 0, length: batch.length } })

  const clone = await core.clone({ storage: RAM, keyPair, upgrade })
  await clone.ready()

  t.is(clone.length, 4)
  t.not(clone.key, core.key)

  await t.execution(clone.append('final'))
})
