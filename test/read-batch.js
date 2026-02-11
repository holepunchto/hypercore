const test = require('brittle')
const b4a = require('b4a')
const HypercoreStorage = require('hypercore-storage')
const crypto = require('hypercore-crypto')

const Hypercore = require('../')
const { create, replicate, eventFlush } = require('./helpers')

test('basic', async function (t) {
  const core = await create(t)

  await core.append('hello')
  await core.append('world')

  t.is(core.length, 2)

  const read = core.read()

  const b0 = read.get(0)
  const b1 = read.get(1)

  read.tryFlush()

  t.alike(await b0, b4a.from('hello'))
  t.alike(await b1, b4a.from('world'))
})

test('replication', async function (t) {
  const core = await create(t)
  const other = await create(t, core.key)

  await core.append('hello')
  await core.append('world')

  t.is(core.length, 2)

  replicate(core, other, t)

  const read = other.read()

  const b0 = read.get(0)
  const b1 = read.get(1, { wait: false })

  read.tryFlush()

  t.alike(await b0, b4a.from('hello'))
  t.alike(await b1, null)
})

test('mixed replication', async function (t) {
  const core = await create(t)
  const other = await create(t, core.key)

  await core.append('hello')
  await core.append('world')

  t.is(core.length, 2)

  replicate(core, other, t)

  t.alike(await other.get(0), b4a.from('hello'))

  const read = other.read()

  const b0 = read.get(0)
  const b1 = read.get(1)

  read.tryFlush()

  t.alike(await b0, b4a.from('hello'))
  t.alike(await b1, b4a.from('world'))
})

test('destroy', async function (t) {
  const core = await create(t)
  const other = await create(t, core.key)

  await core.append('hello')
  await core.append('world')

  t.is(core.length, 2)

  replicate(core, other, t)

  t.alike(await other.get(0), b4a.from('hello'))

  const read = other.read()

  const exception = t.exception(read.get(0), /Batch is destroyed/)

  read.destroy()

  await exception

  await t.execution(other.close())
})
