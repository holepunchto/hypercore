const test = require('brittle')
const ram = require('random-access-memory')

const Hypercore = require('../')
const { create } = require('./helpers')

test('basic', async function (t) {
  const core = await create()
  let appends = 0

  t.is(core.length, 0)
  t.is(core.byteLength, 0)
  t.is(core.writable, true)
  t.is(core.readable, true)

  core.on('append', function () {
    appends++
  })

  await core.append('hello')
  await core.append('world')

  t.is(core.length, 2)
  t.is(core.byteLength, 10)
  t.is(appends, 2)

  t.end()
})

test('session', async function (t) {
  const core = await create()

  const session = core.session()

  await session.append('test')
  t.alike(await core.get(0), Buffer.from('test'))
  t.alike(await session.get(0), Buffer.from('test'))
  t.end()
})

test('close', async function (t) {
  const core = await create()
  await core.append('hello world')

  await core.close()

  try {
    await core.get(0)
    t.fail('core should be closed')
  } catch {
    t.pass('get threw correctly when core was closed')
  }
})

test('close multiple', async function (t) {
  const core = await create()
  await core.append('hello world')

  const ev = t.test('events')

  ev.plan(4)

  let i = 0

  core.on('close', () => ev.is(i++, 0, 'on close'))
  core.close().then(() => ev.is(i++, 1, 'first close'))
  core.close().then(() => ev.is(i++, 2, 'second close'))
  core.close().then(() => ev.is(i++, 3, 'third close'))

  await ev
})

test('storage options', async function (t) {
  const core = new Hypercore({ storage: ram })
  await core.append('hello')
  t.alike(await core.get(0), Buffer.from('hello'))
  t.end()
})
