const tape = require('tape')
const ram = require('random-access-memory')

const Hypercore = require('../')
const { create } = require('./helpers')

tape('basic', async function (t) {
  const core = await create()
  let appends = 0

  t.same(core.length, 0)
  t.same(core.byteLength, 0)
  t.same(core.writable, true)
  t.same(core.readable, true)

  core.on('append', function () {
    appends++
  })

  await core.append('hello')
  await core.append('world')

  t.same(core.length, 2)
  t.same(core.byteLength, 10)
  t.same(appends, 2)

  t.end()
})

tape('session', async function (t) {
  const core = await create()

  const session = core.session()

  await session.append('test')
  t.same(await core.get(0), Buffer.from('test'))
  t.same(await session.get(0), Buffer.from('test'))
  t.end()
})

tape('close', async function (t) {
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

tape('close multiple', async function (t) {
  const core = await create()
  await core.append('hello world')

  const expected = ['close event', 'close 1', 'close 2', 'close 3']

  core.on('close', () => done('close event'))
  core.close().then(() => done('close 1'))
  core.close().then(() => done('close 2'))
  core.close().then(() => done('close 3'))

  await core.close()
  t.same(expected.length, 0, 'all event passed')

  function done (event) {
    t.same(event, expected.shift())
  }
})

tape('storage options', async function (t) {
  const core = new Hypercore({ storage: ram })
  await core.append('hello')
  t.same(await core.get(0), Buffer.from('hello'))
  t.end()
})
