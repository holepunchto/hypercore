const test = require('brittle')
const { create } = require('./helpers')
const Hypercore = require('../')
const RAM = require('random-access-memory')
const b4a = require('b4a')

test('core and session timeout property', async function (t) {
  t.plan(3)

  const core = new Hypercore(RAM)
  t.is(core.timeout, 0)

  const a = core.session()
  t.is(a.timeout, 0)

  const b = core.session({ timeout: 50 })
  t.is(b.timeout, 50)
})

test('core session inherits timeout property', async function (t) {
  t.plan(3)

  const core = new Hypercore(RAM, { timeout: 50 })
  t.is(core.timeout, 50)

  const a = core.session()
  t.is(a.timeout, 50)

  const b = core.session({ timeout: 0 })
  t.is(b.timeout, 0)
})

test('get before timeout', async function (t) {
  t.plan(1)

  const core = await create()

  const get = core.get(0, { timeout: 30000 })
  setTimeout(() => core.append('hi'), 100)
  t.alike(await get, b4a.from('hi'))
})

test('get after timeout', async function (t) {
  t.plan(1)

  const core = await create()

  try {
    await core.get(0, { timeout: 1 })
    t.fail('should not get a block')
  } catch (err) {
    t.is(err.code, 'REQUEST_TIMEOUT')
  }
})

test('get after timeout with constructor', async function (t) {
  t.plan(1)

  const core = await create({ timeout: 1 })

  try {
    await core.get(0)
    t.fail('should not get a block')
  } catch (err) {
    t.is(err.code, 'REQUEST_TIMEOUT')
  }
})

test('session get after timeout', async function (t) {
  t.plan(1)

  const core = await create()
  const session = core.session({ timeout: 1 })

  try {
    await session.get(0)
    t.fail('should not get a block')
  } catch (err) {
    t.is(err.code, 'REQUEST_TIMEOUT')
  }
})

test('session get after inherited timeout', async function (t) {
  t.plan(1)

  const core = await create({ timeout: 1 })
  const session = core.session()

  try {
    await session.get(0)
    t.fail('should not get a block')
  } catch (err) {
    t.is(err.code, 'REQUEST_TIMEOUT')
  }
})

test('core constructor timeout but disable on get', async function (t) {
  t.plan(1)

  const core = await create({ timeout: 1 })

  const get = core.get(0, { timeout: 0 })
  setTimeout(() => core.append('hi'), 100)
  t.alike(await get, b4a.from('hi'))
})

test('core constructor timeout but increase on get', async function (t) {
  t.plan(1)

  const core = await create({ timeout: 1 })

  const get = core.get(0, { timeout: 30000 })
  setTimeout(() => core.append('hi'), 100)
  t.alike(await get, b4a.from('hi'))
})

test('block request gets cancelled before timeout', async function (t) {
  t.plan(1)

  const core = await create()

  const a = core.session()
  const promise = a.get(0, { timeout: 1 })
  const close = a.close()

  try {
    await promise
    t.fail('should have failed')
  } catch (err) {
    t.is(err.code, 'SESSION_CLOSED')
  }

  await close
})
