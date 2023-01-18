const test = require('brittle')
const { create, replicate } = require('./helpers')
const b4a = require('b4a')

test('get before timeout', async function (t) {
  t.plan(1)

  const core = await create()
  setImmediate(() => core.append('hi'))

  const block = await core.get(0, { timeout: 30000 })
  t.alike(block, b4a.from('hi'))
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

test('timeout but tries to hit cache (remote await)', async function (t) {
  t.plan(2)

  const a = await create({ cache: true })
  const b = await create(a.key, { cache: true })
  replicate(a, b, t)

  try {
    const block = await b.get(0, { timeout: 1 })
    t.fail('should not get a block: ' + block)
  } catch (err) {
    t.is(err.code, 'REQUEST_TIMEOUT', 'first request failed')
  }

  setImmediate(() => a.append('hi'))
  t.alike(await b.get(0), b4a.from('hi'), 'second request succeed')
})

test('timeout but tries to hit cache (remote parallel)', async function (t) {
  t.plan(2)

  const a = await create({ cache: true })
  const b = await create(a.key, { cache: true })
  replicate(a, b, t)

  const b1 = b.get(0, { timeout: 1 })
  const b2 = b.get(0)

  try {
    const block = await b1
    t.fail('should not get a block: ' + block)
  } catch (err) {
    t.is(err.code, 'REQUEST_TIMEOUT', 'first request failed')
  }

  setImmediate(() => a.append('hi'))
  t.alike(await b2, b4a.from('hi'), 'second request succeed')
})

test('timeout but tries to hit cache (await)', async function (t) {
  t.plan(2)

  const core = await create({ cache: true })

  try {
    await core.get(0, { timeout: 1 })
    t.fail('should have failed')
  } catch (err) {
    t.is(err.code, 'REQUEST_TIMEOUT')
  }

  setImmediate(() => core.append('hi'))
  t.alike(await core.get(0), b4a.from('hi'))
})

test('timeout but hits cache (parallel)', async function (t) {
  t.plan(2)

  const core = await create({ cache: true })

  const b1 = core.get(0, { timeout: 1 })
  const b2 = core.get(0)

  try {
    await b1
  } catch (err) {
    t.is(err.code, 'REQUEST_TIMEOUT')
  }

  setImmediate(() => core.append('hi'))
  t.alike(await b2, b4a.from('hi'))
})

test('get after timeout with await', async function (t) {
  t.plan(2)

  const core = await create()

  await core.append('sup')
  t.alike(await core.get(0, { timeout: 1 }), b4a.from('sup'))

  try {
    const block = await core.get(1, { timeout: 1 })
    t.fail('should not get a block: ' + block)
  } catch (err) {
    t.is(err.code, 'REQUEST_TIMEOUT')
  }
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
