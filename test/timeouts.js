const test = require('brittle')
const { create, replicate } = require('./helpers')
const b4a = require('b4a')

test('get before timeout', async function (t) {
  t.plan(1)

  const core = await create()

  const req = core.get(0, { timeout: 500 })

  req.then((block) => {
    t.alike(block, b4a.from('hi'))
  }).catch((err) => {
    t.fail(err.message)
  })

  await core.append('hi')
})

test('get after timeout', async function (t) {
  t.plan(1)

  const core = await create()

  const req = core.get(0, { timeout: 500 })

  req.then((block) => {
    t.fail('should not have got block: ' + block)
  }).catch((err) => {
    t.is(err.code, 'REQUEST_TIMEOUT')
  })
})

test('get after 0ms timeout', async function (t) {
  t.plan(1)

  const core = await create()

  const req = core.get(0, { timeout: 0 })

  req.then((block) => {
    t.fail('should not have got block: ' + block)
  }).catch((err) => {
    t.is(err.code, 'REQUEST_TIMEOUT')
  })
})

test('timeout but hits cache (remote await)', async function (t) {
  t.plan(2)

  const a = await create({ cache: true })
  const b = await create(a.key, { cache: true })
  replicate(a, b, t)

  try {
    const block = await b.get(0, { timeout: 0 })
    t.fail('should not have got block: ' + block)
  } catch (err) {
    t.is(err.code, 'REQUEST_TIMEOUT', 'first request failed')
  }

  setTimeout(() => {
    a.append('hi')
  }, 100)

  t.alike(await b.get(0), b4a.from('hi'), 'second request succeed')
})

test('timeout but hits cache (remote parallel)', async function (t) {
  t.plan(2)

  const a = await create({ cache: true })
  const b = await create(a.key, { cache: true })
  replicate(a, b, t)

  const b1 = b.get(0, { timeout: 0 })
  const b2 = b.get(0)

  try {
    const block = await b1
    t.fail('should not have got block: ' + block)
  } catch (err) {
    t.is(err.code, 'REQUEST_TIMEOUT', 'first request failed')
  }

  setTimeout(() => {
    a.append('hi')
  }, 100)

  t.alike(await b2, b4a.from('hi'), 'second request succeed')
})

test('timeout but hits cache (await)', async function (t) {
  t.plan(2)

  const core = await create({ cache: true })

  try {
    await core.get(0, { timeout: 0 })
  } catch (error) {
    t.is(error.code, 'REQUEST_TIMEOUT')
  }

  await core.append('hi')
  t.alike(await core.get(0), b4a.from('hi'))
})

test('timeout but hits cache (parallel)', async function (t) {
  t.plan(2)

  const core = await create({ cache: true })

  const b1 = core.get(0, { timeout: 0 })
  const b2 = core.get(0)

  try {
    await b1
  } catch (err) {
    t.is(err.code, 'REQUEST_TIMEOUT')
  }

  setTimeout(() => {
    core.append('hi')
  }, 100)

  t.alike(await b2, b4a.from('hi'))
})

test('get after timeout with await', async function (t) {
  t.plan(2)

  const core = await create()

  await core.append('sup')
  t.alike(await core.get(0, { timeout: 500 }), b4a.from('sup'))

  try {
    const block = await core.get(1, { timeout: 500 })
    t.fail('should not have got block: ' + block)
  } catch (err) {
    t.is(err.code, 'REQUEST_TIMEOUT')
  }
})

test('block request gets cancelled before timeout', async function (t) {
  t.plan(1)

  const core = await create()

  const promise = core.get(0, { timeout: 500 })

  const b = core.replicator._blocks.get(0)
  b.detach(b.refs[0])

  try {
    await promise
  } catch (err) {
    t.is(err.code, 'REQUEST_CANCELLED')
  }
})
