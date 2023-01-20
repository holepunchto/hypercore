const test = require('brittle')
const { create, replicate } = require('./helpers')
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

  setTimeout(() => core.append('hi'), 3)
  t.alike(await core.get(0, { timeout: 30000 }), b4a.from('hi'))
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

  setTimeout(() => core.append('hi'), 3)
  t.alike(await core.get(0, { timeout: 0 }), b4a.from('hi'))
})

test('core constructor timeout but increase on get', async function (t) {
  t.plan(1)

  const core = await create({ timeout: 1 })

  setTimeout(() => core.append('hi'), 3)
  t.alike(await core.get(0, { timeout: 30000 }), b4a.from('hi'))
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

  setTimeout(() => a.append('hi'), 3)
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

  setTimeout(() => a.append('hi'), 3)
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

  setTimeout(() => core.append('hi'), 3)
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

  setTimeout(() => core.append('hi'), 3)
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

test('temp case: intensive timeouts (3ms)', async function (t) {
  t.plan(1000)

  const core = await create({ timeout: 1 })

  for (let i = 0; i < 1000; i++) {
    let resolved = null
    const promise = new Promise(resolve => {
      resolved = resolve
    })

    const get = core.get(i, { timeout: 1 })

    setTimeout(() => core.append('hi').then(resolved), 3)

    // It should fail because the block is being appended late
    try {
      await get
      t.fail('should have failed (index: ' + i + ')')
    } catch (err) {
      t.pass(err.code, 'got error')
    }

    await promise
  }
})

test('temp case: intensive timeouts (2ms)', async function (t) {
  t.plan(1000)

  const core = await create({ timeout: 1 })

  for (let i = 0; i < 1000; i++) {
    let resolved = null
    const promise = new Promise(resolve => {
      resolved = resolve
    })

    const get = core.get(i, { timeout: 1 })

    setTimeout(() => core.append('hi').then(resolved), 2)

    // It should sometimes fail and sometimes work because the block is in the middle of just in time
    try {
      await get
      t.pass('got block #' + i)
    } catch (err) {
      t.pass(err.code, 'got error')
    }

    await promise
  }
})

test('temp case: intensive timeouts (1ms)', async function (t) {
  t.plan(1000)

  const core = await create({ timeout: 1 })

  for (let i = 0; i < 1000; i++) {
    let resolved = null
    const promise = new Promise(resolve => {
      resolved = resolve
    })

    const get = core.get(i, { timeout: 1 })

    setTimeout(() => core.append('hi').then(resolved), 1)

    // It should always work
    try {
      await get
      t.fail('got block #' + i)
    } catch (err) {
      t.pass(err.code, 'got error')
    }

    await promise
  }
})
