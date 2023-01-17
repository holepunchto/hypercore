const test = require('brittle')
const { create } = require('./helpers')
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
