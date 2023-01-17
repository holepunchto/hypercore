const test = require('brittle')
const { create } = require('./helpers')
const b4a = require('b4a')

test('get before timeout', async function (t) {
  t.plan(1)

  const core = await create()

  const req = core.get(0, { timeout: 500 })

  req.then(
    (block) => {
      t.alike(block, b4a.from('hi'))
    },
    (err) => {
      t.fail(err.message)
    }
  )

  await core.append('hi')
})

test('get after timeout', async function (t) {
  t.plan(1)

  const core = await create()

  const req = core.get(0, { timeout: 500 })

  req.then(
    (block) => {
      console.log('block', block)
      t.fail('should not have got block')
    },
    (err) => {
      t.is(err.code, 'REQUEST_TIMEOUT')
    }
  )
})

test('get after 0ms timeout', async function (t) {
  t.plan(1)

  const core = await create()

  const req = core.get(0, { timeout: 0 })

  req.then(
    (block) => {
      console.log('block', block)
      t.fail('should not have got block')
    },
    (err) => {
      t.is(err.code, 'REQUEST_TIMEOUT')
    }
  )
})
