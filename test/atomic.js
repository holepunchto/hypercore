const test = require('brittle')
const b4a = require('b4a')
const createTempDir = require('test-tmp')

const Hypercore = require('../')
const { create, createStorage, eventFlush } = require('./helpers')

test('atomic - append', async function (t) {
  const core = await create(t)
  const core2 = await create(t)

  let appends = 0

  t.is(core.length, 0)
  t.is(core.writable, true)
  t.is(core.readable, true)

  core.on('append', function () {
    appends++
  })

  const atomizer = core.state.storage.atomizer()

  atomizer.enter()

  const promises = [
    core.append('1', { atomizer }),
    core2.append('2', { atomizer })
  ]

  await new Promise(resolve => setTimeout(resolve, 1000))

  t.is(core.length, 0)
  t.is(core2.length, 0)
  t.is(appends, 0)

  atomizer.exit()
  await Promise.all(promises)

  t.is(core.length, 1)
  t.is(core2.length, 1)
  t.is(appends, 1)
})
