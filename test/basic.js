const tape = require('tape')
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

  t.end()
})
