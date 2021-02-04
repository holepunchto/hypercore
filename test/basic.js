const tape = require('tape')
const { create } = require('./helpers')

tape('basic', async function (t) {
  const core = await create()
  let appends = 0

  t.same(core.length, 0)
  t.same(core.byteLength, 0)

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
