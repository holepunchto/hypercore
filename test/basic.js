const tape = require('tape')
const { create } = require('./helpers')

tape('basic', async function (t) {
  const core = await create()

  t.same(core.length, 0)
  t.same(core.byteLength, 0)

  await core.append('hello')
  await core.append('world')

  t.same(core.length, 2)
  t.same(core.byteLength, 10)

  t.end()
})

tape('basic clone', async function (t) {
  const core = await create()
  const clone = await create(core.key)

  await core.append('hello')
  await core.append('a')
  await core.append('world')

  const p = await core.proof({ block: { index: 1, nodes: 0, value: true }, upgrade: { start: 0, length: 3 } })

  await clone.verify(p)

  t.same(clone.length, 3)
  t.same(clone.byteLength, 11)
  t.same(await clone.get(1), Buffer.from('a'))

  t.same(await clone.has(0), false)
  t.same(await clone.has(1), true)
  t.same(await clone.has(2), false)

  t.end()
})
