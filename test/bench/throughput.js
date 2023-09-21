const test = require('brittle')
const tmp = require('test-tmp')
const b4a = require('b4a')
const { create, replicate } = require('../helpers')
const Hypercore = require('../../index.js')

test('throughput from disk', async function (t) {
  const dir = await tmp(t)

  const a = new Hypercore(dir)
  await a.append(new Array(20000).fill().map(() => b4a.alloc(1)))

  const b = await create(a.key)
  replicate(a, b, t)

  const elapsed = await t.execution(async function () {
    await b.download({ start: 0, end: a.length }).done()
  })

  t.comment(elapsed)

  await a.close()
  await b.close()
})
