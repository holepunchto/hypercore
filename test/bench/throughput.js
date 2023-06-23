const test = require('brittle')
const { create, replicate, createTmpDir } = require('../helpers')
const Hypercore = require('../../index.js')

test('throughput from disk', async function (t) {
  const tmp = createTmpDir(t)

  const a = new Hypercore(tmp)
  await a.append(new Array(20000).fill().map(() => Buffer.alloc(1)))

  const b = await create(a.key)
  replicate(a, b, t)

  const elapsed = await t.execution(async function () {
    await b.download({ start: 0, end: a.length }).done()
  })

  t.comment(elapsed)

  await a.close()
  await b.close()
})
