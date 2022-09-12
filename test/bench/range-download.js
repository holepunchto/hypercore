const test = require('brittle')

const { create, replicate } = require('../helpers')

test('range download', async function (t) {
  const a = await create()
  const b = await create(a.key)

  const n = 10000

  for (let i = 0; i < n; i++) await a.append(`${i}`)

  replicate(a, b, t)

  const elapsed = await t.execution(async function () {
    await b.download({ start: 0, end: n }).done()
  })

  t.comment(elapsed)
})
