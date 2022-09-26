const test = require('brittle')

const { create, replicate } = require('../helpers')

test('range download, single block missing', async function (t) {
  const a = await create()
  const b = await create(a.key)

  const n = 100000

  for (let i = 0; i < n; i++) await a.append(Buffer.of(0))

  replicate(a, b, t)

  await b.download({ start: 0, end: n }).done()
  await b.clear(n - 1)

  const elapsed = await t.execution(async function () {
    await b.download({ start: 0, end: n }).done()
  })

  t.comment(elapsed)
})

test('range download, repeated', async function (t) {
  const a = await create()
  const b = await create(a.key)

  const n = 100000

  for (let i = 0; i < n; i++) await a.append(Buffer.of(0))

  replicate(a, b, t)

  await b.download({ start: 0, end: n }).done()

  const elapsed = await t.execution(async function () {
    for (let i = 0; i < 1000; i++) {
      await b.download({ start: 0, end: n }).done()
    }
  })

  t.comment(elapsed)
})
