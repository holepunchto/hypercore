const test = require('brittle')
const { create, replicate } = require('./helpers')

test('preupgrade', async function (t) {
  const a = await create()
  const b = await create(a.key, {
    preupgrade (latest) {
      return latest.length
    }
  })

  replicate(a, b, t)

  await a.append(['a', 'b', 'c'])
  await b.update()

  t.pass()
})
