const test = require('brittle')
const { create, replicate } = require('./helpers')

test('upgrade to latest length', async function (t) {
  const a = await create()
  await a.append(['a', 'b', 'c'])

  const b = await create(a.key, {
    preupgrade (latest) {
      t.is(b.length, 0)
      t.is(latest.length, 3)

      return latest.length
    }
  })

  replicate(a, b, t)
  t.is(b.length, 0)

  t.ok(await b.update())
  t.is(b.length, 3)
})

test('stay on previous length', async function (t) {
  const a = await create()
  await a.append(['a', 'b', 'c'])

  const b = await create(a.key, {
    preupgrade (latest) {
      t.is(b.length, 0)
      t.is(latest.length, 3)

      return 0
    }
  })

  replicate(a, b, t)
  t.is(b.length, 0)

  t.absent(await b.update())
  t.is(b.length, 0)
})

test('return no length', async function (t) {
  const a = await create()
  await a.append(['a', 'b', 'c'])

  const b = await create(a.key, {
    preupgrade (latest) {
      t.is(b.length, 0)
      t.is(latest.length, 3)
    }
  })

  replicate(a, b, t)
  t.is(b.length, 0)

  t.ok(await b.update())
  t.is(b.length, 3)
})

test('return invalid length', async function (t) {
  const a = await create()
  await a.append(['a', 'b', 'c'])

  const b = await create(a.key, {
    preupgrade (latest) {
      t.is(b.length, 0)
      t.is(latest.length, 3)

      return -1
    }
  })

  replicate(a, b, t)
  t.is(b.length, 0)

  t.ok(await b.update())
  t.is(b.length, 3)
})
