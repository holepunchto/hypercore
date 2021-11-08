const test = require('brittle')

const { create } = require('./helpers')

test('basic read stream', async function (t) {
  const core = await create()

  const expected = [
    'hello',
    'world',
    'verden',
    'welt'
  ]

  await core.append(expected)

  for await (const data of core.createReadStream()) {
    t.alike(data.toString(), expected.shift())
  }

  t.is(expected.length, 0)
})

test('read stream with start / end', async function (t) {
  const core = await create()

  const datas = [
    'hello',
    'world',
    'verden',
    'welt'
  ]

  await core.append(datas)

  {
    const expected = datas.slice(1)

    for await (const data of core.createReadStream({ start: 1 })) {
      t.alike(data.toString(), expected.shift())
    }

    t.is(expected.length, 0)
  }

  {
    const expected = datas.slice(2, 3)

    for await (const data of core.createReadStream({ start: 2, end: 3 })) {
      t.alike(data.toString(), expected.shift())
    }

    t.is(expected.length, 0)
  }
})
