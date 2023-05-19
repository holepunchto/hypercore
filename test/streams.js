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

test('basic write+read stream', async function (t) {
  const core = await create()

  const expected = [
    'hello',
    'world',
    'verden',
    'welt'
  ]

  const ws = core.createWriteStream()

  for (const data of expected) ws.write(data)
  ws.end()

  await new Promise(resolve => ws.on('finish', resolve))

  for await (const data of core.createReadStream()) {
    t.alike(data.toString(), expected.shift())
  }

  t.is(expected.length, 0)
})

test('basic byte stream', async function (t) {
  const core = await create()

  const expected = [
    'hello',
    'world',
    'verden',
    'welt'
  ]

  await core.append(expected)

  for await (const data of core.createByteStream()) {
    t.alike(data.toString(), expected.shift())
  }

  t.is(expected.length, 0)
})

test('basic byte stream with byteOffset / byteLength', async function (t) {
  const core = await create()

  await core.append([
    'hello',
    'world',
    'verden',
    'welt'
  ])

  const opts = { byteOffset: 5, byteLength: 11 }
  const expected = [
    'world',
    'verden'
  ]

  for await (const data of core.createByteStream(opts)) {
    t.alike(data.toString(), expected.shift())
  }

  t.is(expected.length, 0)
})

test('byte stream with lower byteLength than byteOffset', async function (t) {
  const core = await create()

  await core.append([
    'hello',
    'world',
    'verden',
    'welt'
  ])

  const opts = { byteOffset: 10, byteLength: 6 }
  const expected = [
    'verden'
  ]

  for await (const data of core.createByteStream(opts)) {
    t.alike(data.toString(), expected.shift())
  }

  t.is(expected.length, 0)
})

test('basic byte stream with custom byteOffset but default byteLength', async function (t) {
  const core = await create()

  await core.append([
    'hello',
    'world',
    'verden',
    'welt'
  ])

  const opts = { byteOffset: 10 }
  const expected = [
    'verden',
    'welt'
  ]

  for await (const data of core.createByteStream(opts)) {
    t.alike(data.toString(), expected.shift())
  }

  t.is(expected.length, 0)
})
