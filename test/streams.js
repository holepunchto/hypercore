const test = require('brittle')
const b4a = require('b4a')

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
    t.alike(b4a.toString(data), expected.shift())
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
      t.alike(b4a.toString(data), expected.shift())
    }

    t.is(expected.length, 0)
  }

  {
    const expected = datas.slice(2, 3)

    for await (const data of core.createReadStream({ start: 2, end: 3 })) {
      t.alike(b4a.toString(data), expected.shift())
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
    t.alike(b4a.toString(data), expected.shift())
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
    t.alike(b4a.toString(data), expected.shift())
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
    t.alike(b4a.toString(data), expected.shift())
  }

  t.is(expected.length, 0)
})

test('basic byte stream with byteOffset / byteLength of a core that has valueEncoding', async function (t) {
  const core = await create({ valueEncoding: 'utf8' })

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
    t.ok(b4a.isBuffer(data))
    t.alike(b4a.toString(data), expected.shift())
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
    t.alike(b4a.toString(data), expected.shift())
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
    t.alike(b4a.toString(data), expected.shift())
  }

  t.is(expected.length, 0)
})

test('read stream should not be interrupted if core has been cleared earlier', async function (t) {
  const core = await create()

  const append = ['a', 'b', 'c']
  await core.append(append)

  for await (const data of core.createReadStream()) {
    t.alike(b4a.toString(data), append.shift())
  }

  t.is(core.contiguousLength, 3)

  await core.clear(2)

  t.is(core.contiguousLength, 2)

  const afterClear = ['a', 'b']
  for await (const data of core.createReadStream()) {
    t.alike(b4a.toString(data), afterClear.shift())
  }
})

test('read stream should return all non-cleared values', async function (t) {
  const core = await create()

  const append = ['a', 'b', 'c', 'd']
  await core.append(append)

  for await (const data of core.createReadStream()) {
    t.alike(b4a.toString(data), append.shift())
  }

  t.is(core.contiguousLength, 4)

  await core.clear(1, 3)

  t.is(core.contiguousLength, 1)

  const afterClear = ['a', 'd']
  for await (const data of core.createReadStream()) {
    const v = afterClear.shift()
    t.alike(b4a.toString(data), v, v)
  }
})
