const test = require('brittle')
const b4a = require('b4a')
const createTempDir = require('test-tmp')
const CoreStorage = require('hypercore-on-the-rocks')
const Bitfield = require('../lib/bitfield')

test('bitfield - set and get', async function (t) {
  const b = await Bitfield.open(await createStorage(t))

  t.absent(b.get(42))
  b.set(42, true)
  t.ok(b.get(42))

  // bigger offsets
  t.absent(b.get(42000000))
  b.set(42000000, true)
  t.ok(b.get(42000000))

  b.set(42000000, false)
  t.absent(b.get(42000000))

  await b.flush()
})

test('bitfield - random set and gets', async function (t) {
  const b = await Bitfield.open(await createStorage(t))
  const set = new Set()

  for (let i = 0; i < 200; i++) {
    const idx = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER)
    b.set(idx, true)
    set.add(idx)
  }

  for (let i = 0; i < 500; i++) {
    const idx = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER)
    const expected = set.has(idx)
    const val = b.get(idx)
    if (val !== expected) {
      t.fail('expected ' + expected + ' but got ' + val + ' at ' + idx)
      return
    }
  }

  for (const idx of set) {
    const val = b.get(idx)
    if (val !== true) {
      t.fail('expected true but got ' + val + ' at ' + idx)
      return
    }
  }

  t.pass('all random set and gets pass')
})

test('bitfield - reload', async function (t) {
  const dir = await createTempDir(t)

  {
    const b = await Bitfield.open(await createStorage(t, dir))
    b.set(142, true)
    b.set(40000, true)
    b.set(1424242424, true)
    await b.flush()
    await b.storage.close()
  }

  {
    const b = await Bitfield.open(await createStorage(t, dir))
    t.ok(b.get(142))
    t.ok(b.get(40000))
    t.ok(b.get(1424242424))
  }
})

test('bitfield - want', async function (t) {
  // This test will likely break when bitfields are optimised to not actually
  // store pages of all set or unset bits.

  const b = new Bitfield(await createStorage(t), b4a.alloc(1024 * 512) /* 512 KiB */)

  t.alike([...b.want(0, 0)], [])

  t.alike([...b.want(0, 1)], [
    {
      start: 0,
      bitfield: new Uint32Array(1024 /* 4 KiB */)
    }
  ])

  t.alike([...b.want(0, 1024 * 4 * 8 /* 4 KiB */)], [
    {
      start: 0,
      bitfield: new Uint32Array(1024 /* 4 KiB */)
    }
  ])

  t.alike([...b.want(0, 1024 * 13 * 8 /* 13 KiB */)], [
    {
      start: 0,
      bitfield: new Uint32Array(1024 * 16 / 4 /* 16 KiB */)
    }
  ])

  t.alike([...b.want(0, 1024 * 260 * 8 /* 260 KiB */)], [
    {
      start: 0,
      bitfield: new Uint32Array(1024 * 256 / 4 /* 256 KiB */)
    },
    {
      start: 2 ** 18 * 8,
      bitfield: new Uint32Array(1024 /* 4 KiB */)
    }
  ])
})

test('bitfield - sparse array overflow', async function (t) {
  const b = await Bitfield.open(await createStorage(t))

  // Previously bugged due to missing bounds check in sparse array
  b.set(7995511118690925, true)
})

test('bitfield - count', async function (t) {
  const b = await Bitfield.open(await createStorage(t))

  for (const [start, length] of [[0, 2], [5, 1], [7, 2], [13, 1], [16, 3], [20, 5]]) {
    b.setRange(start, length, true)
  }

  t.is(b.count(3, 18, true), 8)
  t.is(b.count(3, 18, false), 10)
})

test('bitfield - find first, all zeroes', async function (t) {
  const b = await Bitfield.open(await createStorage(t))

  t.is(b.findFirst(false, 0), 0)
  t.is(b.findFirst(true, 0), -1)

  t.comment('Page boundaries')
  t.is(b.findFirst(false, 2 ** 15), 2 ** 15)
  t.is(b.findFirst(false, 2 ** 15 - 1), 2 ** 15 - 1)
  t.is(b.findFirst(false, 2 ** 15 + 1), 2 ** 15 + 1)
  t.is(b.findFirst(false, 2 ** 16), 2 ** 16)
  t.is(b.findFirst(false, 2 ** 16 - 1), 2 ** 16 - 1)
  t.is(b.findFirst(false, 2 ** 16 + 1), 2 ** 16 + 1)

  t.comment('Segment boundaries')
  t.is(b.findFirst(false, 2 ** 21), 2 ** 21)
  t.is(b.findFirst(false, 2 ** 21 - 1), 2 ** 21 - 1)
  t.is(b.findFirst(false, 2 ** 21 + 1), 2 ** 21 + 1)
  t.is(b.findFirst(false, 2 ** 22), 2 ** 22)
  t.is(b.findFirst(false, 2 ** 22 - 1), 2 ** 22 - 1)
  t.is(b.findFirst(false, 2 ** 22 + 1), 2 ** 22 + 1)
})

test('bitfield - find first, all ones', async function (t) {
  const b = await Bitfield.open(await createStorage(t))

  b.setRange(0, 2 ** 24, true)

  t.is(b.findFirst(true, 0), 0)
  t.is(b.findFirst(true, 2 ** 24), -1)
  t.is(b.findFirst(false, 0), 2 ** 24)
  t.is(b.findFirst(false, 2 ** 24), 2 ** 24)

  t.comment('Page boundaries')
  t.is(b.findFirst(true, 2 ** 15), 2 ** 15)
  t.is(b.findFirst(true, 2 ** 15 - 1), 2 ** 15 - 1)
  t.is(b.findFirst(true, 2 ** 15 + 1), 2 ** 15 + 1)
  t.is(b.findFirst(true, 2 ** 16), 2 ** 16)
  t.is(b.findFirst(true, 2 ** 16 - 1), 2 ** 16 - 1)
  t.is(b.findFirst(true, 2 ** 16 + 1), 2 ** 16 + 1)

  t.comment('Segment boundaries')
  t.is(b.findFirst(true, 2 ** 21), 2 ** 21)
  t.is(b.findFirst(true, 2 ** 21 - 1), 2 ** 21 - 1)
  t.is(b.findFirst(true, 2 ** 21 + 1), 2 ** 21 + 1)
  t.is(b.findFirst(true, 2 ** 22), 2 ** 22)
  t.is(b.findFirst(true, 2 ** 22 - 1), 2 ** 22 - 1)
  t.is(b.findFirst(true, 2 ** 22 + 1), 2 ** 22 + 1)
})

test('bitfield - find last, all zeroes', async function (t) {
  const b = await Bitfield.open(await createStorage(t))

  t.is(b.findLast(false, 0), 0)
  t.is(b.findLast(true, 0), -1)

  t.comment('Page boundaries')
  t.is(b.findLast(false, 2 ** 15), 2 ** 15)
  t.is(b.findLast(false, 2 ** 15 - 1), 2 ** 15 - 1)
  t.is(b.findLast(false, 2 ** 15 + 1), 2 ** 15 + 1)
  t.is(b.findLast(false, 2 ** 16), 2 ** 16)
  t.is(b.findLast(false, 2 ** 16 - 1), 2 ** 16 - 1)
  t.is(b.findLast(false, 2 ** 16 + 1), 2 ** 16 + 1)

  t.comment('Segment boundaries')
  t.is(b.findLast(false, 2 ** 21), 2 ** 21)
  t.is(b.findLast(false, 2 ** 21 - 1), 2 ** 21 - 1)
  t.is(b.findLast(false, 2 ** 21 + 1), 2 ** 21 + 1)
  t.is(b.findLast(false, 2 ** 22), 2 ** 22)
  t.is(b.findLast(false, 2 ** 22 - 1), 2 ** 22 - 1)
  t.is(b.findLast(false, 2 ** 22 + 1), 2 ** 22 + 1)
})

test('bitfield - find last, all ones', async function (t) {
  const b = await Bitfield.open(await createStorage(t))

  b.setRange(0, 2 ** 24, true)

  t.is(b.findLast(false, 0), -1)
  t.is(b.findLast(false, 2 ** 24), 2 ** 24)
  t.is(b.findLast(true, 0), 0)
  t.is(b.findLast(true, 2 ** 24), 2 ** 24 - 1)

  t.comment('Page boundaries')
  t.is(b.findLast(true, 2 ** 15), 2 ** 15)
  t.is(b.findLast(true, 2 ** 15 - 1), 2 ** 15 - 1)
  t.is(b.findLast(true, 2 ** 15 + 1), 2 ** 15 + 1)
  t.is(b.findLast(true, 2 ** 16), 2 ** 16)
  t.is(b.findLast(true, 2 ** 16 - 1), 2 ** 16 - 1)
  t.is(b.findLast(true, 2 ** 16 + 1), 2 ** 16 + 1)

  t.comment('Segment boundaries')
  t.is(b.findLast(true, 2 ** 21), 2 ** 21)
  t.is(b.findLast(true, 2 ** 21 - 1), 2 ** 21 - 1)
  t.is(b.findLast(true, 2 ** 21 + 1), 2 ** 21 + 1)
  t.is(b.findLast(true, 2 ** 22), 2 ** 22)
  t.is(b.findLast(true, 2 ** 22 - 1), 2 ** 22 - 1)
  t.is(b.findLast(true, 2 ** 22 + 1), 2 ** 22 + 1)
})

test('bitfield - find last, ones around page boundary', async function (t) {
  const b = await Bitfield.open(await createStorage(t))

  b.set(32767, true)
  b.set(32768, true)

  t.is(b.lastUnset(32768), 32766)
  t.is(b.lastUnset(32769), 32769)
})

test('bitfield - set range on page boundary', async function (t) {
  const b = await Bitfield.open(await createStorage(t))

  b.setRange(2032, 26, true)

  t.is(b.findFirst(true, 2048), 2048)
})

test('set last bits in segment and findFirst', async function (t) {
  const b = await Bitfield.open(await createStorage(t))

  b.set(2097150, true)
  t.is(b.findFirst(false, 2097150), 2097151)

  b.set(2097151, true)
  t.is(b.findFirst(false, 2097150), 2097152)
  t.is(b.findFirst(false, 2097151), 2097152)
})

async function createStorage (t, dir) {
  if (!dir) dir = await createTempDir(t)

  const db = new CoreStorage(dir)

  const dkey = b4a.alloc(32)

  const storage = db.get(dkey)
  if (!await storage.open()) await storage.create({ key: b4a.alloc(32) })

  return storage
}
