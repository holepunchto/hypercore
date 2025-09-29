const test = require('brittle')
const b4a = require('b4a')
const CoreStorage = require('hypercore-storage')
const Bitfield = require('../lib/bitfield')
const BitInterlude = require('../lib/bit-interlude')

test('bitfield - set and get', async function (t) {
  const storage = await createStorage(t)
  const b = await Bitfield.open(storage)

  t.absent(await b.get(42))
  await b.set(42, true)
  t.ok(await b.get(42))

  // bigger offsets
  t.absent(await b.get(42000000))
  await b.set(42000000, true)
  t.ok(await b.get(42000000, true))
  await b.set(42000000, false)
  t.absent(await b.get(42000000, true))
})

test('bitfield - random set and gets', async function (t) {
  const b = await Bitfield.open(await createStorage(t), 0)
  const set = new Set()

  for (let i = 0; i < 200; i++) {
    const idx = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER)
    await b.set(idx, true)
    set.add(idx)
  }

  for (let i = 0; i < 500; i++) {
    const idx = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER)
    const expected = set.has(idx)
    const val = await b.get(idx, true)
    if (val !== expected) {
      t.fail('expected ' + expected + ' but got ' + val + ' at ' + idx)
      return
    }
  }

  for (const idx of set) {
    const val = await b.get(idx, true)
    if (val !== true) {
      t.fail('expected true but got ' + val + ' at ' + idx)
      return
    }
  }

  t.pass('all random set and gets pass')
})

test('bitfield - reload', async function (t) {
  const dir = await t.tmp()

  {
    const storage = await createStorage(t, dir)
    const bitfield = await Bitfield.open(storage)
    const b = new BitInterlude()
    b.setRange(142, 143, true)
    b.setRange(40000, 40001, true)
    b.setRange(1424242424, 1424242425, true)
    await flush(storage, b, bitfield)

    // fully close db
    await storage.db.close({ force: true })
  }

  {
    const storage = await createStorage(t, dir)
    const b = await Bitfield.open(storage, 1424242425)
    t.ok(b.get(142))
    t.ok(b.get(40000))
    t.ok(b.get(1424242424))

    // fully close db
    await storage.db.close({ force: true })
  }

  // Skipping because defining length isn't supported since it encoded in the storage
  // {
  //   const storage = await createStorage(t, dir)
  //   const b = await Bitfield.open(storage, 40001)
  //   t.ok(b.get(142))
  //   t.ok(b.get(40000))
  //   t.absent(b.get(1424242424))
  // }
})

test('bitfield - want', async function (t) {
  // This test will likely break when bitfields are optimised to not actually
  // store pages of all set or unset bits.

  const s = await createStorage(t, await t.tmp(), 1024 * 512)
  const b = await Bitfield.open(s /* 512 KiB */)

  t.alike(await Array.fromAsync(b.want(0, 0)), [])

  t.alike(
    await Array.fromAsync(b.want(0, 1)),
    [
      {
        start: 0,
        bitfield: new Uint32Array(1024 /* 4 KiB */)
      }
    ]
  )

  t.alike(
    await Array.fromAsync(b.want(0, 1024 * 4 * 8 /* 4 KiB */)),
    [
      {
        start: 0,
        bitfield: new Uint32Array(1024 /* 4 KiB */)
      }
    ]
  )

  t.alike(
    await Array.fromAsync(b.want(0, 1024 * 13 * 8 /* 13 KiB */)),
    [
      {
        start: 0,
        bitfield: new Uint32Array(1024 * 16 / 4 /* 16 KiB */)
      }
    ]
  )

  t.alike(
    await Array.fromAsync(b.want(0, 1024 * 260 * 8 /* 260 KiB */)),
    [
      {
        start: 0,
        bitfield: new Uint32Array(1024 * 256 / 4 /* 256 KiB */)
      },
      {
        start: 2 ** 18 * 8,
        bitfield: new Uint32Array(1024 /* 4 KiB */)
      }
    ]
  )
})

test('bitfield - sparse array overflow', async function (t) {
  const b = await Bitfield.open(await createStorage(t), 0)

  // Previously bugged due to missing bounds check in sparse array
  await b.set(7995511118690925, true)
})

test('bitfield - count', async function (t) {
  const s = await createStorage(t)
  const b = await Bitfield.open(s)

  for (const [start, end] of [
    [0, 2],
    [5, 6],
    [7, 9],
    [13, 14],
    [16, 19],
    [20, 25]
  ]) {
    await b.setRange(start, end, true)
  }

  t.is(await b.count(3, 18, true), 8)
  t.is(await b.count(3, 18, false), 10)
})

test('bitfield - find first, all zeroes', async function (t) {
  const b = await Bitfield.open(await createStorage(t), 0)

  t.is(await b.findFirst(false, 0), 0)
  t.is(await b.findFirst(true, 0), -1)

  t.comment('Page boundaries')
  t.is(await b.findFirst(false, 2 ** 15), 2 ** 15)
  t.is(await b.findFirst(false, 2 ** 15 - 1), 2 ** 15 - 1)
  t.is(await b.findFirst(false, 2 ** 15 + 1), 2 ** 15 + 1)
  t.is(await b.findFirst(false, 2 ** 16), 2 ** 16)
  t.is(await b.findFirst(false, 2 ** 16 - 1), 2 ** 16 - 1)
  t.is(await b.findFirst(false, 2 ** 16 + 1), 2 ** 16 + 1)

  t.comment('Segment boundaries')
  t.is(await b.findFirst(false, 2 ** 21), 2 ** 21)
  t.is(await b.findFirst(false, 2 ** 21 - 1), 2 ** 21 - 1)
  t.is(await b.findFirst(false, 2 ** 21 + 1), 2 ** 21 + 1)
  t.is(await b.findFirst(false, 2 ** 22), 2 ** 22)
  t.is(await b.findFirst(false, 2 ** 22 - 1), 2 ** 22 - 1)
  t.is(await b.findFirst(false, 2 ** 22 + 1), 2 ** 22 + 1)
})

test('bitfield - find first, all ones', async function (t) {
  const s = await createStorage(t)
  const b = await Bitfield.open(s)

  await b.setRange(0, 2 ** 24, true)

  t.is(await b.findFirst(true, 0), 0)
  t.is(await b.findFirst(true, 2 ** 24), -1)
  t.is(await b.findFirst(false, 0), 2 ** 24)
  t.is(await b.findFirst(false, 2 ** 24), 2 ** 24)

  t.comment('Page boundaries')
  t.is(await b.findFirst(true, 2 ** 15), 2 ** 15)
  t.is(await b.findFirst(true, 2 ** 15 - 1), 2 ** 15 - 1)
  t.is(await b.findFirst(true, 2 ** 15 + 1), 2 ** 15 + 1)
  t.is(await b.findFirst(true, 2 ** 16), 2 ** 16)
  t.is(await b.findFirst(true, 2 ** 16 - 1), 2 ** 16 - 1)
  t.is(await b.findFirst(true, 2 ** 16 + 1), 2 ** 16 + 1)

  t.comment('Segment boundaries')
  t.is(await b.findFirst(true, 2 ** 21), 2 ** 21)
  t.is(await b.findFirst(true, 2 ** 21 - 1), 2 ** 21 - 1)
  t.is(await b.findFirst(true, 2 ** 21 + 1), 2 ** 21 + 1)
  t.is(await b.findFirst(true, 2 ** 22), 2 ** 22)
  t.is(await b.findFirst(true, 2 ** 22 - 1), 2 ** 22 - 1)
  t.is(await b.findFirst(true, 2 ** 22 + 1), 2 ** 22 + 1)
})

test('bitfield - find last, all zeroes', async function (t) {
  const b = await Bitfield.open(await createStorage(t))

  t.is(await b.findLast(false, 0), 0)
  t.is(await b.findLast(true, 0), -1)

  t.comment('Page boundaries')
  t.is(await b.findLast(false, 2 ** 15), 2 ** 15)
  t.is(await b.findLast(false, 2 ** 15 - 1), 2 ** 15 - 1)
  t.is(await b.findLast(false, 2 ** 15 + 1), 2 ** 15 + 1)
  t.is(await b.findLast(false, 2 ** 16), 2 ** 16)
  t.is(await b.findLast(false, 2 ** 16 - 1), 2 ** 16 - 1)
  t.is(await b.findLast(false, 2 ** 16 + 1), 2 ** 16 + 1)

  t.comment('Segment boundaries')
  t.is(await b.findLast(false, 2 ** 21), 2 ** 21)
  t.is(await b.findLast(false, 2 ** 21 - 1), 2 ** 21 - 1)
  t.is(await b.findLast(false, 2 ** 21 + 1), 2 ** 21 + 1)
  t.is(await b.findLast(false, 2 ** 22), 2 ** 22)
  t.is(await b.findLast(false, 2 ** 22 - 1), 2 ** 22 - 1)
  t.is(await b.findLast(false, 2 ** 22 + 1), 2 ** 22 + 1)
})

test('bitfield - find last, all ones', async function (t) {
  const s = await createStorage(t)
  const b = await Bitfield.open(s, 0)

  await b.setRange(0, 2 ** 24, true)

  t.is(await b.findLast(false, 0), -1)
  t.is(await b.findLast(false, 2 ** 24), 2 ** 24)
  t.is(await b.findLast(true, 0), 0)
  t.is(await b.findLast(true, 2 ** 24), 2 ** 24 - 1)

  t.comment('Page boundaries')
  t.is(await b.findLast(true, 2 ** 15), 2 ** 15)
  t.is(await b.findLast(true, 2 ** 15 - 1), 2 ** 15 - 1)
  t.is(await b.findLast(true, 2 ** 15 + 1), 2 ** 15 + 1)
  t.is(await b.findLast(true, 2 ** 16), 2 ** 16)
  t.is(await b.findLast(true, 2 ** 16 - 1), 2 ** 16 - 1)
  t.is(await b.findLast(true, 2 ** 16 + 1), 2 ** 16 + 1)

  t.comment('Segment boundaries')
  t.is(await b.findLast(true, 2 ** 21), 2 ** 21)
  t.is(await b.findLast(true, 2 ** 21 - 1), 2 ** 21 - 1)
  t.is(await b.findLast(true, 2 ** 21 + 1), 2 ** 21 + 1)
  t.is(await b.findLast(true, 2 ** 22), 2 ** 22)
  t.is(await b.findLast(true, 2 ** 22 - 1), 2 ** 22 - 1)
  t.is(await b.findLast(true, 2 ** 22 + 1), 2 ** 22 + 1)
})

test('bitfield - find last, ones around page boundary', async function (t) {
  const s = await createStorage(t)
  const b = await Bitfield.open(s)

  await b.set(32767, true)
  await b.set(32768, true)

  t.is(await b.lastUnset(32768), 32766)
  t.is(await b.lastUnset(32769), 32769)
})

test('bitfield - set range on page boundary', async function (t) {
  const s = await createStorage(t)
  const b = await Bitfield.open(s)

  await b.setRange(2032, 2058, true)

  t.is(await b.findFirst(true, 2048), 2048)
})

test('set last bits in segment and findFirst', async function (t) {
  const s = await createStorage(t)
  const b = await Bitfield.open(s)

  await b.set(2097150, true)

  t.is(await b.findFirst(false, 2097150), 2097151)

  await b.set(2097151, true)

  t.is(await b.findFirst(false, 2097150), 2097152)
  t.is(await b.findFirst(false, 2097151), 2097152)
})

test('bitfield - setRange over multiple pages', async function (t) {
  const storage = await createStorage(t)
  const b = await Bitfield.open(storage)

  await b.setRange(32768, 32769, true)

  t.is(await b.get(0), false)
  t.is(await b.get(32768), true)
  t.is(await b.get(32769), false)

  await b.setRange(0, 32768 * 2, false)
  await b.setRange(32768, 32768 * 2 + 1, true)

  t.is(await b.get(0), false)
  t.is(await b.get(32768), true)
  t.is(await b.get(32768 * 2), true)
  t.is(await b.get(32768 * 2 + 1), false)
})

async function createStorage(t, dir, length) {
  if (!dir) dir = await t.tmp()

  const db = new CoreStorage(dir)

  t.teardown(() => db.close())

  const dkey = b4a.alloc(32)

  const storage = (await db.resume(dkey)) || (await db.create({ key: dkey, discoveryKey: dkey }))

  if (length) {
    const rx = storage.read()
    const headProm = rx.getHead()
    rx.tryFlush()
    const head = await headProm

    const tx = storage.write()
    const newHead = {
      fork: 0,
      rootHash: b4a.alloc(32),
      signature: b4a.alloc(32),
      ...head,
      length
    }
    tx.setHead(newHead)
    await tx.flush()
  }

  return storage
}

async function flush(s, b, bitfield) {
  const tx = s.write()
  await b.flush(tx, bitfield)
  await tx.flush()
}
