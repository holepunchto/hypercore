const test = require('brittle')
const RAM = require('random-access-memory')
const Bitfield = require('../lib/bitfield')

test('bitfield - set and get', async function (t) {
  const b = await Bitfield.open(new RAM())

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
  const b = await Bitfield.open(new RAM())
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
  const s = new RAM()

  {
    const b = await Bitfield.open(s)
    b.set(142, true)
    b.set(40000, true)
    b.set(1424242424, true)
    await b.flush()
  }

  {
    const b = await Bitfield.open(s)
    t.ok(b.get(142))
    t.ok(b.get(40000))
    t.ok(b.get(1424242424))
  }
})

test('bitfield - want', async function (t) {
  const b = await Bitfield.open(new RAM())

  // Ensure 2 pages
  b.set(0, true)
  b.set(0, false)
  b.set(2 ** 18 * 8, true)
  b.set(2 ** 18 * 8, false)

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
