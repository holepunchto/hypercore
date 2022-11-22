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
  // This test will likely break when bitfields are optimised to not actually
  // store pages of all set or unset bits.

  const b = new Bitfield(new RAM(), new Uint32Array(1024 * 512 / 4 /* 512 KiB */))

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
  const b = await Bitfield.open(new RAM())

  // Previously bugged due to missing bounds check in sparse array
  b.set(7995511118690925, true)
})
