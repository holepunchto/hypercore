const test = require('brittle')
const BitInterlude = require('../lib/bit-interlude')

test('bit-interlude - basic', (t) => {
  const bits = new BitInterlude()

  bits.setRange(0, 5, true)
  bits.setRange(10, 15, true)
  bits.setRange(16, 20, true)

  t.is(bits.get(3), true)
  t.is(bits.get(7), false)
  t.is(bits.get(10), true)
  t.is(bits.get(15), false)
  t.is(bits.get(18), true)

  t.is(bits.contiguousLength(0), 5)
  t.is(bits.contiguousLength(10), 15)
  t.is(bits.contiguousLength(16), 20)
})

test('bit-interlude - drop', (t) => {
  const bits = new BitInterlude()
  bits.setRange(0, 20, true)

  bits.setRange(15, 20, false)

  t.is(bits.get(7), true)
  t.is(bits.get(15), false)
  t.is(bits.get(18), false)

  t.is(bits.contiguousLength(0), 15)
  t.is(bits.contiguousLength(16), 15)
})

test('bit-interlude - drop multiple', (t) => {
  const bits = new BitInterlude()
  bits.setRange(0, 20, true)

  bits.setRange(0, 10, false)
  bits.setRange(15, 20, false)

  t.is(bits.get(7), false)
  t.is(bits.get(12), true)
  t.is(bits.get(15), false)
  t.is(bits.get(18), false)

  t.is(bits.contiguousLength(8), 0)
  t.is(bits.contiguousLength(12), 0)
  t.is(bits.contiguousLength(16), 0)
})

test('bit-interlude - set & drop', (t) => {
  const bits = new BitInterlude()

  bits.setRange(0, 10, true)
  bits.setRange(7, 12, false)
  bits.setRange(15, 20, true)
  bits.setRange(2, 3, false)

  t.is(bits.get(0), true)
  t.is(bits.get(2), false)
  t.is(bits.get(3), true)
  t.is(bits.get(7), false)
  t.is(bits.get(12), false)
  t.is(bits.get(15), true)
  t.is(bits.get(18), true)

  t.is(bits.contiguousLength(8), 2)
  t.is(bits.contiguousLength(12), 2)
  t.is(bits.contiguousLength(16), 2)
})

test('bit-interlude - setRange bridges undefine region updates higher range', (t) => {
  const bits = new BitInterlude()

  // Indexes:  [0123456789]
  // Existing: [11111  111]
  // Applied:  [     000  ]
  // Expected: [1111100011]

  // Setup of two ranges w/ gap inbetween
  bits.setRange(0, 5, true)
  bits.setRange(7, 10, true)

  // Applying "bridge" range
  bits.setRange(5, 8, false)

  t.is(bits.get(7), false)

  // Add ranges to the end to make the range after the bridge range the midpoint.
  bits.setRange(10, 11, false)
  bits.setRange(11, 12, true)

  t.is(bits.get(7), false, 'bit not updated should stay the same')
})

test('bit-interlude - setRange overlap but next range is same', (t) => {
  const bits = new BitInterlude()

  // Indexes:  [0123456789]
  // Existing: [11111  000]
  // Applied:  [     000  ]
  // Expected: [1111100000]

  // Setup of two ranges w/ gap inbetween
  bits.setRange(0, 5, true)
  bits.setRange(7, 10, false)

  // Applying overlapping range
  bits.setRange(5, 8, false)

  t.is(bits.get(7), false)
  t.alike(bits.ranges, [
    { start: 0, end: 5, value: true },
    { start: 5, end: 10, value: false }
  ])
})
