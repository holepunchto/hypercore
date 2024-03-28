const test = require('brittle')
const RemoteBitfield = require('../lib/remote-bitfield')

test('remote bitfield - findFirst', function (t) {
  const b = new RemoteBitfield()

  b.set(1000000, true)

  t.is(b.findFirst(true, 0), 1000000)
})

test('remote bitfield - set range on page boundary', function (t) {
  const b = new RemoteBitfield()

  b.setRange(2032, 26, true)

  t.is(b.findFirst(true, 2048), 2048)
})

test('set last bits in segment and findFirst', function (t) {
  const b = new RemoteBitfield()

  b.set(32766, true)
  t.is(b.findFirst(false, 32766), 32767)

  b.set(32767, true)
  t.is(b.findFirst(false, 32766), 32768)
  t.is(b.findFirst(false, 32767), 32768)
})
