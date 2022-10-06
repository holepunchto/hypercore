const test = require('brittle')
const RemoteBitfield = require('../lib/remote-bitfield')

test('remote bitfield - findFirst', function (t) {
  const b = new RemoteBitfield()

  b.set(1000000, true)

  t.is(b.findFirst(true, 0), 1000000)
})
