const test = require('brittle')
const ReceiverQueue = require('../lib/receiver-queue')

test('receiver queue - empty request', function (t) {
  const queue = new ReceiverQueue()
  const req = {
    id: 1,
    block: null,
    hash: null,
    seek: null,
    upgrade: null,
    manifest: false,
    priority: 0
  }

  queue.push(req)

  t.is(queue.shift(), req)
})
