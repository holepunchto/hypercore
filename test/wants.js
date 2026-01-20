const test = require('brittle')
const { LocalWants, RemoteWants, WANT_BATCH } = require('../lib/wants.js')

const MAX = 512

test('local want - .add() returns obj', (t) => {
  const peer = {}
  const localWant = new LocalWants(peer)
  const handler = { addWant: () => t.pass('called addWant') }

  const index = WANT_BATCH * 2 - 10 // anything batch index = 1
  const result = localWant.add(index, handler)

  t.not(result, null, 'returns obj')
  t.is(
    result.want.start,
    Math.floor(index / WANT_BATCH) * WANT_BATCH,
    'returns index of start of batch'
  )
  t.is(result.want.length, WANT_BATCH, 'sets length to batch length')
  t.absent(result.want.any, 'any = false')
  t.is(localWant.wants.size, 1, 'want was added to internal wants')
})

test('local want - calls handles addWant when new', (t) => {
  t.plan(8)

  const peer = {}
  const localWant = new LocalWants(peer)
  const handler = { addWant: () => t.pass('called addWant') }

  const index = WANT_BATCH * 2 - 10 // anything batch index = 1
  const result = localWant.add(index, handler)

  t.not(result, null, 'returns obj')
  t.is(localWant.wants.size, 1, 'want was added to internal wants')

  t.is(localWant.add(index, handler), null, 'noop to readd handler')
  t.is(localWant.wants.get(1).handles.size, 1, 'only 1 handler still')

  t.comment('add a new handler')
  const handler2 = { addWant: () => t.pass('called addWant') }
  t.is(localWant.add(index, handler2), null, 'reuses want')
  t.is(localWant.wants.get(1).handles.size, 2, 'added handler')
})

test('local want - remove()', (t) => {
  t.plan(13) // 5 handler calls + 8 normal asserts

  const peer = {}
  const localWant = new LocalWants(peer)
  const index = WANT_BATCH * 2 - 10 // anything batch index = 1
  const handler = makeHandler(t)
  const result = localWant.add(index, handler)
  t.is(localWant.wants.size, 1, 'want was added to internal wants')

  t.absent(localWant.remove(index, handler), 'removing returns false')
  t.is(localWant.wants.size, 0, 'no wants')
  t.is(localWant.free.size, 1, 'added to free')

  localWant.add(index, makeHandler(t))
  const secondHandler = makeHandler(t)
  localWant.add(index, secondHandler)
  t.is(localWant.wants.size, 1, '1 want 2 handlers')
  t.is(localWant.free.size, 0, '0 frees')

  t.absent(localWant.remove(index, secondHandler), 'removing 2nd handler returns false')
  t.is(localWant.free.size, 0, 'batch isnt freed')
})

test('local want - remove() signals MAX & next add clear free', (t) => {
  const peer = {}
  const localWant = new LocalWants(peer)

  const handlers = []
  for (let i = 0; i < MAX; i++) {
    localWant.add(WANT_BATCH * i, makeHandler({ pass: () => {} }, handlers))
  }

  t.is(localWant.wants.size, MAX, 'maxed out')
  t.is(localWant.free.size, 0, 'free is still empty')

  t.ok(localWant.remove(0, handlers[0]), 'removing when MAXed returns true')
  t.absent(localWant.remove(WANT_BATCH * 1, handlers[1]), 'removing again returns false')
  t.is(localWant.free.size, 2, 'free has entry')

  const nextAddResult = localWant.add(WANT_BATCH * MAX, makeHandler(t, handlers))
  t.alike(nextAddResult.unwant, { start: 0, length: WANT_BATCH, any: false }, 'returns unwant info')
  t.is(localWant.free.size, 1, '1 deleted from free')
})

test('local want - destroy()', (t) => {
  t.plan(5)

  const peer = {}
  const localWant = new LocalWants(peer)
  const handlers = []

  const index = WANT_BATCH * 2 - 10 // anything batch index = 1
  localWant.add(index, makeHandler(t, handlers))

  localWant.destroy()
  t.ok(localWant.destroyed, 'marked as destroyed')
  t.is(localWant.wants.size, 0, 'clears wants map')
  t.is(localWant.add(index, makeHandler(t, handlers)), null, 'add returns null when destroyed')
})

test('remote want - add()', (t) => {
  const remoteWants = new RemoteWants()

  // Add but no batch
  t.ok(remoteWants.add({ start: 0, length: 1, any: false }), 'adds range (0, 1)')
  t.is(remoteWants.batches.length, 0, 'no batch because < MIN_RANGE')
  t.ok(remoteWants.add({ start: 0, length: 33, any: false }), 'adds range (0, 33)')
  t.is(remoteWants.batches.length, 0, 'no batch because length not power of 2')
  t.ok(remoteWants.add({ start: 70, length: 64, any: false }), 'adds range (5, 64)')
  t.is(remoteWants.batches.length, 0, 'no batch because start isnt multiple of length')

  t.ok(remoteWants.add({ start: 0, length: 64, any: false }), 'adds range (0, 64)')
  t.is(remoteWants.batches.length, 1, 'adds batch')
  t.is(remoteWants.size, 1, 'inc size')

  t.ok(remoteWants.add({ start: 128, length: 64, any: false }), 'adds range (128, 64)')
  t.is(remoteWants.batches.length, 1, 'reuse batch')
  t.alike(remoteWants.batches[0].ranges, new Set([0, 2]), 'updates batch')
  t.is(remoteWants.size, 2, 'inc size')

  t.absent(
    remoteWants.add({ start: 0, length: 2 * 1024 * 1024 + 1, any: false }),
    'exceeding MAX_RANGE will fail'
  )
})

test('remote want - remove()', (t) => {
  const remoteWants = new RemoteWants()

  t.absent(remoteWants.remove({ start: 0, length: 1, any: false }), 'fails when empty')

  t.comment('Setup')
  remoteWants.add({ start: 0, length: 64, any: false })
  remoteWants.add({ start: 64, length: 64, any: false })
  t.is(remoteWants.batches.length, 1, 'batches length')
  t.is(remoteWants.size, 2, 'size')

  t.comment('Validation fails')
  t.absent(remoteWants.remove({ start: 0, length: 1, any: false }), 'fails because < MIN_RANGE')
  t.absent(
    remoteWants.remove({ start: 0, length: 33, any: false }),
    'fails because length not power of 2'
  )
  t.absent(
    remoteWants.remove({ start: 5, length: 33, any: false }),
    'fails because start not multiple of length'
  )
  t.absent(remoteWants.remove({ start: 0, length: 128, any: false }), 'fails if range doesnt match')

  t.comment('Success')
  t.ok(remoteWants.remove({ start: 0, length: 64, any: false }), 'remove return true when it works')
  t.is(remoteWants.size, 1, 'size')
  t.is(remoteWants.batches.length, 1, 'batch not removed')
  t.alike(remoteWants.batches[0].ranges, new Set([1]), 'range removed')

  t.ok(remoteWants.remove({ start: 64, length: 64, any: false }), 'removing 2nd range of length 64')
  t.is(remoteWants.size, 0, 'size')
  t.is(remoteWants.batches.length, 0, 'batch fully removed')
})

test('remote want - hasRange()', (t) => {
  const remoteWants = new RemoteWants()

  t.absent(remoteWants.hasRange(0, 1000), 'starts empty')

  remoteWants.add({ start: 0, length: 32, any: false })
  remoteWants.add({ start: 64, length: 64, any: false })
  remoteWants.add({ start: 128, length: 128, any: false })
  remoteWants.add({ start: 256, length: 256, any: false })

  t.absent(remoteWants.all, 'not set to all')
  t.ok(remoteWants.hasRange(64, 1), 'uses .has() for length 1')
  t.absent(remoteWants.hasRange(1024, 1), 'uses .has() for length 1 w/ miss')
  t.ok(remoteWants.hasRange(1024, 512), 'hits max checks')
})

function makeHandler(t, handlers = []) {
  const handler = {
    addWant: () => t.pass('called addWant'),
    removeWant: () => t.pass('called removeWant')
  }
  handlers.push(handler)

  return handler
}
