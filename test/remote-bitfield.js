const test = require('brittle')
const b4a = require('b4a')
const RemoteBitfield = require('../lib/remote-bitfield')
const { create, replicate } = require('./helpers')

test('remote bitfield - findFirst', function (t) {
  const b = new RemoteBitfield()

  b.set(1000000, true)

  t.is(b.findFirst(true, 0), 1000000)
})

test('remote bitfield - set range on page boundary', function (t) {
  const b = new RemoteBitfield()

  b.setRange(2032, 2058, true)

  t.is(b.findFirst(true, 2048), 2048)
})

test('remote bitfield - set range to false', function (t) {
  const b = new RemoteBitfield()

  b.setRange(0, 5000, false)

  t.is(b.findFirst(true, 0), -1)
})

test('set last bits in segment and findFirst', function (t) {
  const b = new RemoteBitfield()

  b.set(32766, true)
  t.is(b.findFirst(false, 32766), 32767)

  b.set(32767, true)
  t.is(b.findFirst(false, 32766), 32768)
  t.is(b.findFirst(false, 32767), 32768)
})

test('remote congituous length consistency (remote-bitfield findFirst edge case)', async function (t) {
  // Indirectly tests the findFirst method for the case where
  // a position > 0 is passed in, while _maxSegments is still 0
  // because nothing was set.
  const a = await create(t)
  const b = await create(t, a.key)
  const c = await create(t, a.key)

  replicate(a, b, t)
  replicate(b, c, t)

  await a.append('block0')
  await a.append('block1')

  await b.get(0)
  await new Promise(resolve => setTimeout(resolve, 500))

  const peer = getPeer(c, b)

  t.is(peer._remoteContiguousLength, 1, 'Sanity check')

  t.is(
    peer._remoteContiguousLength <= peer.remoteContiguousLength,
    true,
    'invariant holds: remoteContiguousLength at least _remoteContiguousLength'
  )
})

test('bitfield messages sent on cache miss', async function (t) {
  const original = await create(t)
  const sparse = await create(t, original.key)
  const empty = await create(t, original.key)

  await original.append(['a', 'b', 'c', 'd', 'e'])

  replicate(original, sparse, t)
  await original.get(2)
  await original.get(3)

  replicate(sparse, empty, t)
  await new Promise(resolve => setTimeout(resolve, 1000))

  t.is(empty.replicator.peers.length, 1, 'Sanity check')
  const stats = empty.replicator.peers[0].stats
  t.is(stats.wireBitfield.rx, 0, 'initially no bitfields sent (sanity check')

  await t.exception(
    async () => {
      await empty.get(1, { timeout: 100 })
    },
    /REQUEST_TIMEOUT/,
    'request on unavailable block times out (sanity check)'
  )
  t.is(stats.wireBitfield.rx, 1, 'Requests bitfield on cache miss')
})

// Peer b as seen by peer a (b is the remote peer)
function getPeer (a, b) {
  for (const aPeer of a.core.replicator.peers) {
    for (const bPeer of b.core.replicator.peers) {
      if (b4a.equals(aPeer.stream.remotePublicKey, bPeer.stream.publicKey)) {
        return aPeer
      }
    }
  }

  throw new Error('Error in test: peer not found')
}
