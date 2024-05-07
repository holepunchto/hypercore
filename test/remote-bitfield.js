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

test('remote congituous length consistency (remote-bitfield findFirst edge case)', async function (t) {
  // Indirectly tests the findFirst method for the case where
  // a position > 0 is passed in, while _maxSegments is still 0
  // because nothing was set.
  const a = await create()
  const b = await create(a.key)
  const c = await create(a.key)

  replicate(a, b, t)
  replicate(b, c, t)

  await a.append('block0')
  await a.append('block1')

  await b.get(0)
  await new Promise(resolve => setTimeout(resolve, 500))

  const peer = getPeer(c, b)
  t.is(
    peer._remoteContiguousLength <= peer.remoteContiguousLength,
    true,
    'invariant holds: remoteContiguousLength at least _remoteContiguousLength'
  )
})

// Peer b as seen by peer a (b is the remote peer)
function getPeer (a, b) {
  for (const aPeer of a.replicator.peers) {
    for (const bPeer of b.replicator.peers) {
      if (b4a.equals(aPeer.stream.remotePublicKey, bPeer.stream.publicKey)) {
        return aPeer
      }
    }
  }

  throw new Error('Error in test: peer not found')
}
