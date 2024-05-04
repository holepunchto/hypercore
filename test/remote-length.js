const test = require('brittle')
const b4a = require('b4a')
const { create, replicate } = require('./helpers')

test('when the writer appends he broadcasts the new contiguous length', async function (t) {
  const a = await create()
  const b = await create(a.key)

  replicate(a, b, t)
  await new Promise(resolve => setTimeout(resolve, 100))

  t.is(getPeer(b, a).remoteContiguousLength, 0, 'Sanity check')

  await a.append('a')
  await new Promise(resolve => setTimeout(resolve, 100))
  t.is(getPeer(b, a).remoteContiguousLength, 1, 'Broadcast new length to other peers')

  await a.append('b')
  await new Promise(resolve => setTimeout(resolve, 100))
  t.is(getPeer(b, a).remoteContiguousLength, 2, 'Broadcast new length to other peers')
})

test('contiguous-length announce-on-update flow', async function (t) {
  const a = await create()
  const b = await create(a.key)
  const c = await create(a.key)

  replicate(a, b, t)
  replicate(b, c, t)

  await a.append('a')
  await new Promise(resolve => setTimeout(resolve, 100))
  t.is(getPeer(c, b).remoteContiguousLength, 0, 'Sanity check: c knows nothing yet')
  t.is(getPeer(b, a).remoteContiguousLength, 1, 'Sanity check: b knows about a')

  await b.get(0)
  await new Promise(resolve => setTimeout(resolve, 100))
  t.is(getPeer(c, b).remoteContiguousLength, 1, 'b broadcast its new contiguous length to the other peers')
  t.is(getPeer(a, b).remoteContiguousLength, 0, 'b did not notify peers he already knows own that block')
})

test('truncates by the writer result in the updated contiguous length being announced', async function (t) {
  const a = await create()
  const b = await create(a.key)

  replicate(a, b, t)
  await new Promise(resolve => setTimeout(resolve, 100))

  t.is(getPeer(b, a).remoteContiguousLength, 0, 'Sanity check')

  await a.append(['a', 'b'])
  await new Promise(resolve => setTimeout(resolve, 100))
  t.is(getPeer(b, a).remoteContiguousLength, 2, 'updated length broadcast to other peers')

  await a.truncate(1)
  await new Promise(resolve => setTimeout(resolve, 100))
  t.is(getPeer(b, a).remoteContiguousLength, 1, 'truncate broadcast to other peers')
})

// Get peer b as seen by peer a (b is the remote peer).
function getPeer (a, b) {
  for (const aPeer of a.replicator.peers) {
    for (const bPeer of b.replicator.peers) {
      if (b4a.equals(aPeer.stream.remotePublicKey, bPeer.stream.publicKey)) return aPeer
    }
  }

  throw new Error('Error in test: peer not found')
}
