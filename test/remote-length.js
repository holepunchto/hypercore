const test = require('brittle')
const { create, replicate } = require('./helpers')

test('basic peer remote contiguous length', async function (t) {
  t.plan(2)

  const a = await create()
  const b = await create(a.key)

  replicate(a, b, t)

  await a.append('a')
  await b.get(0)
  await new Promise(resolve => setTimeout(resolve, 100))
  t.is(getPeer(a, b).remoteContiguousLength, 1)

  await a.append('b')
  await b.get(1)
  await new Promise(resolve => setTimeout(resolve, 100))
  t.is(getPeer(a, b).remoteContiguousLength, 2)
})

test('peer remote contiguous length', async function (t) {
  t.plan(3)

  const a = await create()
  const b = await create(a.key)

  replicate(a, b, t)

  await a.append('a')
  await b.get(0)
  await new Promise(resolve => setTimeout(resolve, 100))
  t.is(getPeer(a, b).remoteContiguousLength, 1)

  await a.append(['d', 'e'])
  await b.get(2)
  await new Promise(resolve => setTimeout(resolve, 100))
  t.is(getPeer(a, b).remoteContiguousLength, 1)

  await b.get(1)
  await new Promise(resolve => setTimeout(resolve, 100))
  t.is(getPeer(a, b).remoteContiguousLength, 3)
})

test('peer truncates the remote contiguous length', async function (t) {
  t.plan(2)

  const a = await create()
  const b = await create(a.key)

  replicate(a, b, t)

  await a.append(['a', 'b'])
  await b.get(0)
  await b.get(1)
  await new Promise(resolve => setTimeout(resolve, 100))
  t.is(getPeer(a, b).remoteContiguousLength, 2)

  await a.truncate(1)
  await new Promise(resolve => setTimeout(resolve, 100))
  t.is(getPeer(a, b).remoteContiguousLength, 1)
})

// "A" wants to know if "B" is finished syncing, so find the corresponding peer
function getPeer (a, b) {
  return a.replicator.peers.find(peer => peer.remotePublicKey.equals(b.replicator.peers[0].stream.publicKey))
}
