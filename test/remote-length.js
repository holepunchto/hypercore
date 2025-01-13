const test = require('brittle')
const b4a = require('b4a')
const RemoteBitfield = require('../lib/remote-bitfield')
const { create, replicate } = require('./helpers')

test('when the writer appends he broadcasts the new contiguous length', async function (t) {
  const a = await create(t)
  const b = await create(t, a.key)

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
  const a = await create(t)
  const b = await create(t, a.key)
  const c = await create(t, a.key)

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

test('announce-range-on-update flow with big core (multiple bitfield pages)', async function (t) {
  t.timeout(1000 * 60 * 5) // Expected to take around 15s. Additional headroom in case of slow CI machine

  const a = await create(t)
  const b = await create(t, a.key)
  const c = await create(t, a.key)

  replicate(a, b, t)
  replicate(b, c, t)

  const nrBlocks = RemoteBitfield.BITS_PER_PAGE + 10

  const blocks = []
  for (let i = 0; i < nrBlocks; i++) {
    blocks.push(`block-${i}`)
  }
  await a.append(blocks)

  await new Promise(resolve => setTimeout(resolve, 500))

  const lastBlock = nrBlocks - 1

  t.is(
    getPeer(c, b)._remoteHasBlock(lastBlock),
    false,
    'Sanity check: c knows nothing yet'
  )
  t.is(
    getPeer(b, a)._remoteHasBlock(lastBlock),
    true,
    'Sanity check: b knows about a'
  )

  await b.get(nrBlocks - 1)
  await new Promise(resolve => setTimeout(resolve, 500))

  t.is(
    getPeer(c, b)._remoteHasBlock(lastBlock),
    true,
    'b broadcast its new block to the other peers')
  t.is(
    getPeer(a, b)._remoteHasBlock(lastBlock),
    false,
    'b did not notify peers he already knows own that block'
  )

  // Some sanity checks on the actual public api

  const getOpts = {
    timeout: 500,
    valueEncoding: 'utf-8'
  }

  // Note: This check is expected to fail if BITS_PER_PAGE changes; just update it then
  t.is(
    await c.get(nrBlocks - 1, getOpts),
    'block-32777',
    'Peer c can get the block peer b also has'
  )

  await t.exception(
    async () => await c.get(nrBlocks - 2, getOpts),
    /REQUEST_TIMEOUT/,
    'Sanity check: peer c can not get blocks peer b does not have')
})

test('truncates by the writer result in the updated contiguous length being announced', async function (t) {
  const a = await create(t)
  const b = await create(t, a.key)

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
  for (const aPeer of a.core.replicator.peers) {
    for (const bPeer of b.core.replicator.peers) {
      if (b4a.equals(aPeer.stream.remotePublicKey, bPeer.stream.publicKey)) return aPeer
    }
  }

  throw new Error('Error in test: peer not found')
}
