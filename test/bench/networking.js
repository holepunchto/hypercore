const test = require('brittle')
const speedometer = require('speedometer')
const byteSize = require('tiny-byte-size')
const b4a = require('b4a')
const { create } = require('../helpers')
const { makeStreamPair } = require('../helpers/networking.js')

async function setup(t, opts = {}) {
  t.timeout(60 * 1000)

  const a = await create()
  const b = await create(a.key)

  await a.append(
    new Array(opts.append).fill().map(() => b4a.alloc(16).fill('a'))
  )

  // Note: stream.rtt will be around double this latency value
  const [n1, n2] = makeStreamPair(t, { latency: opts.latency })
  a.replicate(n1)
  b.replicate(n2)

  const info = track(b)
  let started = Date.now()

  t.comment('Starting to download')
  b.on('download', onchange)
  b.on('upload', onchange)
  await b.download({ start: 0, end: a.length }).done()

  return [a, b]

  function onchange() {
    if (b.replicator.peers.length !== 1)
      throw new Error('Different number of peers')

    if (Date.now() - started < 500) return
    started = Date.now()

    const peer = b.replicator.peers[0]
    const rtt = peer.stream.rawStream.rtt
    t.comment(
      'Blocks',
      Math.ceil(info.blocks.down()),
      '(' + byteSize(info.network.down()) + ' bytes)',
      'RTT',
      rtt,
      'Max inflight',
      peer.getMaxInflight()
    )
  }
}

test('replication speed - localhost', async function (t) {
  await setup(t, { append: 5000, latency: [0, 0] })
})

test('replication speed - nearby', async function (t) {
  await setup(t, { append: 5000, latency: [25, 25] })
})

test('replication speed - different country', async function (t) {
  await setup(t, { append: 5000, latency: [75, 75] })
})

test('replication speed - far away', async function (t) {
  await setup(t, { append: 5000, latency: [250, 250] })
})

test('replication speed - orbit', async function (t) {
  await setup(t, { append: 5000, latency: [500, 500] })
})

function track(core) {
  const info = {
    blocks: { down: speedometer(), up: speedometer() },
    network: { down: speedometer(), up: speedometer() }
  }

  core.on('download', onspeed.bind(null, 'down', info))
  core.on('upload', onspeed.bind(null, 'up', info))

  return info
}

function onspeed(eventName, info, index, byteLength, from) {
  const block = info.blocks[eventName]
  const network = info.network[eventName]

  const blocks = block(1)
  const networks = network(byteLength)

  if (block.max === undefined || blocks > block.max) block.max = blocks
  if (network.max === undefined || networks > network.max)
    network.max = networks
}
