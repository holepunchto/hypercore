const test = require('brittle')
const speedometer = require('speedometer')
const byteSize = require('tiny-byte-size')
const { create } = require('./helpers')
const { makeStreamPair } = require('./helpers/networking.js')

test('replication speed - localhost', async function (t) {
  const a = await create()
  const b = await create(a.key)

  await a.append(new Array(15000).fill().map(() => Math.random().toString(16).substr(2)))

  const [n1, n2] = makeStreamPair(t, { latency: [0, 0] }) // Note: stream.rtt will be around doubl this value
  a.replicate(n1)
  b.replicate(n2)

  const info = track(b)
  let started = Date.now()

  t.comment('Starting to download')
  b.on('download', onchange)
  b.on('upload', onchange)
  b.download()

  await sleep(5000)
  await b.close()
  await a.close()

  function onchange () {
    if (b.replicator.peers.length !== 1) throw new Error('Different number of peers')

    if (Date.now() - started < 500) return
    started = Date.now()

    const peer = b.replicator.peers[0]
    t.comment('Blocks', '↓ ' + Math.ceil(info.blocks.down()), 'Network', '↓ ' + byteSize(info.network.down()), 'RTT', peer.stream.rawStream.rtt, 'Max inflight', peer.getMaxInflight())
  }
})

test('replication speed - nearby', async function (t) {
  const a = await create()
  const b = await create(a.key)

  await a.append(new Array(15000).fill().map(() => Math.random().toString(16).substr(2)))

  const [n1, n2] = makeStreamPair(t, { latency: [25, 25] }) // Note: stream.rtt will be around doubl this value
  a.replicate(n1)
  b.replicate(n2)

  const info = track(b)
  let started = Date.now()

  t.comment('Starting to download')
  b.on('download', onchange)
  b.on('upload', onchange)
  b.download()

  await sleep(10000)
  await b.close()
  await a.close()

  function onchange () {
    if (b.replicator.peers.length !== 1) throw new Error('Different number of peers')

    if (Date.now() - started < 500) return
    started = Date.now()

    const peer = b.replicator.peers[0]
    t.comment('Blocks', '↓ ' + Math.ceil(info.blocks.down()), 'Network', '↓ ' + byteSize(info.network.down()), 'RTT', peer.stream.rawStream.rtt, 'Max inflight', peer.getMaxInflight())
  }
})

test('replication speed - different country', async function (t) {
  const a = await create()
  const b = await create(a.key)

  await a.append(new Array(15000).fill().map(() => Math.random().toString(16).substr(2)))

  const [n1, n2] = makeStreamPair(t, { latency: [75, 75] }) // Note: stream.rtt will be around doubl this value
  a.replicate(n1)
  b.replicate(n2)

  const info = track(b)
  let started = Date.now()

  t.comment('Starting to download')
  b.on('download', onchange)
  b.on('upload', onchange)
  b.download()

  await sleep(10000)
  await b.close()
  await a.close()

  function onchange () {
    if (b.replicator.peers.length !== 1) throw new Error('Different number of peers')

    if (Date.now() - started < 500) return
    started = Date.now()

    const peer = b.replicator.peers[0]
    t.comment('Blocks', '↓ ' + Math.ceil(info.blocks.down()), 'Network', '↓ ' + byteSize(info.network.down()), 'RTT', peer.stream.rawStream.rtt, 'Max inflight', peer.getMaxInflight())
  }
})

test('replication speed - far away', async function (t) {
  const a = await create()
  const b = await create(a.key)

  await a.append(new Array(15000).fill().map(() => Math.random().toString(16).substr(2)))

  const [n1, n2] = makeStreamPair(t, { latency: [250, 250] }) // Note: stream.rtt will be around doubl this value
  a.replicate(n1)
  b.replicate(n2)

  const info = track(b)
  let started = Date.now()

  t.comment('Starting to download')
  b.on('download', onchange)
  b.on('upload', onchange)
  b.download()

  await sleep(10000)
  await b.close()
  await a.close()

  function onchange () {
    if (b.replicator.peers.length !== 1) throw new Error('Different number of peers')

    if (Date.now() - started < 500) return
    started = Date.now()

    const peer = b.replicator.peers[0]
    t.comment('Blocks', '↓ ' + Math.ceil(info.blocks.down()), 'Network', '↓ ' + byteSize(info.network.down()), 'RTT', peer.stream.rawStream.rtt, 'Max inflight', peer.getMaxInflight())
  }
})

function track (core) {
  const info = {
    blocks: { down: speedometer(), up: speedometer() },
    network: { down: speedometer(), up: speedometer() }
  }

  core.on('download', onspeed.bind(null, 'down', info))
  core.on('upload', onspeed.bind(null, 'up', info))

  return info
}

function onspeed (eventName, info, index, byteLength, from) {
  const block = info.blocks[eventName]
  const network = info.network[eventName]

  const blocks = block(1)
  const networks = network(byteLength)

  if (block.max === undefined || blocks > block.max) block.max = blocks
  if (network.max === undefined || networks > network.max) network.max = networks
}

function sleep (ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
