const test = require('brittle')
// const DebuggingStream = require('debugging-stream')
const speedometer = require('speedometer')
const byteSize = require('tiny-byte-size')
const UDX = require('udx-native')
const DHT = require('hyperdht')
const createTestnet = require('hyperdht/testnet')
const Hyperswarm = require('hyperswarm')
const Id = require('hypercore-id-encoding')
const NoiseStream = require('@hyperswarm/secret-stream')
const { create } = require('./helpers')
const proxy = require('./helpers/udx-proxy.js')

test.solo('replication speed', async function (t) {
  const a = await create()
  const b = await create(a.key)

  t.teardown(() => a.close())
  t.teardown(() => b.close())

  await a.append(new Array(15000).fill().map(() => Math.random().toString(16).substr(2)))

  const [n1, n2] = await makeStreamPair(t, { latency: [25, 25] }) // Note: stream.rtt will be around twice this value
  a.replicate(n1)
  b.replicate(n2)

  const info = track(b)
  let started = Date.now()

  b.on('download', onchange)
  b.on('upload', onchange)
  b.download()

  await sleep(10000)
  await b.close()
  await a.close()

  function onchange () {
    // if (b.replicator.peers.length !== 1) throw new Error('Different number of peers')

    if (Date.now() - started < 250) return
    started = Date.now()

    console.log('Blocks', '↓ ' + Math.ceil(info.blocks.down()), '↑ ' + Math.ceil(info.blocks.up()), 'Network', '↓ ' + byteSize(info.network.down()), '↑ ' + byteSize(info.network.up()), 'max', b.replicator.peers[0].maxInflight)
  }
})

/* test.skip('replication speed', { timeout: 99999999 }, async function (t) {
  t.plan(1)

  const opts = null // { latency: [250, 300] }
  const dht = new DHT({ debug: { stream: opts } })
  const swarm = new Hyperswarm({ dht })
  t.teardown(() => swarm.destroy())
  process.once('SIGINT', () => t.pass())
  // return

  const b = await create(Id.decode('4pr4zr8nyd1k7e8ntysnu6yrrwpmqyczs3obnx8dbnse3kf4jtzo'))

  const info = track(b)

  const done = b.findingPeers()
  swarm.on('connection', (socket) => b.replicate(socket))
  swarm.join(b.discoveryKey, { server: false, client: true })
  swarm.flush().then(done, done)

  let started = Date.now()
  let cache = ''

  b.on('download', onchange)
  b.on('upload', onchange)
  b.download()

  function onchange () {
    if (Date.now() - started < 250) {
      return
    }

    started = Date.now()

    console.log('Blocks', '↓ ' + Math.ceil(info.blocks.down()), '↑ ' + Math.ceil(info.blocks.up()), 'Network', '↓ ' + byteSize(info.network.down()), '↑ ' + byteSize(info.network.up()))

    const id = 's' + Math.ceil(info.blocks.down.max || 0) + Math.ceil(info.blocks.up.max || 0) + byteSize(info.network.down.max || 0) + byteSize(info.network.up.max || 0)
    if (cache === id) return
    cache = id

    return

    // TODO: auto-detect when it repeats the same values 5 times to finish
    console.log(
      '[max]',
      // b.contiguousLength,
      'blks',
      '↓ ' + Math.ceil(info.blocks.down.max || 0),
      '↑ ' + Math.ceil(info.blocks.up.max || 0),
      'net',
      '↓ ' + byteSize(info.network.down.max || 0),
      '↑ ' + byteSize(info.network.up.max || 0)
    )
  }
}) */

async function makeStreamPair (t, opts = {}) {
  const u = new UDX()

  const a = u.createSocket()
  const b = u.createSocket()

  t.teardown(() => a.close())
  t.teardown(() => b.close())

  a.bind(0)
  b.bind(0)

  const p = await proxy({ from: a, to: b }, async function (pkt) {
    const delay = opts.latency[0] + Math.round(Math.random() * (opts.latency[1] - opts.latency[0]))
    if (delay) await sleep(delay)
    return false
  })

  t.teardown(() => p.close())

  const s1 = u.createStream(1)
  const s2 = u.createStream(2)

  s1.connect(a, 2, p.address().port)
  s2.connect(b, 1, p.address().port)

  t.teardown(() => s1.destroy())
  t.teardown(() => s2.destroy())

  const n1 = new NoiseStream(true, s1)
  const n2 = new NoiseStream(false, s2)

  return [n1, n2]
}

function sleep (ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

// It could be built in Hypercore, at least a simpler version
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
