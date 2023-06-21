const test = require('brittle')
// const DebuggingStream = require('debugging-stream')
const speedometer = require('speedometer')
const byteSize = require('tiny-byte-size')
// const UDX = require('udx-native')
const DHT = require('hyperdht')
// const createTestnet = require('hyperdht/testnet')
const Hyperswarm = require('hyperswarm')
const Id = require('hypercore-id-encoding')
const { create } = require('./helpers')

test.solo('replication speed', { timeout: 99999999 }, async function (t) {
  t.plan(1)

  const opts = null // { latency: [250, 300] }
  const dht = new DHT({ debug: { stream: opts } })
  const swarm = new Hyperswarm({ dht })
  t.teardown(() => swarm.destroy())
  process.once('SIGINT', () => t.pass())

  /* const a = await create()
  console.log('Core key', a.id)
  await a.append(new Array(100000).fill().map(() => Math.random().toString(16).substr(2)))
  swarm.on('connection', (socket) => a.replicate(socket))
  const discovery = swarm.join(a.discoveryKey)
  await discovery.flushed()
  return */

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
    if (Date.now() - started < 1000) {
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
})

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
