const test = require('brittle')
const speedometer = require('speedometer')
const byteSize = require('tiny-byte-size')
const { create } = require('./helpers')
const { makeStreamPair } = require('./helpers/networking.js')

test.solo('replication speed', async function (t) {
  const a = await create()
  const b = await create(a.key)

  t.teardown(() => a.close())
  t.teardown(() => b.close())

  await a.append(new Array(15000).fill().map(() => Math.random().toString(16).substr(2)))

  const [n1, n2] = makeStreamPair(t, { latency: [25, 25] }) // Note: stream.rtt will be around twice this value
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
    if (b.replicator.peers.length !== 1) throw new Error('Different number of peers')

    if (Date.now() - started < 500) return
    started = Date.now()

    t.comment('Blocks', '↓ ' + Math.ceil(info.blocks.down()), 'Network', '↓ ' + byteSize(info.network.down()), 'Max inflight', b.replicator.peers[0].maxInflight)
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

function sleep (ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
