const Hypercore = require('../')
const Hyperswarm = require('hyperswarm')

const core = new Hypercore('./source')

start()

async function start () {
  await core.ready()
  while (core.length < 1000) {
    await core.append('block #' + core.length)
  }

  const swarm = new Hyperswarm()
  swarm.on('connection', socket => core.replicate(socket))
  swarm.join(core.discoveryKey, { server: true, client: false })

  console.log('Core:', core.key.toString('hex'))
}
