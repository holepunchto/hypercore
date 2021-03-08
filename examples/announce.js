const Hypercore = require('../')

const core = new Hypercore('./source')

start()

async function start () {
  await core.ready()
  while (core.length < 1000) {
    await core.append('block #' + core.length)
  }

  core.ready = (cb) => cb() // need to support promisified ready in replicator
  require('@hyperswarm/replicator')(core, {
    discoveryKey: Buffer.from('aa13976f5edc84ee157071e8acde51438039da67b999d2682318e9f2369db59b', 'hex'),
    announce: true,
    lookup: false
  })

  console.log('?')
}
