const Omega = require('../')

const core = new Omega('./clone')

start()

async function start () {
  core.ready = (cb) => cb() // need to support promisified ready in replicator
  require('@hyperswarm/replicator')(core, {
    discoveryKey: Buffer.from('aa13976f5edc84ee157071e8acde51438039da67b999d2682318e9f2369db59b', 'hex'),
    lookup: true
  })

  console.log((await core.get(42)).toString())
  console.log((await core.get(142)).toString())
  console.log((await core.get(511)).toString())
  console.log((await core.get(512)).toString())
  console.log((await core.get(513)).toString())
}
