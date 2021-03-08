const Hypercore = require('../')

start()

async function start () {
  const core = new Hypercore('/tmp/basic')

  await core.ready()
  console.log(core)
  await core.append(['Hello', 'World'])
  await core.close()
}
