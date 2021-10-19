const Hypercore = require('../')

start()

async function start () {
  const core = new Hypercore('/tmp/basic')
  await core.append(['Hello', 'World'])
  console.log(core)
  await core.close()
}
