const Omega = require('omega')

start()

async function start () {
  const o = new Omega('/tmp/basic')

  await o.ready()
  console.log(o)
  await o.append(['Hello', 'World'])
  await o.close()
}
