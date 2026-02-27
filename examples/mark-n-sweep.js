const Hypercore = require('../')

start()

async function start() {
  const core = new Hypercore('./mark-n-sweep')

  await core.append('block0')
  await core.append('block1')
  await core.append('block2')
  await core.append('block3')
  await core.append('block4')

  await core.startMarking()
  await core.get(2)
  await core.get(4)
  await core.sweep()

  console.log('has(0)', await core.has(0)) // Prints "has(0) false"
  console.log('has(4)', await core.has(4)) // Prints "has(4) true"

  await core.close()
}
