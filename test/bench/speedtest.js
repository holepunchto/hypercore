const test = require('brittle')
const Hypercore = require('../../index.js')
const RAM = require('random-access-memory')
const { replicate } = require('../helpers')

test('speedtest replication with many peers', { timeout: 120000 }, async function (t) {
  const core = new Hypercore(RAM)
  await core.ready()

  const clone1 = new Hypercore(RAM, core.key)
  const clone2 = new Hypercore(RAM, core.key)
  const clone3 = new Hypercore(RAM, core.key)

  for (let i = 0; i < 100000; i++) {
    await core.append('#' + i)
    if (i % 10000 === 0) t.comment('Append ' + i)
  }

  t.comment('Writer complete')

  replicate(core, clone1, t)
  replicate(core, clone2, t)
  replicate(core, clone3, t)
  replicate(clone1, clone2, t)
  replicate(clone1, clone3, t)

  await new Promise(resolve => setTimeout(resolve, 500))
  t.comment(core)

  let count = 0
  clone1.on('download', function () {
    if (++count % 10000 === 0) t.comment(count)
  })
  clone2.on('download', function () {
    if (++count % 10000 === 0) t.comment(count)
  })

  clone2.download({ start: 0, end: core.length })

  const d = clone1.download({ start: 0, end: core.length })

  const started = Date.now()
  await d.done()
  t.comment('Done in ' + (Date.now() - started) + ' ms')
})
