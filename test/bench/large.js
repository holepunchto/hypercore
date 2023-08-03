const test = require('brittle')
const Hypercore = require('../../index.js')
const RAM = require('random-access-memory')
const { replicate, createTmpDir } = require('../helpers')
const speedometer = require('speedometer')
const byteSize = require('byte-size')

test('large core with two non-sparse readers', { timeout: 999999999 }, async function (t) {
  const dir = createTmpDir(t)

  const writer = new Hypercore(dir)
  await writer.ready()

  const clone1 = new Hypercore(RAM, writer.key)
  const clone2 = new Hypercore(RAM, writer.key)

  for (let i = writer.length; i < 1000000; i++) {
    await writer.append(Math.random().toString(16).substr(2))

    if (i % 100000 === 0) t.comment('Append ' + i)
  }

  t.comment('Writer complete')

  intervalSpeed(t, writer, 'Writer')
  intervalSpeed(t, clone1, 'Clone1')
  intervalSpeed(t, clone2, 'Clone2')

  replicate(writer, clone1, t)
  replicate(writer, clone2, t)

  const dl1 = clone1.download()
  const dl2 = clone2.download()

  await dl1.done()
  await dl2.done()

  t.comment('Done')

  await writer.close()
  await clone1.close()
  await clone2.close()
})

function intervalSpeed (t, core, name) {
  const info = {
    blocks: { down: speedometer(), up: speedometer() },
    network: { down: speedometer(), up: speedometer() }
  }

  core.on('download', onspeed.bind(null, 'down', info))
  core.on('upload', onspeed.bind(null, 'up', info))

  const id = setInterval(() => {
    t.comment(
      name,
      '↓ ' + Math.ceil(info.blocks.down()),
      '↑ ' + Math.ceil(info.blocks.up()) + ' blks/s',
      '↓ ' + byteSize(info.network.down()),
      '↑ ' + byteSize(info.network.up())
    )
  }, 1000)

  t.teardown(() => clearInterval(id))
}

function onspeed (eventName, info, index, byteLength, from) {
  info.blocks[eventName](1)
  info.network[eventName](byteLength)
}
