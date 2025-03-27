// Generate encryption fixtures

const fs = require('fs')
const path = require('path')
const crypto = require('hypercore-crypto')
const tmpDir = require('test-tmp')

const Hypercore = require('../../../')
const { version } = require('../../../package.json')

main()

async function main () {
  const encryptionKey = Buffer.alloc(32).fill('encryption key')

  const compatKey = crypto.keyPair(Buffer.alloc(32, 0))
  const defaultKey = crypto.keyPair(Buffer.alloc(32, 1))
  const blockKey = crypto.keyPair(Buffer.alloc(32, 2))

  const closing = []

  const compat = new Hypercore(await tmpDir({ teardown }), { keyPair: compatKey, encryptionKey, compat: true })
  const def = new Hypercore(await tmpDir({ teardown }), { keyPair: defaultKey, encryptionKey, isBlockKey: false })
  const block = new Hypercore(await tmpDir({ teardown }), { keyPair: blockKey, encryptionKey, isBlockKey: true })

  await compat.ready()
  await def.ready()
  await block.ready()

  const largeBlock = Buffer.alloc(512)
  for (let i = 0; i < largeBlock.byteLength; i++) largeBlock[i] = i & 0xff

  for (let i = 0; i < 10; i++) {
    await compat.append('compat test: ' + i.toString())
    await def.append('default test: ' + i.toString())
    await block.append('block test: ' + i.toString())
  }

  await compat.append(largeBlock.toString('hex'))
  await def.append(largeBlock.toString('hex'))
  await block.append(largeBlock.toString('hex'))

  const fixture = fs.createWriteStream(path.join(__dirname, `v${version}`))

  fixture.write('/* eslint-disable */\n\n')

  await writeFixture('compat', compat)
  await writeFixture('default', def)
  await writeFixture('block', block)

  fixture.write('/* eslint-enable */\n')

  fixture.end()
  await new Promise(resolve => fixture.on('close', resolve))

  await compat.close()
  await def.close()
  await block.close()

  await shutdown()

  function teardown (fn) {
    closing.push(fn)
  }

  function shutdown () {
    return Promise.all(closing.map(fn => fn()))
  }

  async function writeFixture (name, core) {
    fixture.write(`exports['${name}'] = [\n`)
    for (let i = 0; i < core.length; i++) {
      const b64 = (await core.get(i, { raw: true })).toString('base64')
      fixture.write(`  '${b64}'${(i === core.length - 1) ? '' : ','}\n`)
    }
    fixture.write(']\n\n')
  }
}
