const test = require('brittle')
const path = require('path')
const RAF = require('random-access-file')
const RAO = require('random-access-memory-overlay')
const Hypercore = require('..')

const abis = [
  'v10.0.0-alpha.39'
]

for (const abi of abis) {
  const root = path.join(__dirname, 'fixtures', 'abi', abi)

  test(abi, async function (t) {
    const core = new Hypercore((file) => new RAO(new RAF(path.join(root, file))))
    await core.ready()

    t.is(core.length, 1000, 'lengths match')
    t.is(core.contiguousLength, 1000, 'contiguous lengths match')

    for (let i = 0; i < 1000; i++) {
      const block = await core.get(i)

      if (!block.equals(Buffer.of(i))) {
        return t.fail(`block ${i} diverges`)
      }
    }

    t.pass('blocks match')
  })
}
