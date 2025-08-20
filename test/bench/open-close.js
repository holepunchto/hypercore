const test = require('brittle')
const b4a = require('b4a')
const Hypercore = require('../../index.js')

test('open and close', async function (t) {
  const tmpDir = await t.tmp()

  const core = new Hypercore(tmpDir)
  for (let i = 0; i < 100; i++) {
    await core.append(b4a.from([0]))
  }
  await core.close()

  const elapsed = await t.execution(async function () {
    for (let i = 0; i < 100; i++) {
      const core = new Hypercore(tmpDir)
      await core.ready()
      await core.close()
    }
  })

  t.comment(elapsed)
})
