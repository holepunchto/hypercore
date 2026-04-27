const test = require('brittle')
test('simple test', async function (t) {
  await new Promise((resolve) => setTimeout(resolve, 1000))
  t.pass('simple test passed')
})
