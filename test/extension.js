const tape = require('tape')
const { create, replicate } = require('./helpers')

tape('basic extension', async function (t) {
  const messages = ['world', 'hello']

  const a = await create()
  a.registerExtension('test-extension', {
    encoding: 'utf-8',
    onmessage: (message, peer) => {
      t.ok(peer === a.peers[0])
      t.same(message, messages.pop())
    }
  })

  const b = await create(a.key)
  const bExt = b.registerExtension('test-extension', {
    encoding: 'utf-8'
  })

  replicate(a, b)

  await new Promise(resolve => setImmediate(resolve))
  t.same(b.peers.length, 1)

  bExt.send('hello', b.peers[0])
  bExt.send('world', b.peers[0])

  await new Promise(resolve => setImmediate(resolve))
  t.false(messages.length)

  t.end()
})
