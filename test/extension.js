const test = require('brittle')
const { create, replicate } = require('./helpers')

test('basic extension', async function (t) {
  const messages = ['world', 'hello']

  const a = await create()
  a.registerExtension('test-extension', {
    encoding: 'utf-8',
    onmessage: (message, peer) => {
      t.ok(peer === a.peers[0])
      t.is(message, messages.pop())
    }
  })

  const b = await create(a.key)
  const bExt = b.registerExtension('test-extension', {
    encoding: 'utf-8'
  })

  replicate(a, b)

  await new Promise(resolve => setImmediate(resolve))
  t.is(b.peers.length, 1)

  bExt.send('hello', b.peers[0])
  bExt.send('world', b.peers[0])

  await new Promise(resolve => setImmediate(resolve))
  t.absent(messages.length)

  t.end()
})

test('two extensions', async function (t) {
  const messages = ['world', 'hello']

  const a = await create()
  const b = await create(a.key)

  replicate(a, b)

  b.registerExtension('test-extension-1', {
    encoding: 'utf-8'
  })
  const bExt2 = b.registerExtension('test-extension-2', {
    encoding: 'utf-8'
  })

  await new Promise(resolve => setImmediate(resolve))
  t.is(b.peers.length, 1)

  bExt2.send('world', b.peers[0])

  await new Promise(resolve => setImmediate(resolve))

  a.registerExtension('test-extension-2', {
    encoding: 'utf-8',
    onmessage: (message, peer) => {
      t.ok(peer === a.peers[0])
      t.is(message, messages.pop())
    }
  })

  bExt2.send('hello', b.peers[0])

  await new Promise(resolve => setImmediate(resolve))
  t.is(messages.length, 1) // First message gets ignored

  t.end()
})
