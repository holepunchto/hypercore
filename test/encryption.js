const test = require('brittle')
const RAM = require('random-access-memory')
const Hypercore = require('..')
const { create, createCore, replicate, useTestnet, createStore } = require('./helpers')
const crypto = require('hypercore-crypto')
const DHT = require('@hyperswarm/dht')
const Hyperswarm = require('hyperswarm')
// const HypercoreId = require('hypercore-id-encoding')

const encryptionKey = Buffer.alloc(32, 'hello world')

test.solo('encrypted append and get', async function (t) {
  const { bootstrap } = await useTestnet(t)
  const keyPair = crypto.keyPair()

  // writer
  const a = await createCore(t, { keyPair, encryptionKey })
  await a.append('hello')
  await a.append('hola')

  const [swarm, discovery] = await hyperswarmReplicate(t, a.discoveryKey, a, { bootstrap, keyPair })
  await discovery.flushed()

  // replica (i.e. simple-seeder)
  const store2 = await createStore(t)
  const b = store2.get(a.key)
  await b.ready()

  const [swarm2] = await hyperswarmReplicate(t, b.discoveryKey, store2, { bootstrap, name: 'seeder1' })
  b.download()
  b.on('download', (index) => console.log('seeder1 downloaded block #' + index))

  // basic checks
  t.alike(await a.get(0), Buffer.from('hello')) // unencrypted
  t.absent((await b.get(0)).includes('hello')) // encrypted

  t.absent((await a.core.blocks.get(0)).includes('hello')) // encrypted
  t.absent((await b.core.blocks.get(0)).includes('hello')) // encrypted

  // kill writer
  await swarm.destroy()
  await a.close()

  // re-create writer in different folder
  console.log('re-creating writer')
  const c = await createCore(t, { keyPair, encryptionKey })

  const [swarm3, discovery3, dht3] = await hyperswarmReplicate(t, c.discoveryKey, c, { bootstrap, keyPair })
  await discovery3.flushed()

  await c.get(1) // important
  console.log('has #0?', await c.has(0))
  console.log(await c.core.blocks.get(0), 'zeros but correct length?')
  console.log(await c.get(0), 'got block')
  console.log('has #0?', await c.has(0))
  console.log(await c.core.blocks.get(0), 'filled!')
})

function hyperswarmReplicate (t, discoveryKey, instance, { name, bootstrap, keyPair } = {}) {
  const dht = new DHT({ bootstrap, keyPair })
  const swarm = new Hyperswarm({ dht, keyPair })
  t.teardown(() => swarm.destroy())

  swarm.on('connection', (socket) => {
    if (name) {
      socket.on('error', (err) => console.error('Error (' + name + '):', err.message))
    }

    instance.replicate(socket)
  })

  // + swarm.listen()?
  const discovery = swarm.join(discoveryKey)

  return [swarm, discovery, dht]
}

function waitForSocketFlush (socket) {
  return new Promise((resolve, reject) => {
    socket.on('open', done)
    socket.on('close', done)
    socket.on('error', done)

    function done (error) {
      socket.off('open', done)
      socket.off('close', done)
      socket.off('error', done)

      if (error) reject(error)
      else resolve()
    }
  })
}

test('encrypted append and get', async function (t) {
  const a = await create({ encryptionKey })

  t.alike(a.encryptionKey, encryptionKey)

  await a.append(['hello'])

  const info = await a.info()
  t.is(info.byteLength, 5)
  t.is(a.core.tree.byteLength, 5 + a.padding)

  const unencrypted = await a.get(0)
  t.alike(unencrypted, Buffer.from('hello'))

  const encrypted = await a.core.blocks.get(0)
  t.absent(encrypted.includes('hello'))
})

test('encrypted seek', async function (t) {
  const a = await create({ encryptionKey })

  await a.append(['hello', 'world', '!'])

  t.alike(await a.seek(0), [0, 0])
  t.alike(await a.seek(4), [0, 4])
  t.alike(await a.seek(5), [1, 0])
  t.alike(await a.seek(6), [1, 1])
  t.alike(await a.seek(6), [1, 1])
  t.alike(await a.seek(9), [1, 4])
  t.alike(await a.seek(10), [2, 0])
  t.alike(await a.seek(11), [3, 0])
})

test('encrypted replication', async function (t) {
  const a = await create({ encryptionKey })

  await a.append(['a', 'b', 'c', 'd', 'e'])

  await t.test('with encryption key', async function (t) {
    const b = await create(a.key, { encryptionKey })

    replicate(a, b, t)

    await t.test('through direct download', async function (t) {
      const r = b.download({ start: 0, length: a.length })
      await r.done()

      for (let i = 0; i < 5; i++) {
        t.alike(await b.get(i), await a.get(i))
      }
    })

    await t.test('through indirect download', async function (t) {
      await a.append(['f', 'g', 'h', 'i', 'j'])

      for (let i = 5; i < 10; i++) {
        t.alike(await b.get(i), await a.get(i))
      }

      await a.truncate(5)
    })
  })

  await t.test('without encryption key', async function (t) {
    const b = await create(a.key)

    replicate(a, b, t)

    await t.test('through direct download', async function (t) {
      const r = b.download({ start: 0, length: a.length })
      await r.done()

      for (let i = 0; i < 5; i++) {
        t.alike(await b.get(i), await a.core.blocks.get(i))
      }
    })

    await t.test('through indirect download', async function (t) {
      await a.append(['f', 'g', 'h', 'i', 'j'])

      for (let i = 5; i < 10; i++) {
        t.alike(await b.get(i), await a.core.blocks.get(i))
      }

      await a.truncate(5)
    })
  })
})

test('encrypted session', async function (t) {
  const a = await create({ encryptionKey })

  await a.append(['hello'])

  const s = a.session()

  t.alike(a.encryptionKey, s.encryptionKey)
  t.alike(await s.get(0), Buffer.from('hello'))

  await s.append(['world'])

  const unencrypted = await s.get(1)
  t.alike(unencrypted, Buffer.from('world'))
  t.alike(await a.get(1), unencrypted)

  const encrypted = await s.core.blocks.get(1)
  t.absent(encrypted.includes('world'))
  t.alike(await a.core.blocks.get(1), encrypted)
})

test('encrypted session before ready core', async function (t) {
  const a = new Hypercore(RAM, { encryptionKey })
  const s = a.session()

  await a.ready()

  t.alike(a.encryptionKey, s.encryptionKey)

  await a.append(['hello'])
  t.alike(await s.get(0), Buffer.from('hello'))
})

test('encrypted session on unencrypted core', async function (t) {
  const a = await create()
  const s = a.session({ encryptionKey })

  t.alike(s.encryptionKey, encryptionKey)
  t.unlike(s.encryptionKey, a.encryptionKey)

  await s.append(['hello'])

  const unencrypted = await s.get(0)
  t.alike(unencrypted, Buffer.from('hello'))

  const encrypted = await a.get(0)
  t.absent(encrypted.includes('hello'))
})

test('encrypted session on encrypted core, same key', async function (t) {
  const a = await create({ encryptionKey })
  const s = a.session({ encryptionKey })

  t.alike(s.encryptionKey, a.encryptionKey)

  await s.append(['hello'])

  const unencrypted = await s.get(0)
  t.alike(unencrypted, Buffer.from('hello'))
  t.alike(unencrypted, await a.get(0))
})

test('encrypted session on encrypted core, different keys', async function (t) {
  const a = await create({ encryptionKey: Buffer.alloc(32, 'a') })
  const s = a.session({ encryptionKey: Buffer.alloc(32, 's') })

  t.unlike(s.encryptionKey, a.encryptionKey)

  await s.append(['hello'])

  const unencrypted = await s.get(0)
  t.alike(unencrypted, Buffer.from('hello'))

  const encrypted = await a.get(0)
  t.absent(encrypted.includes('hello'))
})

test('multiple gets to replicated, encrypted block', async function (t) {
  const a = await create({ encryptionKey })
  await a.append('a')

  const b = await create(a.key, { encryptionKey })

  replicate(a, b, t)

  const p = b.get(0)
  const q = b.get(0)

  t.alike(await p, await q)
  t.alike(await p, Buffer.from('a'))
})

test('encrypted core from existing unencrypted core', async function (t) {
  const a = await create({ encryptionKey: Buffer.alloc(32, 'a') })
  const b = await create({ from: a, encryptionKey })

  t.alike(b.key, a.key)
  t.alike(b.encryptionKey, encryptionKey)

  await b.append(['hello'])

  const unencrypted = await b.get(0)
  t.alike(unencrypted, Buffer.from('hello'))
})
