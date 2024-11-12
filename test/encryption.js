const test = require('brittle')
const b4a = require('b4a')
const Hypercore = require('..')
const { create, createStorage, replicate } = require('./helpers')

const encryptionKey = b4a.alloc(32, 'hello world')

test('encrypted append and get', async function (t) {
  const a = await create(t, { encryptionKey })

  t.alike(a.encryptionKey, encryptionKey)

  await a.append(['hello'])

  const info = await a.info()
  t.is(info.byteLength, 5)
  t.is(a.core.tree.byteLength, 5 + a.padding)

  const unencrypted = await a.get(0)
  t.alike(unencrypted, b4a.from('hello'))

  const encrypted = await getBlock(a, 0)
  t.absent(encrypted.includes('hello'))
})

test('get with decrypt option', async function (t) {
  const a = await create(t, { encryptionKey })

  await a.append('hello')

  const unencrypted = await a.get(0, { decrypt: true })
  t.alike(unencrypted, b4a.from('hello'))

  const encrypted = await a.get(0, { decrypt: false })
  t.absent(encrypted.includes('hello'))
})

test('encrypted seek', async function (t) {
  const a = await create(t, { encryptionKey })

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
  const a = await create(t, { encryptionKey })

  await a.append(['a', 'b', 'c', 'd', 'e'])

  await t.test('with encryption key', async function (t) {
    const b = await create(t, a.key, { encryptionKey })

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
    const b = await create(t, a.key)

    replicate(a, b, t)

    await t.test('through direct download', async function (t) {
      const r = b.download({ start: 0, length: a.length })
      await r.done()

      for (let i = 0; i < 5; i++) {
        t.alike(await b.get(i), await getBlock(a, i))
      }
    })

    await t.test('through indirect download', async function (t) {
      await a.append(['f', 'g', 'h', 'i', 'j'])

      for (let i = 5; i < 10; i++) {
        t.alike(await b.get(i), await getBlock(a, i))
      }

      await a.truncate(5)
    })
  })
})

test('encrypted session', async function (t) {
  const a = await create(t, { encryptionKey })

  await a.append(['hello'])

  const s = a.session()

  t.alike(a.encryptionKey, s.encryptionKey)
  t.alike(await s.get(0), b4a.from('hello'))

  await s.append(['world'])

  const unencrypted = await s.get(1)
  t.alike(unencrypted, b4a.from('world'))
  t.alike(await a.get(1), unencrypted)

  const encrypted = await getBlock(s, 1)
  t.absent(encrypted.includes('world'))
  t.alike(await getBlock(a, 1), encrypted)

  await s.close()
})

test('encrypted session before ready core', async function (t) {
  const storage = await createStorage(t)

  const a = new Hypercore(storage, { encryptionKey })
  const s = a.session()

  await a.ready()

  t.alike(a.encryptionKey, s.encryptionKey)

  await a.append(['hello'])
  t.alike(await s.get(0), b4a.from('hello'))

  await s.close()
  await a.close()
})

test('encrypted session on unencrypted core', async function (t) {
  const a = await create(t)

  const s = a.session({ encryptionKey, debug: 'debug' })

  t.alike(s.encryptionKey, encryptionKey)
  t.unlike(s.encryptionKey, a.encryptionKey)

  await s.append(['hello'])

  const unencrypted = await s.get(0)
  t.alike(unencrypted, b4a.from('hello'))

  const encrypted = await a.get(0)
  t.absent(encrypted.includes('hello'))

  await s.close()
})

test('encrypted session on encrypted core, same key', async function (t) {
  const a = await create(t, { encryptionKey })
  const s = a.session({ encryptionKey })

  t.alike(s.encryptionKey, a.encryptionKey)

  await s.append(['hello'])

  const unencrypted = await s.get(0)
  t.alike(unencrypted, b4a.from('hello'))
  t.alike(unencrypted, await a.get(0))

  await s.close()
})

test('multiple gets to replicated, encrypted block', async function (t) {
  const a = await create(t, { encryptionKey })
  await a.append('a')

  const b = await create(t, a.key, { encryptionKey })

  replicate(a, b, t)

  const p = b.get(0)
  const q = b.get(0)

  t.alike(await p, await q)
  t.alike(await p, b4a.from('a'))
})

test('encrypted core from existing unencrypted core', async function (t) {
  const a = await create(t, { encryptionKey: null })
  const b = new Hypercore({ core: a.core, encryptionKey })

  t.alike(b.key, a.key)
  t.alike(b.encryptionKey, encryptionKey)

  await b.append(['hello'])

  const unencrypted = await b.get(0)
  t.alike(unencrypted, b4a.from('hello'))

  await b.close()
})

test('from session sessions pass encryption', async function (t) {
  const storage = await createStorage(t)

  const a = new Hypercore(storage)
  const b = new Hypercore({ core: a.core, encryptionKey })
  const c = b.session()

  await a.ready()
  await b.ready()
  await c.ready()

  t.absent(a.encryptionKey)
  t.ok(b.encryptionKey)
  t.ok(c.encryptionKey)

  await c.close()
  await b.close()
  await a.close()
})

test('session keeps encryption', async function (t) {
  const storage = await createStorage(t)

  const a = new Hypercore(storage)
  const b = a.session({ encryptionKey })
  await b.ready()

  t.alike(b.encryptionKey, encryptionKey)

  await b.close()
  await a.close()
})

function getBlock (core, index) {
  const batch = core.core.storage.createReadBatch()
  const b = core.core.blocks.get(batch, index)
  batch.tryFlush()
  return b
}
