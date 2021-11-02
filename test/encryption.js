const test = require('brittle')
const RAM = require('random-access-memory')
const Hypercore = require('..')
const { create, replicate } = require('./helpers')

const encryptionKey = Buffer.alloc(32, 'hello world')

test('encrypted append and get', async function (t) {
  const a = await create({ encryptionKey })

  t.alike(a.encryptionKey, encryptionKey)

  await a.append(['hello'])

  t.is(a.byteLength, 5)
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

  t.test('with encryption key', async function (t) {
    const b = await create(a.key, { encryptionKey })

    replicate(a, b, t)

    await t.test('through direct download', async function (t) {
      const r = b.download({ start: 0, end: a.length })
      await r.downloaded()

      for (let i = 0; i < 5; i++) {
        t.alike(await b.get(i), await a.get(i))
      }
    })

    await t.test('through indirect download', async function (t) {
      await a.append(['f', 'g', 'h', 'i', 'j'])

      for (let i = 5; i < 10; i++) {
        t.alike(await b.get(i), await a.get(i))
      }
    })
  })

  t.test('without encryption key', async function (t) {
    const b = await create(a.key)

    replicate(a, b, t)

    await t.test('through direct download', async function (t) {
      const r = b.download({ start: 0, end: a.length })
      await r.downloaded()

      for (let i = 0; i < 5; i++) {
        t.alike(await b.get(i), await a.core.blocks.get(i))
      }
    })

    await t.test('through indirect download', async function (t) {
      await a.append(['f', 'g', 'h', 'i', 'j'])

      for (let i = 5; i < 10; i++) {
        t.alike(await b.get(i), await a.core.blocks.get(i))
      }
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
