const test = require('brittle')
const { create, replicate } = require('./helpers')

const encryptionKey = Buffer.alloc(32, 'hello world')

test('encrypted append and get', async function (t) {
  const a = await create({ encryptionKey })

  t.alike(a.encryptionKey, encryptionKey)

  await a.append(['hello'])

  t.is(a.byteLength, 5)
  t.is(a.core.tree.byteLength, 5 + a.padding)

  const unencrypted = await a.get(0)
  const encrypted = await a.core.blocks.get(0)

  t.alike(unencrypted, Buffer.from('hello'))
  t.unlike(unencrypted, encrypted)
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
    t.plan(10)

    const b = await create(a.key, { encryptionKey })

    b.on('download', (i, block) => {
      t.alike(block, Buffer.from([i + /* a */ 0x61]))
    })

    replicate(a, b, t)

    const r = b.download({ start: 0, end: a.length })
    await r.downloaded()

    for (let i = 0; i < 5; i++) {
      t.alike(await b.get(i), await a.get(i))
    }
  })

  t.test('without encryption key', async function (t) {
    const b = await create(a.key)

    replicate(a, b, t)

    const r = b.download({ start: 0, end: a.length })
    await r.downloaded()

    for (let i = 0; i < 5; i++) {
      t.unlike(await b.get(i), await a.get(i))
      t.alike(await b.get(i), await a.core.blocks.get(i))
    }
  })
})

test('encrypted sessions', async function (t) {
  const a = await create({ encryptionKey })

  await a.append(['hello'])

  const session = a.session()

  t.alike(await session.get(0), Buffer.from('hello'))
})
