const test = require('brittle')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const Hypercore = require('..')
const { create, createStorage, replicate } = require('./helpers')

const fixturesRaw = require('./fixtures/encryption/v11.0.48.cjs')

const encryptionKey = b4a.alloc(32, 'hello world')

test('encrypted append and get', async function (t) {
  const a = await create(t, { encryption: { key: encryptionKey } })

  t.ok(a.encryption)

  await a.append(['hello'])

  const info = await a.info()
  t.is(info.byteLength, 5)
  t.is(a.core.state.byteLength, 5 + a.padding)

  const unencrypted = await a.get(0)
  t.alike(unencrypted, b4a.from('hello'))

  const encrypted = await getBlock(a, 0)
  t.absent(encrypted.includes('hello'))
})

test('get with decrypt option', async function (t) {
  const a = await create(t, { encryption: { key: encryptionKey } })

  await a.append('hello')

  const unencrypted = await a.get(0, { decrypt: true })
  t.alike(unencrypted, b4a.from('hello'))

  const encrypted = await a.get(0, { decrypt: false })
  t.absent(encrypted.includes('hello'))
})

test('encrypted seek', async function (t) {
  const a = await create(t, { encryption: { key: encryptionKey } })

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
  const a = await create(t, { encryption: { key: encryptionKey } })

  await a.append(['a', 'b', 'c', 'd', 'e'])

  await t.test('with encryption key', async function (t) {
    const b = await create(t, a.key, { encryption: { key: encryptionKey } })

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

test('encrypted seek via replication', async function (t) {
  const a = await create(t, { encryption: { key: encryptionKey } })
  const b = await create(t, a.key, { encryption: { key: encryptionKey } })

  await a.append(['hello', 'world', '!'])

  replicate(a, b, t)

  t.alike(await b.seek(0), [0, 0])
  t.alike(await b.seek(4), [0, 4])
  t.alike(await b.seek(5), [1, 0])
  t.alike(await b.seek(6), [1, 1])
  t.alike(await b.seek(6), [1, 1])
  t.alike(await b.seek(9), [1, 4])
  t.alike(await b.seek(10), [2, 0])
  t.alike(await b.seek(11), [3, 0])
})

test('encrypted session', async function (t) {
  const a = await create(t, { encryption: { key: encryptionKey } })

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

  const a = new Hypercore(storage, { encryption: { key: encryptionKey } })
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

  const s = a.session({ encryption: { key: encryptionKey }, debug: 'debug' })

  t.ok(s.encryption)
  t.absent(a.encryption)

  await s.append(['hello'])

  const unencrypted = await s.get(0)
  t.alike(unencrypted, b4a.from('hello'))

  const encrypted = await a.get(0)
  t.absent(encrypted.includes('hello'))

  await s.close()
})

test('encrypted session on encrypted core, same key', async function (t) {
  const a = await create(t, { encryption: { key: encryptionKey } })
  const s = a.session({ encryption: { key: encryptionKey } })

  t.alike(s.encryptionKey, a.encryptionKey)

  await s.append(['hello'])

  const unencrypted = await s.get(0)
  t.alike(unencrypted, b4a.from('hello'))
  t.alike(unencrypted, await a.get(0))

  await s.close()
})

test('multiple gets to replicated, encrypted block', async function (t) {
  const a = await create(t, { encryption: { key: encryptionKey } })
  await a.append('a')

  const b = await create(t, a.key, { encryption: { key: encryptionKey } })

  replicate(a, b, t)

  const p = b.get(0)
  const q = b.get(0)

  t.alike(await p, await q)
  t.alike(await p, b4a.from('a'))
})

test('encrypted core from existing unencrypted core', async function (t) {
  const a = await create(t, { encryptionKey: null })
  const b = new Hypercore({ core: a.core, encryption: { key: encryptionKey } })

  t.alike(b.key, a.key)

  await b.append(['hello'])

  const unencrypted = await b.get(0)
  t.alike(unencrypted, b4a.from('hello'))

  await b.close()
})

test('from session sessions pass encryption', async function (t) {
  const storage = await createStorage(t)

  const a = new Hypercore(storage)
  const b = new Hypercore({ core: a.core, encryption: { key: encryptionKey } })
  const c = b.session()

  await a.ready()
  await b.ready()
  await c.ready()

  t.absent(a.encryption)
  t.ok(b.encryption)
  t.ok(c.encryption)

  await c.close()
  await b.close()
  await a.close()
})

test('session keeps encryption', async function (t) {
  const storage = await createStorage(t)

  const a = new Hypercore(storage)
  const b = a.session({ encryption: { key: encryptionKey } })
  await b.ready()

  await b.close()
  await a.close()
})

// block encryption module is only available after bmping manifest version
test('block encryption module', async function (t) {
  class XOREncryption {
    padding () {
      return 0
    }

    async encrypt (index, block) {
      await new Promise(setImmediate)

      for (let i = 0; i < block.byteLength; i++) {
        block[i] ^= ((index + 1) & 0xff) // +1 so no 0 xor in test
      }
    }

    async decrypt (index, block) {
      await new Promise(setImmediate)

      for (let i = 0; i < block.byteLength; i++) {
        block[i] ^= ((index + 1) & 0xff)
      }
    }
  }

  const core = await create(t, null, { encryption: new XOREncryption() })
  await core.ready()

  await core.append('0')
  await core.append('1')
  await core.append('2')

  t.unlike(await core.get(0, { raw: true }), b4a.from('0'))
  t.unlike(await core.get(1, { raw: true }), b4a.from('1'))
  t.unlike(await core.get(2, { raw: true }), b4a.from('2'))

  t.alike(await core.get(0), b4a.from('0'))
  t.alike(await core.get(1), b4a.from('1'))
  t.alike(await core.get(2), b4a.from('2'))
})

test('encryption backwards compatibility', async function (t) {
  const encryptionKey = b4a.alloc(32).fill('encryption key')

  const compatKey = crypto.keyPair(b4a.alloc(32, 0))
  const defaultKey = crypto.keyPair(b4a.alloc(32, 1))
  const blockKey = crypto.keyPair(b4a.alloc(32, 2))

  const fixtures = [
    getFixture('compat'),
    getFixture('default'),
    getFixture('default'),
    getFixture('block')
  ]

  const compat = await create(t, null, { keyPair: compatKey, encryptionKey, compat: true })
  const def = await create(t, null, { keyPair: defaultKey, encryptionKey, isBlockKey: false })
  const notBlock = await create(t, null, { keyPair: defaultKey, encryptionKey, isBlockKey: false })
  const block = await create(t, null, { keyPair: blockKey, encryptionKey, isBlockKey: true })

  await compat.ready()
  await def.ready()
  await notBlock.ready()
  await block.ready()

  const largeBlock = Buffer.alloc(512)
  for (let i = 0; i < largeBlock.byteLength; i++) largeBlock[i] = i & 0xff

  for (let i = 0; i < 10; i++) {
    await compat.append('compat test: ' + i.toString())
    await def.append('default test: ' + i.toString())
    await notBlock.append('default test: ' + i.toString())
    await block.append('block test: ' + i.toString())
  }

  await compat.append(largeBlock.toString('hex'))
  await def.append(largeBlock.toString('hex'))
  await notBlock.append(largeBlock.toString('hex'))
  await block.append(largeBlock.toString('hex'))

  // compat
  t.comment('test compat mode')
  t.is(compat.length, fixtures[0].length)

  for (let i = 0; i < compat.length; i++) {
    t.alike(await compat.get(i, { raw: true }), fixtures[0][i])
  }

  // default
  t.comment('test default mode')
  t.is(def.length, fixtures[1].length)

  for (let i = 0; i < def.length; i++) {
    t.alike(await def.get(i, { raw: true }), fixtures[1][i])
  }

  // not block
  t.comment('test block false')
  t.is(notBlock.length, fixtures[2].length)

  for (let i = 0; i < notBlock.length; i++) {
    t.alike(await notBlock.get(i, { raw: true }), fixtures[2][i])
  }

  // compat
  t.comment('test block mode')
  t.is(block.length, fixtures[3].length)

  for (let i = 0; i < block.length; i++) {
    t.alike(await block.get(i, { raw: true }), fixtures[3][i])
  }
})

function getBlock (core, index) {
  const batch = core.core.storage.read()
  const b = batch.getBlock(index)
  batch.tryFlush()
  return b
}

function getFixture (name) {
  const blocks = fixturesRaw[name]
  return blocks.map(b => b4a.from(b, 'base64'))
}
