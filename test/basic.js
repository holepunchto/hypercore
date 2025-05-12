const test = require('brittle')
const b4a = require('b4a')
const createTempDir = require('test-tmp')
const HypercoreStorage = require('hypercore-storage')

const Hypercore = require('../')
const { create, createStorage, eventFlush } = require('./helpers')

test('basic', async function (t) {
  const core = await create(t)
  let appends = 0

  t.is(core.length, 0)
  t.is(core.writable, true)
  t.is(core.readable, true)

  core.on('append', function () {
    appends++
  })

  await core.append('hello')
  t.is(core.length, 1)
  await core.append('world')
  t.is(core.length, 2)

  const info = await core.info()

  t.is(core.length, 2)
  t.is(info.byteLength, 10)
  t.is(appends, 2)
})

test('core id', async function (t) {
  const key = b4a.alloc(32).fill('a')

  const db = await createStorage(t)
  const core = new Hypercore(db, key)

  await core.ready()
  t.is(core.id, 'cfosnambcfosnambcfosnambcfosnambcfosnambcfosnambcfoo')

  await core.close()
})

test('session id', async function (t) {
  const key = b4a.alloc(32).fill('a')

  const db = await createStorage(t)
  const core = new Hypercore(db, key)

  const session = core.session()

  await session.ready()
  t.is(session.id, 'cfosnambcfosnambcfosnambcfosnambcfosnambcfosnambcfoo')

  await core.close()
  await session.close()
})

test('session', async function (t) {
  const core = await create(t)

  const session = core.session()

  await session.append('test')
  t.alike(await core.get(0), b4a.from('test'))
  t.alike(await session.get(0), b4a.from('test'))

  await session.close()
})

test('close', async function (t) {
  const core = await create(t)
  await core.append('hello world')

  await core.close()

  try {
    await core.get(0)
    t.fail('core should be closed')
  } catch {
    t.pass('get threw correctly when core was closed')
  }
})

test('close multiple', async function (t) {
  const core = await create(t)
  await core.append('hello world')

  const ev = t.test('events')

  ev.plan(4)

  let i = 0

  core.on('close', () => ev.is(i++, 0, 'on close'))
  core.close().then(() => ev.is(i++, 1, 'first close'))
  core.close().then(() => ev.is(i++, 2, 'second close'))
  core.close().then(() => ev.is(i++, 3, 'third close'))

  await ev
})

test('storage options', async function (t) {
  const db = await createStorage(t)
  const core = new Hypercore({ storage: db })
  await core.append('hello')
  t.alike(await core.get(0), b4a.from('hello'))

  await core.close()
})

test('createIfMissing', async function (t) {
  const db = await createStorage(t)
  const core = new Hypercore(db, { createIfMissing: false })

  await t.exception(core.ready())
  await db.close()
})

test('reopen writable core', async function (t) {
  const dir = await createTempDir(t)

  const core = new Hypercore(dir)
  await core.ready()

  let appends = 0

  t.is(core.length, 0)
  t.is(core.writable, true)
  t.is(core.readable, true)

  core.on('append', function () {
    appends++
  })

  await core.append('hello')
  await core.append('world')

  const info = await core.info()

  t.is(core.length, 2)
  t.is(info.byteLength, 10)
  t.is(appends, 2)

  await core.close()

  const core2 = new Hypercore(dir)
  await core2.ready()

  t.is(core2.length, 2)
  t.is(core2.writable, true)
  t.is(core2.readable, true)

  core2.on('append', function () {
    appends++
  })

  await core2.append('goodbye')
  await core2.append('test')

  t.is(core2.length, 4)
  t.is(appends, 4)

  await core2.close()
})

test('reopen and overwrite', async function (t) {
  const dir = await createTempDir()
  let storage = null

  const core = new Hypercore(await open())

  await core.ready()
  await core.close()
  const key = core.key

  const reopen = new Hypercore(await open())

  await reopen.ready()
  t.alike(reopen.key, key, 'reopened the core')
  await reopen.close()

  const overwritten = new Hypercore(await open(), { overwrite: true })

  await overwritten.ready()
  t.unlike(overwritten.key, key, 'overwrote the core')

  await overwritten.close()

  async function open () {
    if (storage) await storage.close()
    storage = await createStorage(t, dir)
    return storage
  }
})

test('truncate event has truncated-length and fork', async function (t) {
  t.plan(2)

  const core = new Hypercore(await createStorage(t))

  core.on('truncate', function (length, fork) {
    t.is(length, 2)
    t.is(fork, 1)
  })

  await core.append(['a', 'b', 'c'])
  await core.truncate(2)
  await core.close()
})

test('treeHash gets the tree hash at a given core length', async function (t) {
  const core = new Hypercore(await createStorage(t))
  await core.ready()

  const { core: { state } } = core

  const hashes = [state.hash()]

  for (let i = 1; i < 10; i++) {
    await core.append([`${i}`])
    hashes.push(state.hash())
  }

  for (let i = 0; i < 10; i++) {
    t.alike(await core.treeHash(i), hashes[i])
  }

  await core.close()
})

test('treeHash with default length', async function (t) {
  const core = new Hypercore(await createStorage(t))
  const core2 = new Hypercore(await createStorage(t))
  await core.ready()
  await core2.ready()

  t.alike(await core.treeHash(), await core2.treeHash())

  await core.append('a')

  t.unlike(await core.treeHash(), await core2.treeHash())

  await core.close()
  await core2.close()
})

test('snapshot locks the state', async function (t) {
  const core = new Hypercore(await createStorage(t))
  await core.ready()

  const a = core.snapshot()

  await core.append('a')

  t.is(a.length, 0)
  t.is(core.length, 1)

  const b = core.snapshot()

  await core.append('c')

  t.is(a.length, 0)
  t.is(b.length, 1)

  await core.close()
  await a.close()
  await b.close()
})

test('downloading local range', async function (t) {
  t.plan(1)

  const core = new Hypercore(await createStorage(t))

  await core.append('a')

  const range = core.download({ start: 0, end: 1 })

  await eventFlush()

  await range.destroy()

  t.pass('did not throw')

  await core.close()
})

test('read ahead', async function (t) {
  t.plan(1)

  const core = new Hypercore(await createStorage(t), { valueEncoding: 'utf-8' })

  await core.append('a')

  const blk = core.get(1, { wait: true }) // readahead

  await eventFlush()

  await core.append('b')

  t.alike(await blk, 'b')

  await core.close()
})

test('defaults for wait', async function (t) {
  t.plan(5)

  const core = new Hypercore(await createStorage(t), b4a.alloc(32), { valueEncoding: 'utf-8' })

  const a = core.get(1)

  a.catch(function (err) {
    t.ok(err, 'a failed')
  })

  t.is(await core.get(1, { wait: false }), null)

  const s = core.session({ wait: false })

  const b = s.get(1, { wait: true })

  b.catch(function (err) {
    t.ok(err, 'b failed')
  })

  t.is(await s.get(1), null)

  const s2 = s.session() // check if wait is inherited

  t.is(await s2.get(1), null)

  await s.close()
  await s2.close()
  await core.close()
})

test('has', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c', 'd', 'e', 'f'])

  for (let i = 0; i < core.length; i++) {
    t.ok(await core.has(i), `has ${i}`)
  }

  await core.clear(2)
  t.comment('2 cleared')

  for (let i = 0; i < core.length; i++) {
    if (i === 2) {
      t.absent(await core.has(i), `does not have ${i}`)
    } else {
      t.ok(await core.has(i), `has ${i}`)
    }
  }

  await core.close()
})

test('has range', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c', 'd', 'e', 'f'])

  t.ok(await core.has(0, 5), 'has 0 to 4')

  await core.clear(2)
  t.comment('2 cleared')

  t.absent(await core.has(0, 5), 'does not have 0 to 4')
  t.ok(await core.has(0, 2), 'has 0 to 1')
  t.ok(await core.has(3, 5), 'has 3 to 4')

  await core.close()
})

test.skip('storage info', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c', 'd', 'e', 'f'])

  const info = await core.info({ storage: true })

  t.snapshot(info.storage)

  await core.close()
})

test('storage info, off by default', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c', 'd', 'e', 'f'])

  const info = await core.info()

  t.is(info.storage, null)

  await core.close()
})

test('signedLength mirrors core length (linearised core compat)', async function (t) {
  const core = await create(t)
  t.is(core.length, 0)
  t.is(core.signedLength, core.length)

  await core.append(['a', 'b'])
  t.is(core.length, 2)
  t.is(core.signedLength, core.length)

  await core.close()
})

test('key is set sync', async function (t) {
  const key = b4a.from('a'.repeat(64), 'hex')

  const dir1 = await createStorage(t)
  const dir2 = await createStorage(t)
  const dir3 = await createStorage(t)
  const dir4 = await createStorage(t)

  const core1 = new Hypercore(dir1, key)
  const core2 = new Hypercore(dir2)
  const core3 = new Hypercore(dir3, { key })
  const core4 = new Hypercore(dir4, { })

  // flush all db ops before teardown
  t.teardown(() => core1.close())
  t.teardown(() => core2.close())
  t.teardown(() => core3.close())
  t.teardown(() => core4.close())

  t.alike(core1.key, key)
  t.is(core2.key, null)
  t.alike(core3.key, key)
  t.is(core4.key, null)
})

test('disable writable option', async function (t) {
  t.plan(2)

  const core = new Hypercore(await createStorage(t), { writable: false })
  await core.ready()

  t.is(core.writable, false)

  try {
    await core.append('abc')
    t.fail('should have failed')
  } catch (err) {
    t.pass(err.code, 'SESSION_NOT_WRITABLE')
  }

  await core.close()
})

test('disable session writable option', async function (t) {
  t.plan(3)

  const core = new Hypercore(await createStorage(t))
  await core.ready()

  const session = core.session({ writable: false })
  await session.ready()

  t.is(core.writable, true)
  await core.append('abc')

  t.is(session.writable, false)
  try {
    await session.append('abc')
    t.fail('should have failed')
  } catch (err) {
    t.pass(err.code, 'SESSION_NOT_WRITABLE')
  }

  await session.close()
  await core.close()
})

test('session of a session with the writable option disabled', async function (t) {
  t.plan(1)

  const core = new Hypercore(await createStorage(t))
  const s1 = core.session({ writable: false })
  const s2 = s1.session()

  try {
    await s2.append('abc')
    t.fail('should have failed')
  } catch (err) {
    t.pass(err.code, 'SESSION_NOT_WRITABLE')
  }

  await s1.close()
  await s2.close()
  await core.close()
})

test('writable session on a readable only core', async function (t) {
  t.plan(2)

  const core = new Hypercore(await createStorage(t))
  await core.ready()

  const a = new Hypercore(await createStorage(t), core.key)
  const s = a.session({ writable: true })
  await s.ready()
  t.is(s.writable, false)

  try {
    await s.append('abc')
    t.fail('should have failed')
  } catch (err) {
    t.pass(err.code, 'SESSION_NOT_WRITABLE')
  }

  await s.close()
  await a.close()
  await core.close()
})

test('append above the max suggested block size', async function (t) {
  t.plan(1)

  const core = new Hypercore(await createStorage(t))

  try {
    await core.append(Buffer.alloc(Hypercore.MAX_SUGGESTED_BLOCK_SIZE))
  } catch (e) {
    t.fail('should not throw')
  }

  try {
    await core.append(Buffer.alloc(Hypercore.MAX_SUGGESTED_BLOCK_SIZE + 1))
  } catch {
    t.pass('should throw')
  }

  await core.close()
})

test('get undefined block is not allowed', async function (t) {
  t.plan(1)

  const core = new Hypercore(await createStorage(t))

  try {
    await core.get(undefined)
    t.fail()
  } catch (err) {
    t.pass(err.code, 'ERR_ASSERTION')
  }

  await core.close()
})

test('valid manifest passed to a session is stored', async function (t) {
  t.plan(1)

  const core = new Hypercore(await createStorage(t), {
    manifest: {
      prologue: {
        hash: b4a.alloc(32),
        length: 1
      },
      signers: []
    }
  })

  await core.ready()

  const a = new Hypercore(await createStorage(t), core.key)
  const b = new Hypercore(null, core.key, { manifest: core.manifest, core: a.core })

  await b.ready()

  t.alike(b.manifest, core.manifest)

  await a.close()
  await b.close()
  await core.close()
})

test('exclusive sessions', async function (t) {
  const core = new Hypercore(await createStorage(t))

  const a = core.session({ exclusive: true })
  await a.ready()

  setTimeout(() => a.close(), 200)

  const b = core.session({ exclusive: true })
  await b.ready()
  t.ok(a.closed)
  await b.close()

  await core.close()
})

test('truncate has correct storage state in memory and persisted', async function (t) {
  const tmpDir = await t.tmp()
  {
    const storage = new HypercoreStorage(tmpDir)
    const core = new Hypercore(storage)
    await core.append(['a', 'b', 'c', 'd', 'e'])
    await core.truncate(2)
    t.alike(getBitfields(core, 0, 5), [true, true, false, false, false])
    t.is(core.contiguousLength, 2)
    t.is(core.core.header.hints.contiguousLength, 2)
    t.is(await getContiguousLengthInStorage(core), 2)
    await core.close()
  }

  {
    const storage = new HypercoreStorage(tmpDir)
    const core = new Hypercore(storage)
    await core.ready()
    t.alike(getBitfields(core, 0, 5), [true, true, false, false, false])
    t.is(core.contiguousLength, 2)
    t.is(core.core.header.hints.contiguousLength, 2)
    t.is(await getContiguousLengthInStorage(core), 2)
    await core.close()
  }
})

test('clear has correct storage state in memory and persisted', async function (t) {
  const tmpDir = await t.tmp()
  {
    const storage = new HypercoreStorage(tmpDir)
    const core = new Hypercore(storage)
    await core.append(['a', 'b', 'c', 'd', 'e'])
    await core.clear(2)
    t.alike(getBitfields(core, 0, 5), [true, true, false, true, true])
    t.is(core.contiguousLength, 2)
    t.is(core.core.header.hints.contiguousLength, 2)
    t.is(await getContiguousLengthInStorage(core), 2)
    await core.close()
  }

  {
    const storage = new HypercoreStorage(tmpDir)
    const core = new Hypercore(storage)
    await core.ready()
    t.alike(getBitfields(core, 0, 5), [true, true, false, true, true])
    t.is(core.contiguousLength, 2)
    t.is(core.core.header.hints.contiguousLength, 2)
    t.is(await getContiguousLengthInStorage(core), 2)
    await core.close()
  }
})

test('contiguousLength 0 for in-memory view after core ready', async function (t) {
  const tmpDir = await t.tmp()
  {
    const storage = new HypercoreStorage(tmpDir)
    const core = new Hypercore(storage)
    await core.ready()
    t.is(core.contiguousLength, 0)
    t.is(core.core.header.hints.contiguousLength, 0)
    await core.close()
  }

  {
    const storage = new HypercoreStorage(tmpDir)
    const core = new Hypercore(storage)
    await core.ready()
    t.is(core.contiguousLength, 0)
    t.is(core.core.header.hints.contiguousLength, 0)
    await core.close()
  }
})

test('contiguousLength gets updated after an append (also on disk)', async function (t) {
  const tmpDir = await t.tmp()
  {
    const storage = new HypercoreStorage(tmpDir)
    const core = new Hypercore(storage)
    await core.append(['a', 'b', 'c', 'd', 'e'])
    t.alike(getBitfields(core, 0, 5), [true, true, true, true, true])
    t.is(core.contiguousLength, 5)
    t.is(core.core.header.hints.contiguousLength, 5)
    t.is(await getContiguousLengthInStorage(core), 5)
    await core.close()
  }

  {
    const storage = new HypercoreStorage(tmpDir)
    const core = new Hypercore(storage)
    await core.ready()
    t.alike(getBitfields(core, 0, 5), [true, true, true, true, true])
    t.is(core.contiguousLength, 5)
    t.is(core.core.header.hints.contiguousLength, 5)
    t.is(await getContiguousLengthInStorage(core), 5)

    await core.append(['f', 'g'])
    t.alike(getBitfields(core, 0, 8), [true, true, true, true, true, true, true, false])
    t.is(core.contiguousLength, 7)
    t.is(core.core.header.hints.contiguousLength, 7)
    t.is(await getContiguousLengthInStorage(core), 7)

    await core.clear(4)
    t.alike(getBitfields(core, 0, 8), [true, true, true, true, false, true, true, false])
    t.is(core.contiguousLength, 4)
    t.is(core.core.header.hints.contiguousLength, 4)
    t.is(await getContiguousLengthInStorage(core), 4)

    await core.append(['h'])
    t.alike(getBitfields(core, 0, 8), [true, true, true, true, false, true, true, true])
    t.is(core.contiguousLength, 4)
    t.is(core.core.header.hints.contiguousLength, 4)
    t.is(await getContiguousLengthInStorage(core), 4)

    await core.close()
  }

  {
    const storage = new HypercoreStorage(tmpDir)
    const core = new Hypercore(storage)
    await core.ready()
    t.alike(getBitfields(core, 0, 8), [true, true, true, true, false, true, true, true])
    t.is(core.contiguousLength, 4)
    t.is(core.core.header.hints.contiguousLength, 4)
    t.is(await getContiguousLengthInStorage(core), 4)
    await core.close()
  }
})

test('append alignment to bitfield boundary', async function (t) {
  const tmpDir = await t.tmp()

  const expectedBitfields = new Array(32768)
  expectedBitfields.fill(true)
  expectedBitfields.push(false)

  {
    const storage = new HypercoreStorage(tmpDir)
    const core = new Hypercore(storage)
    await core.ready()

    const b = []
    for (let i = 0; i < 32768; i++) {
      b.push('#')
    }

    await core.append(b)

    t.alike(getBitfields(core, 0, 32769), expectedBitfields)
    t.is(core.contiguousLength, 32768)
    t.is(core.core.header.hints.contiguousLength, 32768)

    await core.close()
  }

  {
    const storage = new HypercoreStorage(tmpDir)
    const core = new Hypercore(storage)
    await core.ready()

    t.alike(getBitfields(core, 0, 32769), expectedBitfields)
    t.is(core.contiguousLength, 32768)
    t.is(core.core.header.hints.contiguousLength, 32768)

    await core.close()
  }
})

function getBitfields (hypercore, start = 0, end = null) {
  if (!end) end = hypercore.length

  const res = []
  for (let i = start; i < end; i++) {
    res.push(hypercore.core.bitfield.get(i))
  }

  return res
}

async function getContiguousLengthInStorage (hypercore) {
  const storageRx = hypercore.core.storage.read()
  const [res] = await Promise.all([storageRx.getHints(), storageRx.tryFlush()])
  return res === null ? null : res.contiguousLength
}
