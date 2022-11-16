const test = require('brittle')
const RAM = require('random-access-memory')

const Hypercore = require('../')
const { create, eventFlush } = require('./helpers')

test('basic', async function (t) {
  const core = await create()
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
})

test('core id', async function (t) {
  const key = Buffer.alloc(32).fill('a')

  const core = new Hypercore(RAM, key)
  t.is(core.id, null)

  await core.ready()
  t.is(core.id, 'cfosnambcfosnambcfosnambcfosnambcfosnambcfosnambcfoo')
})

test('session id', async function (t) {
  const key = Buffer.alloc(32).fill('a')
  const core = new Hypercore(RAM, key)

  const session = core.session()
  t.is(session.id, null)

  await session.ready()
  t.is(session.id, 'cfosnambcfosnambcfosnambcfosnambcfosnambcfosnambcfoo')
})

test('session', async function (t) {
  const core = await create()

  const session = core.session()

  await session.append('test')
  t.alike(await core.get(0), Buffer.from('test'))
  t.alike(await session.get(0), Buffer.from('test'))
})

test('close', async function (t) {
  const core = await create()
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
  const core = await create()
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
  const core = new Hypercore({ storage: RAM })
  await core.append('hello')
  t.alike(await core.get(0), Buffer.from('hello'))
})

test(
  'allow publicKeys with different byteLength that 32, if opts.crypto were passed',
  function (t) {
    const key = Buffer.alloc(33).fill('a')

    const core = new Hypercore(RAM, key, { crypto: {} })

    t.is(core.key, key)
    t.pass('creating a core with more than 32 byteLength key did not throw')
  }
)

test('createIfMissing', async function (t) {
  const core = new Hypercore(RAM, { createIfMissing: false })

  await t.exception(core.ready())
})

test('reopen and overwrite', async function (t) {
  const st = {}
  const core = new Hypercore(open)

  await core.ready()
  const key = core.key

  const reopen = new Hypercore(open)

  await reopen.ready()
  t.alike(reopen.key, key, 'reopened the core')

  const overwritten = new Hypercore(open, { overwrite: true })

  await overwritten.ready()
  t.unlike(overwritten.key, key, 'overwrote the core')

  function open (name) {
    if (st[name]) return st[name]
    st[name] = new RAM()
    return st[name]
  }
})

test('truncate event has truncated-length and fork', async function (t) {
  t.plan(2)

  const core = new Hypercore(RAM)

  core.on('truncate', function (length, fork) {
    t.is(length, 2)
    t.is(fork, 1)
  })

  await core.append(['a', 'b', 'c'])
  await core.truncate(2)
})

test('treeHash gets the tree hash at a given core length', async function (t) {
  const core = new Hypercore(RAM)
  await core.ready()

  const { core: { tree } } = core

  const hashes = [tree.hash()]

  for (let i = 1; i < 10; i++) {
    await core.append([`${i}`])
    hashes.push(tree.hash())
  }

  for (let i = 0; i < 10; i++) {
    t.alike(await core.treeHash(i), hashes[i])
  }
})

test('treeHash with default length', async function (t) {
  const core = new Hypercore(RAM)
  const core2 = new Hypercore(RAM)
  await core.ready()
  await core2.ready()

  t.alike(await core.treeHash(), await core2.treeHash())

  await core.append('a')

  t.unlike(await core.treeHash(), await core2.treeHash())
})

test('snapshot locks the state', async function (t) {
  const core = new Hypercore(RAM)
  await core.ready()

  const a = core.snapshot()

  await core.append('a')

  t.is(a.length, 0)
  t.is(core.length, 1)

  const b = core.snapshot()

  await core.append('c')

  t.is(a.length, 0)
  t.is(b.length, 1)
})

test('downloading local range', async function (t) {
  t.plan(1)

  const core = new Hypercore(RAM)

  await core.append('a')

  const range = core.download({ start: 0, end: 1 })

  await eventFlush()

  await range.destroy()

  t.pass('did not throw')
})

test('read ahead', async function (t) {
  t.plan(1)

  const core = new Hypercore(RAM, { valueEncoding: 'utf-8' })

  await core.append('a')

  const blk = core.get(1, { wait: true }) // readahead

  await eventFlush()

  await core.append('b')

  t.alike(await blk, 'b')
})

test('defaults for wait', async function (t) {
  t.plan(5)

  const core = new Hypercore(RAM, Buffer.alloc(32), { valueEncoding: 'utf-8' })

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
  await core.close()
})

test('has', async function (t) {
  const core = await create()
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
})

test('has range', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c', 'd', 'e', 'f'])

  t.ok(await core.has(0, 5), 'has 0 to 4')

  await core.clear(2)
  t.comment('2 cleared')

  t.absent(await core.has(0, 5), 'does not have 0 to 4')
  t.ok(await core.has(0, 2), 'has 0 to 1')
  t.ok(await core.has(3, 5), 'has 3 to 4')
})

test('storage info', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c', 'd', 'e', 'f'])

  const info = await core.info({ storage: true })

  t.snapshot(info.storage)
})

test('storage info, off by default', async function (t) {
  const core = await create()
  await core.append(['a', 'b', 'c', 'd', 'e', 'f'])

  const info = await core.info()

  t.is(info.storage, null)
})
