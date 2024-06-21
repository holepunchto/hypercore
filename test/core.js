const test = require('brittle')
const RAM = require('random-access-memory')
const b4a = require('b4a')
const createTempDir = require('test-tmp')
const CoreStorage = require('hypercore-on-the-rocks')
const Core = require('../lib/core')

test('core - append', async function (t) {
  const { core } = await create(t)

  {
    const info = await core.append([
      b4a.from('hello'),
      b4a.from('world')
    ])

    t.alike(info, { length: 2, byteLength: 10 })
    t.is(core.tree.length, 2)
    t.is(core.tree.byteLength, 10)

    t.alike([
      await getBlock(core, 0),
      await getBlock(core, 1)
    ], [
      b4a.from('hello'),
      b4a.from('world')
    ])
  }

  {
    const info = await core.append([
      b4a.from('hej')
    ])

    t.alike(info, { length: 3, byteLength: 13 })
    t.is(core.tree.length, 3)
    t.is(core.tree.byteLength, 13)

    t.alike([
      await getBlock(core, 0),
      await getBlock(core, 1),
      await getBlock(core, 2)
    ], [
      b4a.from('hello'),
      b4a.from('world'),
      b4a.from('hej')
    ])
  }
})

test('core - append and truncate', async function (t) {
  const { core, reopen } = await create(t)

  await core.append([
    b4a.from('hello'),
    b4a.from('world'),
    b4a.from('fo'),
    b4a.from('ooo')
  ])

  await core.truncate(3, 1)

  t.is(core.tree.length, 3)
  t.is(core.tree.byteLength, 12)
  t.is(core.tree.fork, 1)
  t.alike(core.header.hints.reorgs, [{ from: 0, to: 1, ancestors: 3 }])

  await core.append([
    b4a.from('a'),
    b4a.from('b'),
    b4a.from('c'),
    b4a.from('d')
  ])

  await core.truncate(3, 2)

  t.is(core.tree.length, 3)
  t.is(core.tree.byteLength, 12)
  t.is(core.tree.fork, 2)
  t.alike(core.header.hints.reorgs, [{ from: 0, to: 1, ancestors: 3 }, { from: 1, to: 2, ancestors: 3 }])

  await core.truncate(2, 3)

  t.alike(core.header.hints.reorgs, [{ from: 2, to: 3, ancestors: 2 }])

  await core.append([b4a.from('a')])
  await core.truncate(2, 4)

  await core.append([b4a.from('a')])
  await core.truncate(2, 5)

  await core.append([b4a.from('a')])
  await core.truncate(2, 6)

  await core.append([b4a.from('a')])
  await core.truncate(2, 7)

  t.is(core.header.hints.reorgs.length, 4)

  // check that it was persisted
  const coreReopen = await reopen()

  t.is(coreReopen.tree.length, 2)
  t.is(coreReopen.tree.byteLength, 10)
  t.is(coreReopen.tree.fork, 7)
  t.is(coreReopen.header.hints.reorgs.length, 4)
})

test('core - user data', async function (t) {
  const { core, reopen } = await create(t)

  await core.userData('hello', b4a.from('world'))
  t.alike(core.header.userData, [{ key: 'hello', value: b4a.from('world') }])

  await core.userData('hej', b4a.from('verden'))
  t.alike(core.header.userData, [
    { key: 'hello', value: b4a.from('world') },
    { key: 'hej', value: b4a.from('verden') }
  ])

  await core.userData('hello', null)
  t.alike(core.header.userData, [{ key: 'hej', value: b4a.from('verden') }])

  await core.userData('hej', b4a.from('world'))
  t.alike(core.header.userData, [{ key: 'hej', value: b4a.from('world') }])

  // check that it was persisted
  const coreReopen = await reopen()

  t.alike(coreReopen.header.userData, [{ key: 'hej', value: b4a.from('world') }])
})

test('core - verify', async function (t) {
  const { core } = await create(t)
  const { core: clone } = await create(t, { keyPair: { publicKey: core.header.keyPair.publicKey } })

  t.is(clone.header.keyPair.publicKey, core.header.keyPair.publicKey)

  await core.append([b4a.from('a'), b4a.from('b')])

  {
    const p = await core.tree.proof({ upgrade: { start: 0, length: 2 } })
    await clone.verify(p)
  }

  t.is(clone.header.tree.length, 2)
  t.is(clone.header.tree.signature, core.header.tree.signature)

  {
    const p = await core.tree.proof({ block: { index: 1, nodes: await clone.tree.nodes(2), value: true } })
    p.block.value = await getBlock(core, 1)
    await clone.verify(p)
  }
})

test('core - verify parallel upgrades', async function (t) {
  const { core } = await create(t)
  const { core: clone } = await create(t, { keyPair: { publicKey: core.header.keyPair.publicKey } })

  t.is(clone.header.keyPair.publicKey, core.header.keyPair.publicKey)

  await core.append([b4a.from('a'), b4a.from('b'), b4a.from('c'), b4a.from('d')])

  {
    const p1 = await core.tree.proof({ upgrade: { start: 0, length: 2 } })
    const p2 = await core.tree.proof({ upgrade: { start: 0, length: 3 } })

    const v1 = clone.verify(p1)
    const v2 = clone.verify(p2)

    await v1
    await v2
  }

  t.is(clone.header.tree.length, core.header.tree.length)
  t.is(clone.header.tree.signature, core.header.tree.signature)
})

test('core - update hook is triggered', async function (t) {
  const { core } = await create(t)
  const { core: clone } = await create(t, { keyPair: { publicKey: core.header.keyPair.publicKey } })

  let ran = 0

  core.onupdate = (status, bitfield, value, from) => {
    t.ok(status & 0b01, 'was appended')
    t.is(from, null, 'was local')
    t.alike(bitfield, { drop: false, start: 0, length: 4 })
    ran |= 1
  }

  await core.append([b4a.from('a'), b4a.from('b'), b4a.from('c'), b4a.from('d')])

  const peer = {}

  clone.onupdate = (status, bitfield, value, from) => {
    t.ok(status & 0b01, 'was appended')
    t.is(from, peer, 'was remote')
    t.alike(bitfield, { drop: false, start: 1, length: 1 })
    t.alike(value, b4a.from('b'))
    ran |= 2
  }

  {
    const p = await core.tree.proof({ block: { index: 1, nodes: 0, value: true }, upgrade: { start: 0, length: 2 } })
    p.block.value = await getBlock(core, 1)
    await clone.verify(p, peer)
  }

  clone.onupdate = (status, bitfield, value, from) => {
    t.is(status, 0b00, 'no append or truncate')
    t.is(from, peer, 'was remote')
    t.alike(bitfield, { drop: false, start: 3, length: 1 })
    t.alike(value, b4a.from('d'))
    ran |= 4
  }

  {
    const p = await core.tree.proof({ block: { index: 3, nodes: await clone.tree.nodes(6), value: true } })
    p.block.value = await getBlock(core, 3)
    await clone.verify(p, peer)
  }

  core.onupdate = (status, bitfield, value, from) => {
    t.ok(status & 0b10, 'was truncated')
    t.is(from, null, 'was local')
    t.alike(bitfield, { drop: true, start: 1, length: 3 })
    ran |= 8
  }

  await core.truncate(1, 1)

  core.onupdate = (status, bitfield, value, from) => {
    t.ok(status & 0b01, 'was appended')
    t.is(from, null, 'was local')
    t.alike(bitfield, { drop: false, start: 1, length: 1 })
    ran |= 16
  }

  await core.append([b4a.from('e')])

  clone.onupdate = (status, bitfield, value, from) => {
    t.ok(status & 0b11, 'was appended and truncated')
    t.is(from, peer, 'was remote')
    t.alike(bitfield, { drop: true, start: 1, length: 3 })
    ran |= 32
  }

  {
    const p = await core.tree.proof({ hash: { index: 0, nodes: 0 }, upgrade: { start: 0, length: 2 } })
    const r = await clone.tree.reorg(p)
    await clone.reorg(r, peer)
  }

  core.onupdate = (status, bitfield, value, from) => {
    t.ok(status & 0b10, 'was truncated')
    t.is(from, null, 'was local')
    t.alike(bitfield, { drop: true, start: 1, length: 1 })
    ran |= 64
  }

  await core.truncate(1, 2)

  clone.onupdate = (status, bitfield, value, from) => {
    t.ok(status & 0b10, 'was truncated')
    t.is(from, peer, 'was remote')
    t.alike(bitfield, { drop: true, start: 1, length: 1 })
    ran |= 128
  }

  {
    const p = await core.tree.proof({ hash: { index: 0, nodes: 0 }, upgrade: { start: 0, length: 1 } })
    const r = await clone.tree.reorg(p)

    await clone.reorg(r, peer)
  }

  t.is(ran, 255, 'ran all')
})

test.skip('core - clone', async function (t) {
  const { core } = await create(t)

  await core.userData('hello', b4a.from('world'))

  await core.append([
    b4a.from('hello'),
    b4a.from('world')
  ])

  const manifest = { prologue: { hash: await core.tree.hash(), length: core.tree.length } }
  const { core: copy } = (await create(t, { manifest }))

  await copy.copyPrologue(core)

  t.alike(copy.header.userData, [{ key: 'hello', value: b4a.from('world') }])

  t.alike([
    await copy.blocks.get(0),
    await copy.blocks.get(1)
  ], [
    b4a.from('hello'),
    b4a.from('world')
  ])

  const signature = copy.tree.signature
  const roots = copy.tree.roots.map(r => r.index)

  for (let i = 0; i <= core.tree.length * 2; i++) {
    t.alike(
      await copy.tree.get(i, false),
      await core.tree.get(i, false)
    )
  }

  await core.append([b4a.from('c')])

  // copy should be independent
  t.alike(copy.tree.signature, signature)
  t.alike(copy.tree.roots.map(r => r.index), roots)
})

test.skip('core - clone verify', async function (t) {
  const { core } = await create(t)

  await core.append([b4a.from('a'), b4a.from('b')])

  const manifest = { prologue: { hash: await core.tree.hash(), length: core.tree.length } }
  const { core: copy } = await create(t, { manifest })
  const { core: clone } = await create(t, { manifest })

  await copy.copyPrologue(core)

  // copy should be independent
  await core.append([b4a.from('c')])

  {
    const p = await copy.tree.proof({ upgrade: { start: 0, length: 2 } })
    t.ok(await clone.verify(p))
  }

  t.is(clone.header.tree.length, 2)

  {
    const p = await copy.tree.proof({ block: { index: 1, nodes: await clone.tree.nodes(2), value: true } })
    p.block.value = await copy.blocks.get(1)
    await clone.verify(p)
  }

  t.pass('verified')
})

test.skip('core - partial clone', async function (t) {
  const { core } = await create(t)

  await core.append([b4a.from('0')])
  await core.append([b4a.from('1')])

  const manifest = { prologue: { hash: await core.tree.hash(), length: core.tree.length } }

  await core.append([b4a.from('2')])
  await core.append([b4a.from('3')])

  const { core: copy } = (await create(t, { manifest }))

  await copy.copyPrologue(core)

  t.is(core.tree.length, 4)
  t.is(copy.tree.length, 2)

  t.alike([
    await copy.blocks.get(0),
    await copy.blocks.get(1)
  ], [
    b4a.from('0'),
    b4a.from('1')
  ])

  await t.exception(copy.blocks.get(2))
})

test.skip('core - clone with additional', async function (t) {
  const { core } = await create(t)

  await core.append([b4a.from('a'), b4a.from('b')])

  const manifest = { prologue: { hash: await core.tree.hash(), length: core.tree.length } }
  const { core: copy } = await create(t, { manifest })

  await copy.copyPrologue(core, core.tree.signature)

  // copy should be independent
  await core.append([b4a.from('c')])

  const secondManifest = { prologue: { hash: await core.tree.hash(), length: core.tree.length } }
  const { core: clone } = await create(t, { manifest: secondManifest })

  await clone.copyPrologue(copy, { additional: [b4a.from('c')] })

  t.is(clone.header.tree.length, 3)

  t.is(clone.tree.length, core.tree.length)
  t.is(clone.tree.byteLength, core.tree.byteLength)
  t.alike(clone.roots, core.roots)

  t.alike(await clone.blocks.get(0), b4a.from('a'))
  t.alike(await clone.blocks.get(1), b4a.from('b'))
  t.alike(await clone.blocks.get(2), b4a.from('c'))
})

test.skip('core - clone with additional, larger tree', async function (t) {
  const { core } = await create(t)

  await core.append([b4a.from('a'), b4a.from('b')])

  const manifest = { prologue: { hash: await core.tree.hash(), length: core.tree.length } }
  const { core: copy } = await create(t, { manifest })

  await copy.copyPrologue(core)

  const additional = [
    b4a.from('c'),
    b4a.from('d'),
    b4a.from('e'),
    b4a.from('f'),
    b4a.from('g'),
    b4a.from('h'),
    b4a.from('i'),
    b4a.from('j')
  ]

  await core.append(additional)

  const secondManifest = { prologue: { hash: await core.tree.hash(), length: core.tree.length } }
  const { core: clone } = await create(t, { manifest: secondManifest })

  // copy should be independent
  await clone.copyPrologue(copy, { additional })

  t.is(clone.header.tree.length, core.header.tree.length)

  t.is(clone.tree.length, core.tree.length)
  t.is(clone.tree.byteLength, core.tree.byteLength)
  t.alike(clone.roots, core.roots)

  t.alike(await clone.blocks.get(0), b4a.from('a'))
  t.alike(await clone.blocks.get(1), b4a.from('b'))
  t.alike(await clone.blocks.get(2), b4a.from('c'))
  t.alike(await clone.blocks.get(3), b4a.from('d'))
  t.alike(await clone.blocks.get(4), b4a.from('e'))
  t.alike(await clone.blocks.get(5), b4a.from('f'))
  t.alike(await clone.blocks.get(6), b4a.from('g'))
  t.alike(await clone.blocks.get(7), b4a.from('h'))
  t.alike(await clone.blocks.get(8), b4a.from('i'))
  t.alike(await clone.blocks.get(9), b4a.from('j'))
})

test.skip('core - copyPrologue bails if core is not the same', async function (t) {
  const { core } = await create(t)
  const { core: copy } = await create(t, { manifest: { prologue: { hash: b4a.alloc(32), length: 1 } } })

  // copy should be independent
  await core.append([b4a.from('a')])

  await t.exception(copy.copyPrologue(core))
})

test.skip('core - copyPrologue can recover from bad additional', async function (t) {
  const { core } = await create(t)

  await core.append([b4a.from('a'), b4a.from('b')])

  const manifest = { prologue: { hash: await core.tree.hash(), length: core.tree.length } }
  const { core: copy } = await create(t, { manifest })
  await copy.copyPrologue(core)

  // copy should be independent
  await core.append([b4a.from('c')])

  const secondManifest = { prologue: { hash: await core.tree.hash(), length: core.tree.length } }
  const { core: clone } = await create(t, { manifest: secondManifest })

  await t.exception(clone.copyPrologue(copy, { additional: [b4a.from('d')] }))
  await t.execution(clone.copyPrologue(copy, { additional: [b4a.from('c')] }))

  t.is(clone.header.tree.length, 3)

  t.is(clone.tree.length, core.tree.length)
  t.is(clone.tree.byteLength, core.tree.byteLength)
  t.alike(clone.roots, core.roots)

  t.alike(await clone.blocks.get(0), b4a.from('a'))
  t.alike(await clone.blocks.get(1), b4a.from('b'))
  t.alike(await clone.blocks.get(2), b4a.from('c'))
})

test.skip('core - copyPrologue many', async function (t) {
  const { core } = await create(t, { compat: false, version: 1 })
  await core.append([b4a.from('a'), b4a.from('b')])

  const manifest = { ...core.header.manifest }
  manifest.prologue = { length: core.tree.length, hash: core.tree.hash() }

  const { core: copy } = await create(t, { manifest })
  const { core: copy2 } = await create(t, { manifest })
  const { core: copy3 } = await create(t, { manifest })

  await copy.copyPrologue(core)

  t.alike(copy.header.manifest.signers[0].publicKey, core.header.manifest.signers[0].publicKey)

  t.is(copy.tree.length, core.tree.length)
  t.is(copy.tree.byteLength, core.tree.byteLength)

  // copy should be independent
  await core.append([b4a.from('c')])

  // upgrade clone
  {
    const batch = core.tree.batch()
    const p = await core.tree.proof({ upgrade: { start: 0, length: 3 } })
    p.upgrade.signature = copy2.verifier.sign(batch, core.header.keyPair)
    t.ok(await copy2.verify(p))
  }

  await t.execution(copy2.copyPrologue(core))
  await t.execution(copy3.copyPrologue(core))

  t.is(copy2.tree.length, core.tree.length)
  t.is(copy.tree.length, copy3.tree.length)

  t.is(copy2.header.tree.length, core.header.tree.length)
  t.is(copy.header.tree.length, copy3.header.tree.length)

  t.is(copy2.tree.byteLength, core.tree.byteLength)
  t.is(copy.tree.byteLength, copy3.tree.byteLength)

  manifest.prologue = { length: core.tree.length, hash: core.tree.hash() }
  const { core: copy4 } = await create(t, { manifest })
  await copy4.copyPrologue(copy2)

  t.is(copy4.tree.length, 3)
  t.is(copy4.header.tree.length, 3)

  t.alike(await copy4.blocks.get(0), b4a.from('a'))
  t.alike(await copy4.blocks.get(1), b4a.from('b'))
})

async function createDb (dir, discoveryKey) {
  const db = new CoreStorage(dir)

  const storage = db.get(discoveryKey)
  if (!await storage.open()) await storage.create({})

  return storage
}

async function create (t, dir, opts) {
  if (!opts && typeof dir === 'object') {
    opts = dir
    dir = null
  }

  const storage = new Map()

  if (!dir) dir = await createTempDir(t)
  const dkey = b4a.alloc(32)

  const createFile = async (name) => {
    if (!storage.has('db')) {
      const db = await createDb(dir, dkey)
      storage.set('db', db)
    }

    if (storage.has(name)) return storage.get(name)

    const s = RAM.reusable()()
    storage.set(name, s)
    return s
  }

  const close = async () => {
    await core.tree.close()
    await storage.get('db').close()
    storage.delete('db')
  }

  const reopen = async () => {
    if (storage.has('db')) await close()
    return Core.open(createFile, opts)
  }

  const core = await reopen()
  return { core, reopen }
}

function getBlock (core, index) {
  const r = core.storage.createReadBatch()
  const p = core.blocks.get(r, 0, index)
  r.flush()

  return p
}
