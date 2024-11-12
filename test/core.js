const test = require('brittle')
const b4a = require('b4a')
const createTempDir = require('test-tmp')
const CoreStorage = require('hypercore-storage')
const Core = require('../lib/core')

test('core - append', async function (t) {
  const { core } = await create(t)

  {
    const info = await core.state.append([
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
    const info = await core.state.append([
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

  await core.state.append([
    b4a.from('hello'),
    b4a.from('world'),
    b4a.from('fo'),
    b4a.from('ooo')
  ])

  await core.state.truncate(3, 1)

  t.is(core.tree.length, 3)
  t.is(core.tree.byteLength, 12)
  t.is(core.tree.fork, 1)
  t.alike(core.header.hints.reorgs, [{ from: 0, to: 1, ancestors: 3 }])

  await core.state.append([
    b4a.from('a'),
    b4a.from('b'),
    b4a.from('c'),
    b4a.from('d')
  ])

  await core.state.truncate(3, 2)

  t.is(core.tree.length, 3)
  t.is(core.tree.byteLength, 12)
  t.is(core.tree.fork, 2)
  t.alike(core.header.hints.reorgs, [{ from: 0, to: 1, ancestors: 3 }, { from: 1, to: 2, ancestors: 3 }])

  await core.state.truncate(2, 3)

  t.alike(core.header.hints.reorgs, [{ from: 2, to: 3, ancestors: 2 }])

  await core.state.append([b4a.from('a')])
  await core.state.truncate(2, 4)

  await core.state.append([b4a.from('a')])
  await core.state.truncate(2, 5)

  await core.state.append([b4a.from('a')])
  await core.state.truncate(2, 6)

  await core.state.append([b4a.from('a')])
  await core.state.truncate(2, 7)

  t.is(core.header.hints.reorgs.length, 4)

  // check that it was persisted
  const coreReopen = await reopen()

  t.is(coreReopen.tree.length, 2)
  t.is(coreReopen.tree.byteLength, 10)
  t.is(coreReopen.tree.fork, 7)
  // t.is(coreReopen.header.hints.reorgs.length, 4)
})

test('core - user data', async function (t) {
  const { core, reopen } = await create(t)

  await setUserData(core, 'hello', b4a.from('world'))
  t.alike(await getUserData(core.storage, 'hello'), b4a.from('world'))

  await setUserData(core, 'hej', b4a.from('verden'))
  t.alike(await getUserData(core.storage, 'hello'), b4a.from('world'))
  t.alike(await getUserData(core.storage, 'hej'), b4a.from('verden'))

  await setUserData(core, 'hello', null)
  t.alike(await getUserData(core.storage, 'hello'), null)
  t.alike(await getUserData(core.storage, 'hej'), b4a.from('verden'))

  await setUserData(core, 'hej', b4a.from('world'))
  t.alike(await getUserData(core.storage, 'hej'), b4a.from('world'))

  // check that it was persisted
  const coreReopen = await reopen()

  t.alike(await getUserData(coreReopen.storage, 'hej'), b4a.from('world'))

  function getUserData (storage, key) {
    const b = storage.createReadBatch()
    const p = b.getUserData(key)
    b.tryFlush()
    return p
  }
})

test('core - header does not retain slabs', async function (t) {
  const { core, reopen } = await create(t)
  await setUserData(core, 'hello', b4a.from('world'))

  t.is(core.header.key.buffer.byteLength, 32, 'unslabbed key')
  t.is(core.header.keyPair.publicKey.buffer.byteLength, 32, 'unslabbed public key')
  t.is(core.header.keyPair.secretKey.buffer.byteLength, 64, 'unslabbed private key')
  t.is(core.header.manifest.signers[0].namespace.buffer.byteLength, 32, 'unslabbed signers namespace')
  t.is(core.header.manifest.signers[0].publicKey.buffer.byteLength, 32, 'unslabbed signers publicKey')

  t.is(core.header.userData[0].value.buffer.byteLength, 5, 'unslabbed the userdata value')

  // check the different code path when re-opening
  const coreReopen = await reopen()

  t.is(coreReopen.header.key.buffer.byteLength, 32, 'reopen unslabbed key')
  t.is(coreReopen.header.keyPair.publicKey.buffer.byteLength, 32, 'reopen unslabbed public key')
  t.is(coreReopen.header.keyPair.secretKey.buffer.byteLength, 64, 'reopen unslabbed secret key')
  t.is(coreReopen.header.manifest.signers[0].namespace.buffer.byteLength, 32, 'reopen unslabbed signers namespace')
  t.is(coreReopen.header.manifest.signers[0].publicKey.buffer.byteLength, 32, 'reopen unslabbed signers publicKey')

  t.is(coreReopen.header.userData[0].value.buffer.byteLength, 5, 'reopen unslabbed the userdata value')

  await coreReopen.close()
})

test('core - verify', async function (t) {
  const { core } = await create(t)
  const { core: clone } = await create(t, { keyPair: { publicKey: core.header.keyPair.publicKey } })

  t.is(clone.header.keyPair.publicKey, core.header.keyPair.publicKey)

  await core.state.append([b4a.from('a'), b4a.from('b')])

  {
    const p = await getProof(core, { upgrade: { start: 0, length: 2 } })
    await clone.verify(p)
  }

  const tree1 = await getCoreHead(core.storage)
  const tree2 = await getCoreHead(clone.storage)

  t.is(tree1.length, 2)
  t.alike(tree1.signature, tree2.signature)

  {
    const p = await getProof(core, { block: { index: 1, nodes: await clone.tree.nodes(2), value: true } })
    await clone.verify(p)
  }
})

test('core - verify parallel upgrades', async function (t) {
  const { core } = await create(t)
  const { core: clone } = await create(t, { keyPair: { publicKey: core.header.keyPair.publicKey } })

  t.is(clone.header.keyPair.publicKey, core.header.keyPair.publicKey)

  await core.state.append([b4a.from('a'), b4a.from('b'), b4a.from('c'), b4a.from('d')])

  {
    const p1 = await getProof(core, { upgrade: { start: 0, length: 2 } })
    const p2 = await getProof(core, { upgrade: { start: 0, length: 3 } })

    const v1 = clone.verify(p1)
    const v2 = clone.verify(p2)

    await v1
    await v2
  }

  const tree1 = await getCoreHead(core.storage)
  const tree2 = await getCoreHead(clone.storage)

  t.is(tree2.length, tree1.length)
  t.alike(tree2.signature, tree1.signature)
})

test('core - clone', async function (t) {
  const { core } = await create(t)

  await setUserData(core, 'hello', b4a.from('world'))

  await core.state.append([
    b4a.from('hello'),
    b4a.from('world')
  ])

  const manifest = { prologue: { hash: await core.tree.hash(), length: core.tree.length } }
  const { core: copy } = (await create(t, { manifest }))

  await copy.copyPrologue(core.state)

  const userData = []
  const str = copy.storage.createUserDataStream()
  for await (const { key, value } of str) userData.push({ key, value })

  t.alike(userData, [{ key: 'hello', value: b4a.from('world') }])

  t.alike([
    await getBlock(copy, 0),
    await getBlock(copy, 1)
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

  await core.state.append([b4a.from('c')])

  // copy should be independent
  t.alike(copy.tree.signature, signature)
  t.alike(copy.tree.roots.map(r => r.index), roots)
  t.is(copy.header.hints.contiguousLength, 2)
})

test('core - clone verify', async function (t) {
  const { core } = await create(t)

  await core.state.append([b4a.from('a'), b4a.from('b')])

  const manifest = { prologue: { hash: await core.tree.hash(), length: core.tree.length } }
  const { core: copy } = await create(t, { manifest })
  const { core: clone } = await create(t, { manifest })

  await copy.copyPrologue(core.state)

  // copy should be independent
  await core.state.append([b4a.from('c')])

  {
    const p = await getProof(copy, { upgrade: { start: 0, length: 2 } })
    t.ok(await clone.verify(p))
  }

  t.is(clone.header.tree.length, 2)

  {
    const p = await getProof(copy, { block: { index: 1, nodes: await clone.tree.nodes(2), value: true } })
    p.block.value = await getBlock(copy, 1)
    await clone.verify(p)
  }

  t.is(core.header.hints.contiguousLength, 3)
  t.is(copy.header.hints.contiguousLength, 2)
  t.is(clone.header.hints.contiguousLength, 0)

  t.pass('verified')
})

test('core - partial clone', async function (t) {
  const { core } = await create(t)

  await core.state.append([b4a.from('0')])
  await core.state.append([b4a.from('1')])

  const manifest = { prologue: { hash: await core.tree.hash(), length: core.tree.length } }

  await core.state.append([b4a.from('2')])
  await core.state.append([b4a.from('3')])

  const { core: copy } = (await create(t, { manifest }))

  await copy.copyPrologue(core.state)

  t.is(core.tree.length, 4)
  t.is(copy.tree.length, 2)

  t.is(core.header.hints.contiguousLength, 4)
  t.is(copy.header.hints.contiguousLength, 2)

  t.alike([
    await getBlock(copy, 0),
    await getBlock(copy, 1),
    await getBlock(copy, 2)
  ], [
    b4a.from('0'),
    b4a.from('1'),
    null
  ])
})

test('core - copyPrologue bails if core is not the same', async function (t) {
  const { core } = await create(t)
  const { core: copy } = await create(t, { manifest: { prologue: { hash: b4a.alloc(32), length: 1 } } })

  // copy should be independent
  await core.state.append([b4a.from('a')])

  await t.exception(copy.copyPrologue(core.state))

  t.is(copy.header.hints.contiguousLength, 0)
})

test('core - copyPrologue many', async function (t) {
  const { core } = await create(t, { compat: false, version: 1 })
  await core.state.append([b4a.from('a'), b4a.from('b')])

  const manifest = { ...core.header.manifest }
  manifest.prologue = { length: core.tree.length, hash: core.tree.hash() }

  const { core: copy } = await create(t, { manifest })
  const { core: copy2 } = await create(t, { manifest })
  const { core: copy3 } = await create(t, { manifest })

  await copy.copyPrologue(core.state)

  t.alike(copy.header.manifest.signers[0].publicKey, core.header.manifest.signers[0].publicKey)

  t.is(copy.tree.length, core.tree.length)
  t.is(copy.tree.byteLength, core.tree.byteLength)

  // copy should be independent
  await core.state.append([b4a.from('c')])

  // upgrade clone
  {
    const batch = core.tree.batch()
    const p = await getProof(core, { upgrade: { start: 0, length: 3 } })
    p.upgrade.signature = copy2.verifier.sign(batch, core.header.keyPair)
    t.ok(await copy2.verify(p))
  }

  await t.execution(copy2.copyPrologue(core.state))
  await t.execution(copy3.copyPrologue(core.state))

  t.is(copy2.tree.length, core.tree.length)
  t.is(copy.tree.length, copy3.tree.length)

  t.is(copy2.header.tree.length, core.header.tree.length)
  t.is(copy.header.tree.length, copy3.header.tree.length)

  t.is(copy2.tree.byteLength, core.tree.byteLength)
  t.is(copy.tree.byteLength, copy3.tree.byteLength)

  manifest.prologue = { length: core.tree.length, hash: core.tree.hash() }
  const { core: copy4 } = await create(t, { manifest })
  await copy4.copyPrologue(copy2.state)

  t.is(copy4.tree.length, 3)
  t.is(copy4.header.tree.length, 3)

  t.is(core.header.hints.contiguousLength, 3)
  t.is(copy.header.hints.contiguousLength, 2)
  t.is(copy2.header.hints.contiguousLength, 2)
  t.is(copy3.header.hints.contiguousLength, 2)
  t.is(copy4.header.hints.contiguousLength, 2)

  t.alike(await getBlock(copy4, 0), b4a.from('a'))
  t.alike(await getBlock(copy4, 1), b4a.from('b'))
})

async function create (t, opts = {}) {
  const dir = opts.dir || await createTempDir(t)

  const dkey = b4a.alloc(32, 1)
  let db = null

  t.teardown(teardown, { order: 1 })

  const reopen = async () => {
    if (db) await db.close()

    db = new CoreStorage(dir)

    if (!opts.discoveryKey) opts.discoveryKey = dkey

    const core = new Core(db, opts)
    await core.ready()
    t.teardown(() => core.close())
    return core
  }

  const core = await reopen()

  return { core, reopen }

  async function teardown () {
    if (db) await db.close()
  }
}

async function getBlock (core, i) {
  const r = core.storage.createReadBatch()
  const p = core.blocks.get(r, i)
  await r.flush()
  return p
}

async function setUserData (core, key, value) {
  return core.userData(key, value)
}

async function getProof (core, req) {
  const batch = core.storage.createReadBatch()
  const p = await core.tree.proof(batch, req)
  const block = req.block ? core.blocks.get(batch, req.block.index) : null
  batch.tryFlush()
  const proof = await p.settle()
  if (block) proof.block.value = await block
  return proof
}

function getCoreHead (storage) {
  const b = storage.createReadBatch()
  const p = b.getCoreHead()
  b.tryFlush()
  return p
}
