const test = require('brittle')
const b4a = require('b4a')
const createTempDir = require('test-tmp')
const CoreStorage = require('hypercore-storage')
const { MerkleTree } = require('../lib/merkle-tree')
const Core = require('../lib/core')

test('core - append', async function (t) {
  const { core } = await create(t)

  {
    const info = await core.state.append([
      b4a.from('hello'),
      b4a.from('world')
    ])

    t.alike(info, { length: 2, byteLength: 10 })
    t.is(core.state.length, 2)
    t.is(core.state.byteLength, 10)
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
    t.is(core.state.length, 3)
    t.is(core.state.byteLength, 13)
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

  t.is(core.state.lastTruncation, null)

  await core.state.truncate(3, 1)

  t.is(core.state.lastTruncation.from, 4)
  t.is(core.state.lastTruncation.to, 3)

  t.is(core.state.length, 3)
  t.is(core.state.byteLength, 12)
  t.is(core.state.fork, 1)

  await core.state.append([
    b4a.from('a'),
    b4a.from('b'),
    b4a.from('c'),
    b4a.from('d')
  ])

  await core.state.truncate(3, 2)

  t.is(core.state.lastTruncation.from, 7)
  t.is(core.state.lastTruncation.to, 3)

  t.is(core.state.length, 3)
  t.is(core.state.byteLength, 12)
  t.is(core.state.fork, 2)

  await core.state.truncate(2, 3)
  t.is(core.state.lastTruncation.from, 3)
  t.is(core.state.lastTruncation.to, 2)

  await core.state.append([b4a.from('a')])
  t.is(core.state.lastTruncation, null)

  await core.state.truncate(2, 4)
  t.is(core.state.lastTruncation.from, 3)
  t.is(core.state.lastTruncation.to, 2)

  await core.state.append([b4a.from('a')])
  t.is(core.state.lastTruncation, null)

  await core.state.truncate(2, 5)
  t.is(core.state.lastTruncation.from, 3)
  t.is(core.state.lastTruncation.to, 2)

  await core.state.append([b4a.from('a')])
  t.is(core.state.lastTruncation, null)

  await core.state.truncate(2, 6)
  t.is(core.state.lastTruncation.from, 3)
  t.is(core.state.lastTruncation.to, 2)

  await core.state.append([b4a.from('a')])
  t.is(core.state.lastTruncation, null)

  await core.state.truncate(2, 7)
  t.is(core.state.lastTruncation.from, 3)
  t.is(core.state.lastTruncation.to, 2)

  // check that it was persisted
  const coreReopen = await reopen()

  t.is(coreReopen.state.length, 2)
  t.is(coreReopen.state.byteLength, 10)
  t.is(coreReopen.state.fork, 7)
  t.is(coreReopen.state.lastTruncation, null)
  // t.is(coreReopen.header.hints.reorgs.length, 4)
})

test('core - user data', async function (t) {
  const { core, reopen } = await create(t)

  await putUserData(core.storage, 'hello', b4a.from('world'))

  for await (const { key, value } of core.createUserDataStream()) {
    t.alike(key, 'hello')
    t.alike(value, b4a.from('world'))
  }

  t.is(await countEntries(core.createUserDataStream({ gte: 'x', lt: 'z' })), 0)

  await putUserData(core.storage, 'hej', b4a.from('verden'))

  t.is(await countEntries(core.createUserDataStream()), 2)

  for await (const { key, value } of core.createUserDataStream({ gte: 'hello' })) {
    t.alike(key, 'hello')
    t.alike(value, b4a.from('world'))
  }

  await putUserData(core.storage, 'hello', null)

  t.is(await countEntries(core.createUserDataStream()), 1)
  t.is(await countEntries(core.createUserDataStream({ gte: 'hello' })), 0)

  await putUserData(core.storage, 'hej', b4a.from('world'))

  // check that it was persisted
  const coreReopen = await reopen()

  for await (const { key, value } of coreReopen.createUserDataStream()) {
    t.alike(key, 'hej')
    t.alike(value, b4a.from('world'))
  }

  t.is(await countEntries(coreReopen.createUserDataStream({ gte: 'hello' })), 0)

  function putUserData (storage, key, value) {
    const tx = storage.write()
    tx.putUserData(key, value)
    return tx.flush()
  }

  async function countEntries (stream) {
    let count = 0
    // eslint-disable-next-line no-unused-vars
    for await (const entry of stream) count++
    return count
  }
})

test('core - header does not retain slabs', async function (t) {
  const { core, reopen } = await create(t)

  t.is(core.header.key.buffer.byteLength, 32, 'unslabbed key')
  t.is(core.header.keyPair.publicKey.buffer.byteLength, 32, 'unslabbed public key')
  t.is(core.header.keyPair.secretKey.buffer.byteLength, 64, 'unslabbed private key')
  t.is(core.header.manifest.signers[0].namespace.buffer.byteLength, 32, 'unslabbed signers namespace')
  t.is(core.header.manifest.signers[0].publicKey.buffer.byteLength, 32, 'unslabbed signers publicKey')

  // check the different code path when re-opening
  const coreReopen = await reopen()

  t.is(coreReopen.header.key.buffer.byteLength, 32, 'reopen unslabbed key')
  t.is(coreReopen.header.keyPair.publicKey.buffer.byteLength, 32, 'reopen unslabbed public key')
  t.is(coreReopen.header.keyPair.secretKey.buffer.byteLength, 64, 'reopen unslabbed secret key')
  t.is(coreReopen.header.manifest.signers[0].namespace.buffer.byteLength, 32, 'reopen unslabbed signers namespace')
  t.is(coreReopen.header.manifest.signers[0].publicKey.buffer.byteLength, 32, 'reopen unslabbed signers publicKey')

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
    const nodes = await MerkleTree.missingNodes(clone.state, 2, clone.state.length)
    const p = await getProof(core, { block: { index: 1, nodes, value: true } })
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

  await core.state.append([
    b4a.from('hello'),
    b4a.from('world')
  ])

  const manifest = { prologue: { hash: await core.state.hash(), length: core.state.length } }
  const { core: copy } = (await create(t, { manifest }))

  await copy.copyPrologue(core.state)

  t.alike([
    await getBlock(copy, 0),
    await getBlock(copy, 1)
  ], [
    b4a.from('hello'),
    b4a.from('world')
  ])

  const signature = copy.state.signature
  const roots = copy.state.roots.map(r => r.index)

  for (let i = 0; i <= core.state.length * 2; i++) {
    t.alike(
      await MerkleTree.get(copy.state, i, false),
      await MerkleTree.get(core.state, i, false)
    )
  }

  await core.state.append([b4a.from('c')])

  // copy should be independent
  t.alike(copy.state.signature, signature)
  t.alike(copy.state.roots.map(r => r.index), roots)
  t.is(copy.header.hints.contiguousLength, 2)
})

test('core - clone verify', async function (t) {
  const { core } = await create(t)

  await core.state.append([b4a.from('a'), b4a.from('b')])

  const manifest = { prologue: { hash: await core.state.hash(), length: core.state.length } }
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
    const nodes = await MerkleTree.missingNodes(clone.state, 2, clone.state.length)
    const p = await getProof(copy, { block: { index: 1, nodes, value: true } })
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

  const manifest = { prologue: { hash: await core.state.hash(), length: core.state.length } }

  await core.state.append([b4a.from('2')])
  await core.state.append([b4a.from('3')])

  const { core: copy } = (await create(t, { manifest }))

  await copy.copyPrologue(core.state)

  t.is(core.state.length, 4)
  t.is(copy.state.length, 2)

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
  manifest.prologue = { length: core.state.length, hash: core.state.hash() }

  const { core: copy } = await create(t, { manifest })
  const { core: copy2 } = await create(t, { manifest })
  const { core: copy3 } = await create(t, { manifest })

  await copy.copyPrologue(core.state)

  t.alike(copy.header.manifest.signers[0].publicKey, core.header.manifest.signers[0].publicKey)

  t.is(copy.state.length, core.state.length)
  t.is(copy.state.byteLength, core.state.byteLength)

  // copy should be independent
  await core.state.append([b4a.from('c')])

  // upgrade clone
  {
    const batch = core.state.createTreeBatch()
    const p = await getProof(core, { upgrade: { start: 0, length: 3 } })
    p.upgrade.signature = copy2.verifier.sign(batch, core.header.keyPair)
    t.ok(await copy2.verify(p))
  }

  await t.execution(copy2.copyPrologue(core.state))
  await t.execution(copy3.copyPrologue(core.state))

  t.is(copy2.state.length, core.state.length)
  t.is(copy.state.length, copy3.state.length)

  t.is(copy2.header.tree.length, core.header.tree.length)
  t.is(copy.header.tree.length, copy3.header.tree.length)

  t.is(copy2.state.byteLength, core.state.byteLength)
  t.is(copy.state.byteLength, copy3.state.byteLength)

  manifest.prologue = { length: core.state.length, hash: core.state.hash() }
  const { core: copy4 } = await create(t, { manifest })
  await copy4.copyPrologue(copy2.state)

  t.is(copy4.state.length, 3)
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

  let db = null

  t.teardown(teardown, { order: 1 })

  const reopen = async () => {
    if (db) await db.close()

    db = new CoreStorage(dir)

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
  const r = core.storage.read()
  const p = r.getBlock(i)
  r.tryFlush()
  return p
}

async function getProof (core, req) {
  const batch = core.storage.read()
  const p = await MerkleTree.proof(core.state, batch, req)
  const block = req.block ? batch.getBlock(req.block.index) : null
  batch.tryFlush()
  const proof = await p.settle()
  if (block) proof.block.value = await block
  return proof
}

function getCoreHead (storage) {
  const b = storage.read()
  const p = b.getHead()
  b.tryFlush()
  return p
}
