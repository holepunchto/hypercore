const test = require('brittle')
const flat = require('flat-tree')
const { MerkleTree } = require('../lib/merkle-tree.js')
const Hypercore = require('../index.js')
const { createStorage } = require('./helpers/index.js')
const { proof, verify } = require('../lib/fully-remote-proof.js')

test('recover - bad merkle root core can still ready', async (t) => {
  const dir = await t.tmp()
  let storage = null

  const core = new Hypercore(await open())
  await core.ready()
  t.teardown(() => core.close())

  // Add content
  const num = 32
  for (let i = 0; i < num; i++) {
    await core.append('i' + i)
  }

  // Delete tree nodes
  const tx = core.core.storage.write()
  const roots = flat.fullRoots(2 * num)
  for (const root of roots) {
    tx.deleteTreeNode(root)
  }
  await tx.flush()

  await core.close()

  t.comment('closed initial')

  const core2 = new Hypercore(await open())
  await t.execution(() => core2.ready())

  await core2.close()

  async function open() {
    if (storage) await storage.close()
    storage = await createStorage(t, dir)
    return storage
  }
})

test('recover - bad merkle root - fix via fully remote proof', async (t) => {
  const dir = await t.tmp()
  let storage = null

  const core = new Hypercore(await open())
  await core.ready()
  t.teardown(() => core.close())

  // Add content
  const num = 30
  for (let i = 0; i < num; i++) {
    await core.append('i' + i)
  }

  // Delete tree nodes
  const tx = core.core.storage.write()
  const [rootIndex] = flat.fullRoots(2 * num)

  const initialHash = await core.treeHash(rootIndex) // store for later check

  // Get proof from good core, before deleting
  const p = await core.generateRemoteProofForTreeNode(rootIndex)
  t.ok(await MerkleTree.get(core.core, rootIndex))

  tx.deleteTreeNode(rootIndex)
  await tx.flush()

  // Verify tree node removed
  t.absent(await MerkleTree.get(core.core, rootIndex), 'removed tree node')

  await core.close()

  const core2 = new Hypercore(await open(), { writable: false })
  await t.execution(() => core2.ready())

  // Still no tree node
  t.absent(await MerkleTree.get(core2.core, rootIndex))

  t.is(core2.length, num, 'still has length')

  const hash = await core2.treeHash(rootIndex)
  t.alike(hash, initialHash, 'still can construct the hash')

  // Verify remote proof & patch with it's proof
  t.ok(await core2.recoverFromRemoteProof(p), 'recovery verified correctly')
  t.ok(await MerkleTree.get(core2.core, rootIndex))

  async function open() {
    if (storage) await storage.close()
    storage = await createStorage(t, dir)
    return storage
  }
})

test('recover - bad merkle sub root - fix via fully remote proof', async (t) => {
  const dir = await t.tmp()
  let storage = null

  const core = new Hypercore(await open())
  await core.ready()
  t.teardown(() => core.close())

  // Add content
  const num = 64
  for (let i = 0; i < num; i++) {
    await core.append('i' + i)
  }

  // Delete tree nodes
  const tx = core.core.storage.write()
  const indexes = flat.fullRoots(2 * num)
  let leftChild = indexes[0]
  for (let i = 0; i < 2; i++) {
    ;[leftChild, _] = flat.children(leftChild)
  }
  const targetBlockIndex = flat.rightSpan(leftChild) / 2 + 1

  const initialHash = await core.treeHash(targetBlockIndex) // store for later check
  // Get proof from good core, before deleting
  const p = await core.generateRemoteProofForTreeNode(leftChild)
  t.ok(await MerkleTree.get(core.core, leftChild))

  tx.deleteTreeNode(leftChild)
  await tx.flush()

  // Verify tree node removed
  t.absent(await MerkleTree.get(core.core, leftChild), 'removed tree node')

  await core.close()

  const core2 = new Hypercore(await open(), { writable: false })
  await t.execution(() => core2.ready())

  // Still no tree node
  t.absent(await MerkleTree.get(core2.core, leftChild))

  t.is(core2.length, num, 'still has length')

  // Verify remote proof & patch with it's proof
  t.ok(await core2.recoverFromRemoteProof(p), 'recovery verified correctly')
  t.ok(await MerkleTree.get(core2.core, leftChild))

  const hash = await core2.treeHash(targetBlockIndex)
  t.alike(hash, initialHash, 'still can construct the hash')

  async function open() {
    if (storage) await storage.close()
    storage = await createStorage(t, dir)
    return storage
  }
})
