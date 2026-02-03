const test = require('brittle')
const flat = require('flat-tree')
const Hypercore = require('../index.js')
const { createStorage } = require('./helpers/index.js')

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
