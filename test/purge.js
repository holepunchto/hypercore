const test = require('brittle')

const Hypercore = require('../')
const { createStorage, toArray } = require('./helpers')

test('basic purge', async function (t) {
  const dir = await t.tmp()
  const storage = await createStorage(t, dir)
  const core = new Hypercore(storage)

  await core.append(['a', 'b', 'c'])

  // Sanity check for core having data
  t.is(core.length, 3)

  const discoveryKey = core.discoveryKey

  await core.purge()

  const reopenedStorage = await createStorage(t, dir)
  const coreStorage = await reopenedStorage.resumeCore(discoveryKey)

  const allBlocks = await toArray(coreStorage.createBlockStream())
  t.is(allBlocks.length, 0)

  const allTreeNodes = await toArray(coreStorage.createTreeNodeStream())
  t.is(allTreeNodes.length, 0)

  const allBitfieldPages = await toArray(coreStorage.createBitfieldStream())
  t.is(allBitfieldPages.length, 0)
})

test('purge closes all sessions', async function (t) {
  const dir = await t.tmp()
  const core = new Hypercore(dir)
  await core.append(['a', 'b', 'c'])
  const otherSession = core.session()
  await otherSession.ready()

  await core.purge()

  t.is(core.closed, true)
  t.is(otherSession.closed, true)
})
