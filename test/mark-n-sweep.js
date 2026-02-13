const test = require('brittle')
const { create, createStorage } = require('./helpers')

const Hypercore = require('../')

test('startMarking - basic', async (t) => {
  const core = await create(t)

  const num = 10_000
  for (let i = 0; i < num; i++) {
    await core.append('i' + i)
  }

  await core.get(42)
  for await (const mark of core.state.storage.createMarkStream()) {
    t.fail('found a mark!')
  }

  t.absent(core._marking, 'not enabled by default')
  await core.startMarking()
  t.ok(core._marking, 'enabled after startMarking')
  for await (const mark of core.state.storage.createMarkStream()) {
    t.fail('found a mark!')
  }

  // Get 2^n
  let getI = 0
  let gets = []
  while (getI < num) {
    const index = core.length - getI - 1
    gets.push(core.get(index))
    getI = getI === 0 ? 1 : getI * 2
  }

  await Promise.all(gets)
  t.comment('gets made')

  t.is(core.contiguousLength, num, 'contiguous before sweep')
  await core.sweep()
  t.is(core.contiguousLength, 0, 'non-contig')
  t.absent(core._marking, 'auto disables marking')

  // Re-get w/ wait false to ensure exists
  getI = 0
  gets = []
  while (getI < num) {
    const index = core.length - getI - 1
    gets.push(core.get(index, { wait: false }))
    getI = getI === 0 ? 1 : getI * 2
  }

  const markedGets = await Promise.all(gets)
  t.absent(markedGets.some((v) => v === null), 'marked all exist')

  // Other indexes fail
  getI = 3
  gets = []
  while (getI < num) {
    const index = core.length - getI - 1
    gets.push(core.get(index, { wait: false }))
    getI *= 3
  }

  const clearedGets = await Promise.all(gets)
  t.absent(clearedGets.some((v) => v !== null), 'non-marked are cleared')
})

test.skip('startMarking - large cores', { timeout: 5 * 60 * 1000 }, async (t) => {
  const dir = await t.tmp()
  let storage = null

  let core = new Hypercore(await open())
  t.teardown(() => core.close())
  await core.ready()

  const num = 1_000_000
  for (let i = 0; i < num; i++) {
    await core.append('i' + i)
  }

  await core.close()

  // Reopen to isolate memory to marking
  core = new Hypercore(await open())
  t.teardown(() => core.close())
  await core.ready()

  t.absent(core._marking, 'not enabled by default')

  await core.startMarking()

  const totalGets = 1000
  // Get random
  let getI = 0
  let gets = []
  let getIndexes = new Set()
  while (getI < totalGets) {
    const index = Math.floor(Math.random() * core.length)
    gets.push(core.get(index))
    if (!getIndexes.has(index)) getI++
    getIndexes.add(index)
  }

  await Promise.all(gets)
  t.comment('gets made')

  t.is(core.contiguousLength, num, 'contiguous before sweep')
  await core.sweep()
  t.absent(core._marking, 'auto disables marking')

  // Re-get w/ wait false to ensure exists
  gets = []
  for (const index of getIndexes.values()) {
    gets.push(core.get(index, { wait: false }))
  }

  const markedGets = await Promise.all(gets)
  t.absent(markedGets.some((v) => v === null), 'marked all exist')

  // Other indexes fail
  for (let i = 0; i < core.length; i++) {
    if (getIndexes.has(i)) continue

    if (await core.get(i, { wait: false })) {
      t.fail('found block at ${i}')
    }
  }

  async function open() {
    if (storage) await storage.close()
    storage = await createStorage(t, dir)
    return storage
  }
})
