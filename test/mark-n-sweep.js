const test = require('brittle')
const { create, createStorage } = require('./helpers')

const Hypercore = require('../')

test('startMarking - basic', async (t) => {
  const core = await create(t)

  const num = 50_000
  for (let i = 0; i < num; i++) {
    await core.append('i' + i)
  }

  await core.get(42)
  for await (const mark of core.state.storage.createMarkStream()) {
    t.fail(`found a mark at ${mark}!`)
  }

  t.absent(core._marking, 'not enabled by default')
  await core.startMarking()
  t.ok(core._marking, 'enabled after startMarking')
  for await (const mark of core.state.storage.createMarkStream()) {
    t.fail(`found a mark at ${mark}!`)
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
  t.absent(
    markedGets.some((v) => v === null),
    'marked all exist'
  )

  // Other indexes fail
  getI = 3
  gets = []
  while (getI < num) {
    const index = core.length - getI - 1
    gets.push(core.get(index, { wait: false }))
    getI *= 3
  }

  const clearedGets = await Promise.all(gets)
  t.absent(
    clearedGets.some((v) => v !== null),
    'non-marked are cleared'
  )
})

test('startMarking - cant run 2x', async (t) => {
  const core = await create(t)

  await core.append('i0')

  await t.execution(() => core.startMarking(), '1st run works')
  await t.exception(() => core.startMarking(), /ASSERTION/, '2nd run throws')
})

test('startMarking then immediate sweep', async (t) => {
  const core = await create(t)

  await core.append('i0')

  await core.startMarking()
  await t.execution(core.sweep(), 'sweep can run')
  t.absent(await core.has(0, core.length), 'cleared all blocks')
})

test('startMarking - on session', async (t) => {
  const core = await create(t)

  const num = 50_000
  for (let i = 0; i < num; i++) {
    await core.append('i' + i)
  }

  await core.get(42)
  for await (const mark of core.state.storage.createMarkStream()) {
    t.fail(`found a mark at ${mark}!`)
  }

  const s = core.session()

  t.absent(s._marking, 'not enabled by default')
  await s.startMarking()
  t.ok(s._marking, 'enabled after startMarking')
  for await (const mark of s.state.storage.createMarkStream()) {
    t.fail(`found a mark at ${mark}!`)
  }

  // Get 2^n
  let getI = 0
  let gets = []
  while (getI < num) {
    const index = s.length - getI - 1
    gets.push(s.get(index))
    getI = getI === 0 ? 1 : getI * 2
  }

  await Promise.all(gets)
  t.comment('gets made')

  t.is(s.contiguousLength, num, 'contiguous before sweep')
  await s.sweep()
  t.is(s.contiguousLength, 0, 'non-contig')
  t.absent(s._marking, 'auto disables marking')

  // Re-get w/ wait false to ensure exists
  getI = 0
  gets = []
  while (getI < num) {
    const index = s.length - getI - 1
    gets.push(s.get(index, { wait: false }))
    getI = getI === 0 ? 1 : getI * 2
  }

  const markedGets = await Promise.all(gets)
  t.absent(
    markedGets.some((v) => v === null),
    'marked all exist'
  )

  // Other indexes fail
  getI = 3
  gets = []
  while (getI < num) {
    const index = s.length - getI - 1
    gets.push(core.get(index, { wait: false }))
    getI *= 3
  }

  const clearedGets = await Promise.all(gets)
  t.absent(
    clearedGets.some((v) => v !== null),
    'non-marked are cleared on parent'
  )

  await s.close()
  await core.close()
})

// SKIP because of issue with clearing named sessions
test.skip('startMarking - on named session', async (t) => {
  const core = await create(t)

  const num = 50_000
  for (let i = 0; i < num; i++) {
    await core.append('i' + i)
  }
  await core.truncate(num - 1)
  await core.append('i beep')

  await core.get(42)
  for await (const mark of core.state.storage.createMarkStream()) {
    t.fail('found a mark!')
  }

  // To emphasize that it wasn't cleared despite current bug
  t.ok(await core.get(49999), 'core has end block')

  const s = core.session({ name: 'batch' })

  // To emphasize that it wasn't cleared before running sweep
  t.ok(await s.get(49999, { wait: false }), 'session has end block')

  t.absent(s._marking, 'not enabled by default')
  await s.startMarking()
  t.ok(s._marking, 'enabled after startMarking')
  for await (const mark of s.state.storage.createMarkStream()) {
    t.fail('found a mark!')
  }

  // Get 2^n
  let getI = 0
  let gets = []
  const getIndexes = new Set()
  while (getI < num) {
    const index = s.length - getI - 1
    getIndexes.add(index)
    gets.push(s.get(index))
    getI = getI === 0 ? 1 : getI * 2
  }

  await Promise.all(gets)
  t.comment('gets made')

  await s.sweep()
  t.absent(s._marking, 'auto disables marking')

  // Re-get w/ wait false to ensure exists
  getI = 0
  gets = []
  const getIndexesCheck = new Set()
  while (getI < num) {
    const index = s.length - getI - 1
    getIndexesCheck.add(index)
    gets.push(s.get(index, { wait: false }))
    getI = getI === 0 ? 1 : getI * 2
  }

  const markedGets = await Promise.all(gets)
  // console.log('markedGets', markedGets) // Will return null for everything
  t.absent(
    markedGets.some((v) => v === null),
    'marked all exist'
  )
  t.alike(getIndexesCheck, getIndexes, 'checked the correct indexes')

  // Re-get w/ wait false to ensure exists
  getI = 0
  gets = []
  while (getI < num) {
    const index = s.length - getI - 1
    gets.push(core.get(index, { wait: false }))
    getI = getI === 0 ? 1 : getI * 2
  }

  const markedGets2 = await Promise.all(gets)
  t.absent(
    markedGets2.some((v) => v === null),
    'marked all exist'
  )

  // Other indexes fail
  getI = 3
  gets = []
  while (getI < num) {
    const index = s.length - getI - 1
    gets.push(s.get(index, { wait: false }))
    getI *= 3
  }

  const clearedGets = await Promise.all(gets)
  t.absent(
    clearedGets.some((v) => v !== null),
    'non-marked are cleared on session'
  )

  await s.close()
  await core.close()
})

test('startMarking - large cores', { timeout: 5 * 60 * 1000 }, async (t) => {
  const dir = await t.tmp()
  let storage = null

  let core = new Hypercore(await open())
  t.teardown(() => core.close())
  await core.ready()

  const num = 250_000
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
  const getIndexes = new Set()
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
  t.absent(
    markedGets.some((v) => v === null),
    'marked all exist'
  )

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

test('markBlock - range', async (t) => {
  const core = await create(t)

  for (let i = 0; i < 10; i++) {
    await core.append('i' + i)
  }

  await core.startMarking()

  await core.markBlock(2, 7)

  t.ok(await core.has(2), 'has start')
  t.ok(await core.has(7), 'has end index')

  await core.sweep()

  t.absent(await core.has(0, 2), 'cleared start')
  t.ok(await core.has(2, 7), 'kept range')
  t.absent(await core.has(7, core.length), 'end index (non inclusive)')
})

test('markBlock - works on snap but sweep on non-snap', async (t) => {
  const core = await create(t)

  for (let i = 0; i < 10; i++) {
    await core.append('i' + i)
  }

  const snap = core.snapshot()
  await snap.ready()

  await snap.startMarking()

  await snap.markBlock(2, 7)

  t.ok(await snap.has(2), 'has start')
  t.ok(await snap.has(7), 'has end index')

  await t.exception(snap.sweep(), /Cannot sweep a snapshot/, 'throws if calling sweep on snap')

  await t.execution(core.sweep(), 'sweep can be run on parent core')

  t.absent(await core.has(0, 2), 'cleared start')
  t.ok(await core.has(2, 7), 'kept range')
  t.absent(await core.has(7, core.length), 'end index (non inclusive)')
})
