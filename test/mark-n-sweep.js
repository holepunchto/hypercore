const test = require('brittle')
const { create } = require('./helpers')

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
