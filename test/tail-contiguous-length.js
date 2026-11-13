const test = require('brittle')
const { create, createStored, replicate, unreplicate, eventFlush } = require('./helpers')

function trackScans(core) {
  const bitfield = core.core.bitfield
  const scans = []

  for (const method of ['lastUnset', 'firstUnset']) {
    const original = bitfield[method].bind(bitfield)
    bitfield[method] = function (position) {
      scans.push({ method, position })
      return original(position)
    }
  }

  return scans
}

test('tailContiguousLength - repeated reads and sessions reuse the first scan', async function (t) {
  const core = await create(t)

  await core.append(['a', 'b', 'c', 'd', 'e'])

  const scans = trackScans(core)

  t.is(core.tailContiguousLength, 5)
  t.alike(scans, [{ method: 'lastUnset', position: 4 }])

  t.is(core.tailContiguousLength, 5)
  t.is(core.tailContiguousLength, 5)
  t.is(scans.length, 1, 'repeated reads do not rescan the tail')

  const named = core.session({ name: 'reader' })
  await named.ready()

  const snap = core.snapshot()
  await snap.ready()

  t.is(named.tailContiguousLength, 5)
  t.is(snap.tailContiguousLength, 5)
  t.is(scans.length, 1, 'sessions share the core cache')

  await named.close()
  await snap.close()
})

test('tailContiguousLength - downloads only scan before the written tail', async function (t) {
  const a = await create(t)
  await a.append(['0', '1', '2', '3', '4', '5', '6', '7'])

  const b = await create(t, a.key)
  replicate(a, b, t)

  await b.download({ start: 5, end: 8 }).done()

  const scans = trackScans(b)
  t.is(b.tailContiguousLength, 3)
  t.alike(scans, [{ method: 'lastUnset', position: 7 }])

  t.is(b.tailContiguousLength, 3)
  t.is(scans.length, 1, 'repeated reads reuse the tail scan')

  const downloads = []
  b.on('download', (index) => downloads.push([index, b.tailContiguousLength]))

  await b.get(1)
  await b.get(2)
  t.alike(
    downloads,
    [
      [1, 3],
      [2, 3]
    ],
    'real earlier blocks downloaded'
  )
  t.is(b.tailContiguousLength, 3, 'an unrelated earlier download leaves the tail cached')
  t.is(scans.length, 1, 'unrelated earlier blocks do not scan the known tail')

  downloads.length = 0
  await b.get(3)
  await b.get(4)
  t.alike(
    downloads,
    [
      [3, 3],
      [4, 7]
    ],
    'real boundary blocks downloaded'
  )
  t.is(b.tailContiguousLength, 7, 'the tail now reaches block 1, block 0 is still missing')
  t.alike(
    scans.slice(1),
    [{ method: 'lastUnset', position: 3 }],
    'only the join at the cached boundary scans'
  )
})

test('tailContiguousLength - a missing head retains the known tail across growth', async function (t) {
  const a = await create(t)
  await a.append(['0', '1', '2', '3', '4'])

  const b = await create(t, a.key)
  replicate(a, b, t)
  await b.download({ start: 0, end: 5 }).done()

  t.is(b.tailContiguousLength, 5)

  const scans = trackScans(b)

  await a.append('5')
  await b.update({ wait: true })

  t.is(b.length, 6)
  t.is(b.tailContiguousLength, 0)
  t.is(b.tailContiguousLength, 0)
  t.is(scans.length, 0, 'a missing newest block does not rescan the retained tail')

  const downloads = []
  b.on('download', (index) => downloads.push([index, b.tailContiguousLength]))
  await b.get(5)

  t.alike(downloads, [[5, 6]])
  t.is(b.tailContiguousLength, 6, 'the downloaded head extends the retained run')
  t.is(scans.length, 0, 'the retained tail suffix is not rescanned')
})

test('tailContiguousLength - descending head downloads close the gap once', async function (t) {
  const a = await create(t)
  await a.append(['0', '1', '2', '3', '4'])

  const b = await create(t, a.key)
  replicate(a, b, t)
  await b.download({ start: 0, end: 5 }).done()

  t.is(b.tailContiguousLength, 5)

  const scans = trackScans(b)

  await a.append('5')
  await b.update({ wait: true })
  await a.append('6')
  await b.update({ wait: true })
  await a.append('7')
  await b.update({ wait: true })

  t.is(b.length, 8)
  t.is(b.tailContiguousLength, 0)
  t.is(b.tailContiguousLength, 0)
  t.is(scans.length, 0, 'repeated head-only growth keeps the old tail cached')

  const downloads = []
  b.on('download', (index) => downloads.push([index, b.tailContiguousLength]))

  await b.get(7)
  t.is(b.tailContiguousLength, 1)

  await b.get(6)
  t.is(b.tailContiguousLength, 2)

  await b.get(5)
  t.is(b.tailContiguousLength, 8)

  t.alike(downloads, [
    [7, 1],
    [6, 2],
    [5, 8]
  ])
  t.alike(scans, [
    { method: 'firstUnset', position: 5 },
    { method: 'lastUnset', position: 7 },
    { method: 'lastUnset', position: 5 },
    { method: 'lastUnset', position: 4 }
  ])

  t.is(b.tailContiguousLength, 8)
  t.is(scans.length, 4, 'repeated reads stay cached after the gap closes')
})

test('tailContiguousLength - in-place writes update the cached tail', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c', 'd', 'e'])

  t.is(core.tailContiguousLength, 5)

  const scans = trackScans(core)
  const appendTails = []
  core.once('append', () => appendTails.push(core.tailContiguousLength))

  await core.append(['f', 'g'])
  t.alike(appendTails, [7], 'the append event sees the extended tail')
  t.is(core.tailContiguousLength, 7)
  t.is(scans.length, 0, 'an ordinary append extends the cache without scanning')

  await core.clear(0, 2)
  t.is(core.tailContiguousLength, 5, 'clearing a prefix does not affect the tail')
  t.is(scans.length, 0)

  await core.clear(5, 6)
  t.is(core.tailContiguousLength, 1, 'clearing inside the tail keeps the right remainder')
  t.is(scans.length, 0)

  await core.clear(6)
  t.is(core.tailContiguousLength, 0, 'clearing the last block empties the tail')
  t.is(scans.length, 0)

  await core.append(['h'])
  t.is(core.tailContiguousLength, 1, 'appending after a tail hole restarts the run')
  t.is(scans.length, 0)
})

test('tailContiguousLength - truncation then sparse regrowth without a read', async function (t) {
  const a = await create(t)
  await a.append(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'])

  const b = await create(t, { keyPair: a.keyPair })
  const streams = replicate(a, b, t)
  await b.download({ start: 0, end: 10 }).done()

  t.is(b.tailContiguousLength, 10)

  await unreplicate(streams)

  await b.truncate(5)
  await a.truncate(5)
  await a.append(['5', '6', '7', '8', '9'])

  t.is(a.length, 10)
  t.is(b.length, 5)

  const appended = new Promise((resolve) => b.once('append', resolve))
  replicate(a, b, t)
  await appended
  await eventFlush()

  t.is(b.length, 10)
  t.is(b.tailContiguousLength, 0, 'a sparse regrowth does not resurrect truncated blocks')

  await b.download({ start: 5, end: 10 }).done()
  t.is(b.tailContiguousLength, 10, 'downloaded regrown blocks restore the tail')
})

test('tailContiguousLength - equal length reorg clears the replaced tail', async function (t) {
  const a = await create(t)
  await a.append(['a', 'b', 'c', 'd', 'e', 'f'])

  const b = await create(t, a.key)
  const streams = replicate(a, b, t)
  await b.download({ start: 0, end: 6 }).done()

  t.is(b.tailContiguousLength, 6)

  await unreplicate(streams)

  await a.truncate(3)
  await a.append(['d2', 'e2', 'f2'])

  t.is(a.length, 6)
  t.is(a.fork, 1)

  const truncated = new Promise((resolve) => b.once('truncate', resolve))
  replicate(a, b, t)
  await truncated
  await eventFlush()

  t.is(b.length, 6)
  t.is(b.tailContiguousLength, 0, 'replaced blocks are gone even though the length is unchanged')

  await b.download({ start: 3, end: 6 }).done()
  t.is(b.tailContiguousLength, 6)
})

test('tailContiguousLength - atomic commits keep the shared default semantics', async function (t) {
  const core = await create(t)
  await core.append(['a', 'b', 'c', 'd', 'e'])

  t.is(core.tailContiguousLength, 5)

  const atom = core.state.storage.createAtom()
  const atomic = core.session({ atom })

  await atomic.truncate(3)
  t.is(atomic.tailContiguousLength, 5, 'an atomic draft keeps the shared default tail')
  t.is(core.tailContiguousLength, 5)

  const truncateTails = []
  core.once('truncate', () => truncateTails.push(core.tailContiguousLength))

  await atom.flush()
  t.alike(truncateTails, [3], 'the truncate event sees the committed tail')
  t.is(core.tailContiguousLength, 3)

  await atomic.close()

  const atom2 = core.state.storage.createAtom()
  const atomic2 = core.session({ atom: atom2 })

  await atomic2.append('f')
  t.is(core.tailContiguousLength, 3, 'a draft append is not visible on the default core')

  const appendTails = []
  core.once('append', () => appendTails.push(core.tailContiguousLength))

  await atom2.flush()
  t.alike(appendTails, [4], 'the append event sees the committed tail')
  t.is(core.tailContiguousLength, 4)

  await atomic2.close()
})

test('tailContiguousLength - a reopened sparse core scans once then reuses', async function (t) {
  const createCore = await createStored(t)

  const a = await createCore()
  await a.ready()
  await a.append(['0', '1', '2', '3', '4', '5'])
  await a.clear(1)
  await a.close()

  const b = await createCore()
  await b.ready()
  t.teardown(() => b.close())

  const scans = trackScans(b)

  t.is(b.tailContiguousLength, 4)
  t.alike(scans, [{ method: 'lastUnset', position: 5 }])

  t.is(b.tailContiguousLength, 4)
  t.is(scans.length, 1, 'a reopened sparse core reuses its first scan')
})
