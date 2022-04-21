const { create, createStored } = require('./helpers')
const test = require('brittle')

test('implicit snapshot - gets are snapshotted at call time', async function (t) {
  t.plan(8)

  const core = await create()
  const clone = await create(core.key, { valueEncoding: 'utf-8' })

  clone.on('truncate', function (len) {
    t.is(len, 2, 'remote truncation')
  })

  core.on('truncate', function (len) {
    t.is(len, 2, 'local truncation')
  })

  await core.append('block #0.0')
  await core.append('block #1.0')
  await core.append('block #2.0')

  const r1 = replicate(core, clone)

  t.is(await clone.get(0), 'block #0.0')

  await unreplicate(r1)

  const range1 = core.download({ start: 0, end: 4 })
  const range2 = clone.download({ start: 0, end: 4 })

  const p2 = clone.get(1)
  const p3 = clone.get(2)

  await core.truncate(2)

  await core.append('block #2.1')
  await core.append('block #3.1')

  replicate(core, clone)

  t.is(await p2, 'block #1.0')
  t.exception(p3, 'should fail cause snapshot not available')

  t.is(await clone.get(2), 'block #2.1')

  await range1.downloaded()
  t.pass('local range finished')

  await range2.downloaded()
  t.pass('remote range finished')
})

test('snapshots wait for ready', async function (t) {
  t.plan(10)

  const create = createStored()

  const core = create()
  const s1 = core.snapshot()

  await core.append('block #0.0')
  await core.append('block #1.0')

  const s2 = core.snapshot()

  await core.append('block #2.0')

  t.is(s1.length, 0, 'empty snapshot')
  t.is(s2.length, 2, 'set after ready')

  await core.append('block #3.0')

  // check that they are static
  t.is(s1.length, 0, 'is static')
  t.is(s2.length, 2, 'is static')

  await s1.update()
  await s2.update()

  // check that they can be updated
  t.is(s1.length, 4, 'explictly updated')
  t.is(s2.length, 4, 'explictly updated')

  await core.close()

  const coreCopy = create()

  // if a snapshot is made on an opening core, it should wait until opened
  const s3 = coreCopy.snapshot()

  await s3.ready()

  t.is(s3.length, 4, 'waited for ready')

  const s4 = coreCopy.snapshot()

  t.is(s4.length, 4, 'sync but opened')

  await s3.update()
  await s4.update()

  t.is(s3.length, 4, 'no changes')
  t.is(s4.length, 4, 'no changes')
})

function replicate (a, b) {
  const s1 = a.replicate(true)
  const s2 = b.replicate(false)

  s1.pipe(s2).pipe(s1)

  return [s1, s2]
}

function unreplicate (streams) {
  const ps = streams.map(s => {
    return new Promise((resolve) => {
      s.on('error', () => {})
      s.on('close', resolve)
      s.destroy()
    })
  })
  return Promise.all(ps)
}
