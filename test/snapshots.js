const test = require('brittle')
const createTempDir = require('test-tmp')
const b4a = require('b4a')
const Hypercore = require('../')
const { replicate, unreplicate, create, createStorage } = require('./helpers')

test('snapshot does not change when original gets modified', async function (t) {
  const core = await create(t)

  await core.append('block0')
  await core.append('block1')
  await core.append('block2')

  const snap = core.snapshot()
  await snap.ready()

  t.is(snap.length, 3, 'correct length')
  t.is(snap.signedLength, 3, 'correct signed length')
  t.is(b4a.toString(await snap.get(2)), 'block2', 'block exists')

  await core.append('Block3')
  t.is(snap.length, 3, 'correct length')
  t.is(snap.signedLength, 3, 'correct signed length')
  t.is(b4a.toString(await snap.get(2)), 'block2', 'block exists')

  await core.truncate(3)
  t.is(snap.length, 3, 'correct length')
  t.is(snap.signedLength, 3, 'correct signed length')
  t.is(b4a.toString(await snap.get(2)), 'block2', 'block exists')

  await core.truncate(2)
  t.is(snap.length, 3, 'correct length')
  t.is(snap.signedLength, 2, 'signed length now lower since it truncated below snap')
  t.is(b4a.toString(await snap.get(2)), 'block2', 'block exists')

  await core.append('new Block2')
  t.is(snap.length, 3, 'correct length')
  t.is(snap.signedLength, 2, 'signed length remains at lowest value after appending again to the original')
  t.is(b4a.toString(await snap.get(2)), 'block2', 'Old block still (snapshot did not change)')

  {
    const res = []
    for await (const b of snap.createReadStream()) {
      res.push(b4a.toString(b))
    }
    t.alike(res, ['block0', 'block1', 'block2'])
  }

  await snap.close()
})

test('implicit snapshot - gets are snapshotted at call time', async function (t) {
  t.plan(8)

  const core = await create(t)
  const clone = await create(t, core.key, { valueEncoding: 'utf-8' })

  clone.on('truncate', function (len) {
    t.is(len, 2, 'remote truncation')
  })

  core.on('truncate', function (len) {
    t.is(len, 2, 'local truncation')
  })

  await core.append('block #0.0')
  await core.append('block #1.0')
  await core.append('block #2.0')

  const r1 = replicate(core, clone, t)

  t.is(await clone.get(0), 'block #0.0')

  await unreplicate(r1)

  const range1 = core.download({ start: 0, end: 4 })
  const range2 = clone.download({ start: 0, end: 4 })

  const p2 = clone.get(1)
  const p3 = clone.get(2)

  const exception = t.exception(p3, 'should fail cause snapshot not available')

  await core.truncate(2)

  await core.append('block #2.1')
  await core.append('block #3.1')

  replicate(core, clone, t)

  t.is(await p2, 'block #1.0')
  await exception

  t.is(await clone.get(2), 'block #2.1')

  await range1.done()
  t.pass('local range finished')

  await range2.done()
  t.pass('remote range finished')
})

test('snapshots wait for ready', async function (t) {
  t.plan(8)

  const dir = await createTempDir(t)
  const db = await createStorage(t, dir)

  const core = new Hypercore(db)
  await core.ready()

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

  await core.close()
  await s1.close()
  await s2.close()
  await db.close()

  const db2 = await createStorage(t, dir)
  const coreCopy = new Hypercore(db2)

  // if a snapshot is made on an opening core, it should wait until opened
  const s3 = coreCopy.snapshot()

  await s3.ready()

  t.is(s3.length, 4, 'waited for ready')

  const s4 = coreCopy.snapshot()
  await s4.ready()

  t.is(s4.length, 4)

  await s3.update()
  await s4.update()

  t.is(s3.length, 4, 'no changes')
  t.is(s4.length, 4, 'no changes')

  await coreCopy.close()
  await s3.close()
  await s4.close()
})

test('snapshots are consistent', async function (t) {
  t.plan(6)

  const core = await create(t)
  const clone = await create(t, core.key)

  await core.append('block #0.0')
  await core.append('block #1.0')
  await core.append('block #2.0')

  replicate(clone, core, t)

  await clone.update({ wait: true })

  const snapshot = clone.snapshot({ valueEncoding: 'utf-8' })
  await snapshot.ready()

  t.is(snapshot.length, 3)

  t.is(await snapshot.get(1), 'block #1.0')

  const promise = new Promise(resolve => clone.once('truncate', resolve))

  await core.truncate(1)
  await core.append('block #1.1')
  await core.append('block #2.1')

  // wait for clone to update
  await promise

  t.is(clone.fork, 1, 'clone updated')

  const b = snapshot.get(0)
  t.exception(snapshot.get(1))
  t.exception(snapshot.get(2))
  t.is(await b, 'block #0.0')

  await snapshot.close()
})

test('snapshot over named batch persists after truncate', async function (t) {
  t.plan(8)

  const core = await create(t)

  await core.append('block #0.0')
  await core.append('block #1.0')
  await core.append('block #2.0')

  const session = core.session({ name: 'session' })

  const snapshot = session.snapshot({ valueEncoding: 'utf-8' })
  await snapshot.ready()

  await session.close()

  t.is(snapshot.length, 3)

  t.is(await snapshot.get(1), 'block #1.0')

  await core.truncate(1)
  await core.append('block #1.1')

  t.is(core.fork, 1, 'clone updated')
  t.is(core.length, 2, 'core updated')

  // t.is(snapshot.fork, 0, 'snapshot remains')
  t.is(snapshot.length, 3, 'snapshot remains')

  t.is(await snapshot.get(0), 'block #0.0')
  t.is(await snapshot.get(1), 'block #1.0')
  t.is(await snapshot.get(2), 'block #2.0')

  await core.close()
  await snapshot.close()
})
