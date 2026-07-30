const test = require('brittle')
const b4a = require('b4a')
const Hypercore = require('..')
const { create } = require('./helpers')

test('groups - basic', async function (t) {
  const a = await create(t, { group: b4a.alloc(32, 1) })
  t.alike(a.core.header.group?.key, b4a.alloc(32, 1))
})

test('groups - persisted', async function (t) {
  const dir = await t.tmp()
  const a = new Hypercore(dir, { group: b4a.alloc(32, 1) })
  await a.ready()

  t.alike(a.core.header.group?.key, b4a.alloc(32, 1))

  await a.close()

  const b = new Hypercore(dir)
  await b.ready()

  t.alike(b.core.header.group?.key, b4a.alloc(32, 1))
  await b.close()
})

test('groups - dynamic', async function (t) {
  const dir = await t.tmp()
  const a = new Hypercore(dir)
  await a.ready()

  await a.setGroup(b4a.alloc(32, 1))
  t.alike(a.core.header.group?.key, b4a.alloc(32, 1))

  await a.close()

  const b = new Hypercore(dir)
  await b.ready()

  t.alike(b.core.header.group?.key, b4a.alloc(32, 1))
  await b.close()
})

test('groups - conflict', async function (t) {
  const dir = await t.tmp()
  const a = new Hypercore(dir)
  await a.ready()

  await a.setGroup(b4a.alloc(32, 1))
  t.alike(a.core.header.group?.key, b4a.alloc(32, 1))

  await a.close()

  const b = new Hypercore(dir)
  await t.exception(b.setGroup(b4a.alloc(32, 2)))

  await b.close()
})

test('groups - core hook - ongroupupdate()', async function (t) {
  t.plan(2)
  const a = await create(t)

  const groupKey = b4a.alloc(32, 1)

  a.core.ongroupupdate = (key) => {
    t.alike(key, groupKey, 'got group key in event')
  }

  await a.setGroup(groupKey)
  t.alike(a.core.header.group.key, b4a.alloc(32, 1), 'a has a group')

  await a.append('beep')
})

test('groups - core hook - ongroupupdate() w/head', async function (t) {
  t.plan(7)
  const a = await create(t)

  const groupKey = b4a.alloc(32, 1)
  const events = []

  a.core.ongroupupdate = (key, head) => {
    t.alike(key, groupKey, 'got group key in event')
    events.push(head)
  }

  await a.setGroup(groupKey)
  t.alike(a.core.header.group.key, b4a.alloc(32, 1), 'a has a group')

  await a.append('beep')

  t.is(events.length, 1)
  t.alike(events, [
    {
      key: a.core.key,
      length: 1,
      fork: 0
    }
  ])

  await a.append('beep')

  t.is(events.length, 2)
  t.alike(events, [
    {
      key: a.core.key,
      length: 1,
      fork: 0
    },
    {
      key: a.core.key,
      length: 2,
      fork: 0
    }
  ])
})

test('groups - core hook - ongroupupdate() w/head multiple', async function (t) {
  t.plan(7)
  const a = await create(t)
  const b = await create(t)

  const groupKey = b4a.alloc(32, 1)
  const events = []

  a.core.ongroupupdate = (key, head) => {
    t.alike(key, groupKey, 'got group key in event')
    events.push(head)
  }

  b.core.ongroupupdate = (key, head) => {
    t.alike(key, groupKey, 'got group key in event')
    events.push(head)
  }

  await a.setGroup(groupKey)
  await b.setGroup(groupKey)
  t.alike(a.core.header.group.key, b4a.alloc(32, 1), 'a has a group')
  t.alike(b.core.header.group.key, b4a.alloc(32, 1), 'b has a group')

  await a.append('beep')
  await b.append('beep')
  await a.append('beep')

  t.is(events.length, 3)
  t.alike(events, [
    {
      key: a.core.key,
      length: 1,
      fork: 0
    },
    {
      key: b.core.key,
      length: 1,
      fork: 0
    },
    {
      key: a.core.key,
      length: 2,
      fork: 0
    }
  ])
})

test('groups - core hook - ongroupupdate() w/head and fork', async function (t) {
  t.plan(10)
  const a = await create(t)

  const groupKey = b4a.alloc(32, 1)
  const events = []

  a.core.ongroupupdate = (key, head) => {
    t.alike(key, groupKey, 'got group key in event')
    events.push(head)
  }

  await a.setGroup(groupKey)
  t.alike(a.core.header.group.key, b4a.alloc(32, 1), 'a has a group')

  await a.append('beep')

  t.is(events.length, 1)
  t.alike(events, [
    {
      key: a.core.key,
      length: 1,
      fork: 0
    }
  ])

  await a.truncate(0)

  t.is(events.length, 2)
  t.alike(
    events,
    [
      {
        key: a.core.key,
        length: 1,
        fork: 0
      },
      {
        key: a.core.key,
        length: 0,
        fork: 1
      }
    ],
    'forked event'
  )

  await a.append('beep')

  t.is(events.length, 3)
  t.alike(events, [
    {
      key: a.core.key,
      length: 1,
      fork: 0
    },
    {
      key: a.core.key,
      length: 0,
      fork: 1
    },
    {
      key: a.core.key,
      length: 1,
      fork: 1
    }
  ])
})

test('groups - ongroupupdate() fires after append event & length updating', async function (t) {
  t.plan(2)
  const a = await create(t)
  const groupKey = b4a.alloc(32, 1)

  let appendFired = false

  a.on('append', function () {
    appendFired = true
  })

  a.core.ongroupupdate = (key, head) => {
    t.ok(appendFired, 'append event fired before ongroupupdate')
    t.is(head.length, a.length, 'ongroupupdate head.length matches core.length')
  }

  await a.setGroup(groupKey)
  await a.append('beep')
})

test('groups - ongroupupdate() fires after truncate event & length updating', async function (t) {
  t.plan(3)
  const a = await create(t)
  const groupKey = b4a.alloc(32, 1)

  await a.setGroup(groupKey)
  await a.append('beep')

  let truncateFired = false

  a.on('truncate', function () {
    truncateFired = true
  })

  a.core.ongroupupdate = (key, head) => {
    t.ok(truncateFired, 'truncate event fired before ongroupupdate')
    t.is(head.length, a.length, 'ongroupupdate head.length matches core.length after truncate')
    t.is(head.fork, a.fork, 'ongroupupdate head.fork matches core.fork after truncate')
  }

  await a.truncate(0, { fork: 3 })
})
