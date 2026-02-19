const test = require('brittle')
const { create, replicate } = require('./helpers')

test('no requests in pushOnly mode', async function (t) {
  const a = await create(t)
  const b = await create(t, a.key, { allowPush: true, pushOnly: true })

  b.replicator.setPushOnly(true)

  t.is(b.replicator.pushOnly, true)

  replicate(a, b, t)

  await a.append('1')
  await a.append('2')
  await a.append('3')

  await t.exception(b.get(0, { timeout: 500 }), /REQUEST_TIMEOUT/)
  await t.exception(b.get(1, { timeout: 500 }), /REQUEST_TIMEOUT/)
  await t.exception(b.get(2, { timeout: 500 }), /REQUEST_TIMEOUT/)

  t.ok(a.peers[0].remoteAllowPush)

  await a.replicator.push(0)
  await a.replicator.push(1)
  await a.replicator.push(2)

  await t.execution(b.get(0, { timeout: 500 }))
  await t.execution(b.get(1, { timeout: 500 }))
  await t.execution(b.get(2, { timeout: 500 }))
})

test('push and pull concurrently', async function (t) {
  const a = await create(t)
  const b = await create(t, a.key, { allowPush: true, pushOnly: true })

  t.is(b.replicator.pushOnly, true)

  replicate(a, b, t)

  await new Promise((resolve) => a.on('peer-add', resolve))
  for (let i = 0; i < 20; i++) {
    await a.append(i.toString())
  }

  const bHasLength = new Promise((resolve) =>
    b.on('append', () => {
      if (b.length === 30) resolve()
    })
  )
  const appends = []
  for (let i = 20; i < 30; i++) {
    appends.push(a.append(i.toString()).then(() => a.replicator.push(i)))
  }

  await b.get(10, { force: true })

  await Promise.all(appends)
  await bHasLength

  t.pass('b synced length')
  t.ok(await b.has(29))
})

test('push before append', async function (t) {
  const a = await create(t)
  const b = await create(t, a.key, { allowPush: true })

  replicate(a, b, t)

  await new Promise((resolve) => a.on('peer-add', resolve))

  // wait for peer to sync
  await new Promise((resolve) => setTimeout(resolve, 1000))

  const recv = new Promise((resolve) => b.on('append', resolve))
  const send = a.append('hello world')

  // block needs to be written to storage before
  await new Promise((resolve) => setTimeout(resolve, 2))

  await t.execution(a.replicator.push(0))

  await Promise.all([send, recv])

  t.comment(b.length ? 'b synced length' : 'b did not sync length')
})
