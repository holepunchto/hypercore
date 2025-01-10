const test = require('brittle')
const b4a = require('b4a')

const Hypercore = require('../')
const { create, createStorage } = require('./helpers')

test('atomic - append', async function (t) {
  const core = await create(t)
  const core2 = await create(t)

  let appends = 0

  t.is(core.length, 0)
  t.is(core.writable, true)
  t.is(core.readable, true)

  core.on('append', function () {
    appends++
  })

  const atom = core.state.storage.atom()

  const a1 = core.session({ atom })
  const a2 = core2.session({ atom })

  await a1.append('1')
  await a2.append('2')

  t.is(a1.length, 1)
  t.is(a2.length, 1)

  t.is(core.length, 0)
  t.is(core2.length, 0)

  t.is(core.core.bitfield.get(0), false)
  t.is(core2.core.bitfield.get(0), false)

  t.is(appends, 0)

  await atom.flush()

  t.is(core.length, 1)
  t.is(core2.length, 1)

  t.is(core.core.bitfield.get(0), true)
  t.is(core2.core.bitfield.get(0), true)

  t.is(appends, 1)
})

test('atomic - overwrite', async function (t) {
  const core = await create(t)
  const core2 = await create(t)

  await core.append('hello')
  await core.append('world')

  await core2.append('hello')

  t.is(core.length, 2)
  t.is(core2.length, 1)

  const draft = core.session({ draft: true })
  const draft2 = core2.session({ draft: true })

  await draft.append('all the way')

  await draft2.append('back')
  await draft2.append('to the')
  await draft2.append('beginning')

  const atom = core.state.storage.atom()

  atom.enter()

  const overwrite = [
    core.core.commit(draft.state, { treeLength: core.length, atom }),
    core2.core.commit(draft2.state, { treeLength: core2.length, atom })
  ]

  await new Promise(resolve => setTimeout(resolve, 100))

  t.is(core.length, 2)
  t.is(core2.length, 1)

  atom.exit()
  await t.execution(Promise.all(overwrite))

  t.is(core.length, 3)
  t.is(core2.length, 4)

  await draft.close()
  await draft2.close()
})

test('atomic - user data', async function (t) {
  const core = await create(t)

  await core.setUserData('hello', 'world')

  t.alike(await core.getUserData('hello'), b4a.from('world'))

  const atom = core.state.storage.atom()

  atom.enter()

  const userData = core.setUserData('hello', 'done', { atom })

  await new Promise(resolve => setTimeout(resolve, 100))

  t.alike(await core.getUserData('hello'), b4a.from('world'))

  atom.exit()
  await t.execution(userData)

  t.alike(await core.getUserData('hello'), b4a.from('done'))
})

test('atomic - append and user data', async function (t) {
  const core = await create(t)

  await core.setUserData('hello', 'world')

  t.is(core.length, 0)
  t.alike(await core.getUserData('hello'), b4a.from('world'))

  const atom = core.state.storage.atom()

  atom.enter()

  const promises = [
    core.setUserData('hello', 'done', { atom }),
    core.append('append', { atom })
  ]

  await new Promise(resolve => setTimeout(resolve, 100))

  t.alike(await core.getUserData('hello'), b4a.from('world'))
  t.is(core.length, 0)

  atom.exit()
  await t.execution(Promise.all(promises))

  t.is(core.length, 1)
  t.alike(await core.getUserData('hello'), b4a.from('done'))
})

test('atomic - overwrite and user data', async function (t) {
  const storage = await createStorage(t)

  const core = new Hypercore(storage)
  const core2 = new Hypercore(storage)

  await core.ready()
  await core2.ready()

  await core.append('hello')
  await core.append('world')

  await core2.append('hello')

  t.is(core.length, 2)
  t.is(core2.length, 1)
  t.alike(await core.getUserData('hello'), null)
  t.alike(await core.getUserData('goodbye'), null)

  const draft = core.session({ draft: true })
  const draft2 = core2.session({ draft: true })

  await draft.append('all the way')

  await draft2.append('back')
  await draft2.append('to the')
  await draft2.append('beginning')

  const atom = core.state.storage.atom()

  atom.enter()

  const promises = [
    core.core.commit(draft.state, { treeLength: core.length, atom }),
    core2.core.commit(draft2.state, { treeLength: core2.length, atom }),
    core.setUserData('hello', 'world', { atom }),
    core2.setUserData('goodbye', 'everybody', { atom })
  ]

  await new Promise(resolve => setTimeout(resolve, 100))

  t.is(core.length, 2)
  t.is(core2.length, 1)
  t.alike(await core.getUserData('hello'), null)
  t.alike(await core.getUserData('goodbye'), null)

  atom.exit()
  await t.execution(Promise.all(promises))

  t.is(core.length, 3)
  t.is(core2.length, 4)
  t.alike(await core.getUserData('hello'), b4a.from('world'))
  t.alike(await core2.getUserData('goodbye'), b4a.from('everybody'))

  await draft.close()
  await draft2.close()

  await core.close()
  await core2.close()
})

test('atomic - move to', async function (t) {
  const storage = await createStorage(t)

  const core = new Hypercore(storage)
  const core2 = new Hypercore(storage)

  await core.ready()
  await core2.ready()

  await core.append('hello')
  await core.append('world')

  await core2.append('hello')

  t.is(core.length, 2)
  t.is(core2.length, 1)

  const session = core.session({ name: 'moveable' })
  await session.ready()

  let truncates = 0
  session.on('truncate', () => { truncates++ })

  t.is(session.length, 2)
  t.ok(session.core === core.core)
  t.is(truncates, 0)

  const atom = core.state.storage.atom()

  atom.enter()

  const moving = session.state.moveTo(core2.core, 1, { atom })

  await new Promise(resolve => setTimeout(resolve, 100))

  t.is(session.length, 2)
  t.ok(session.core === core.core)
  t.is(truncates, 0)

  atom.exit()
  await t.execution(moving)

  t.is(session.length, 1)
  t.ok(session.core !== core.core)
  t.ok(session.core === core2.core)
  t.is(truncates, 1)

  await core.close()
  await session.close()
  await core2.close()
})
