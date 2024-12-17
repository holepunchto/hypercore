const test = require('brittle')
const b4a = require('b4a')

const { create } = require('./helpers')

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

  const atomizer = core.state.storage.atomizer()

  atomizer.enter()

  const promises = [
    core.append('1', { atomizer }),
    core2.append('2', { atomizer })
  ]

  await new Promise(resolve => setTimeout(resolve, 100))

  t.is(core.length, 0)
  t.is(core2.length, 0)
  t.is(appends, 0)

  atomizer.exit()
  await Promise.all(promises)

  t.is(core.length, 1)
  t.is(core2.length, 1)
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

  const atomizer = core.state.storage.atomizer()

  atomizer.enter()

  const overwrite = [
    core.core.commit(draft.state, { treeLength: core.length, atomizer }),
    core2.core.commit(draft2.state, { treeLength: core2.length, atomizer })
  ]

  await new Promise(resolve => setTimeout(resolve, 100))

  t.is(core.length, 2)
  t.is(core2.length, 1)

  atomizer.exit()
  await t.execution(Promise.all(overwrite))

  t.is(core.length, 3)
  t.is(core2.length, 4)

  draft.close()
  draft2.close()
})

test('atomic - user data', async function (t) {
  const core = await create(t)

  await core.setUserData('hello', 'world')

  t.alike(await core.getUserData('hello'), b4a.from('world'))

  const atomizer = core.state.storage.atomizer()

  atomizer.enter()

  const userData = core.setUserData('hello', 'done', { atomizer })

  await new Promise(resolve => setTimeout(resolve, 100))

  t.alike(await core.getUserData('hello'), b4a.from('world'))

  atomizer.exit()
  await t.execution(userData)

  t.alike(await core.getUserData('hello'), b4a.from('done'))
})

test('atomic - append and user data', async function (t) {
  const core = await create(t)

  await core.setUserData('hello', 'world')

  t.is(core.length, 0)
  t.alike(await core.getUserData('hello'), b4a.from('world'))

  const atomizer = core.state.storage.atomizer()

  atomizer.enter()

  const promises = [
    core.setUserData('hello', 'done', { atomizer }),
    core.append('append', { atomizer })
  ]

  await new Promise(resolve => setTimeout(resolve, 100))

  t.alike(await core.getUserData('hello'), b4a.from('world'))
  t.is(core.length, 0)

  atomizer.exit()
  await t.execution(Promise.all(promises))

  t.is(core.length, 1)
  t.alike(await core.getUserData('hello'), b4a.from('done'))
})

test('atomic - overwrite and user data', async function (t) {
  const core = await create(t)
  const core2 = await create(t)

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

  const atomizer = core.state.storage.atomizer()

  atomizer.enter()

  const promises = [
    core.core.commit(draft.state, { treeLength: core.length, atomizer }),
    core2.core.commit(draft2.state, { treeLength: core2.length, atomizer }),
    core.setUserData('hello', 'world', { atomizer }),
    core2.setUserData('goodbye', 'everybody', { atomizer })
  ]

  await new Promise(resolve => setTimeout(resolve, 100))

  t.is(core.length, 2)
  t.is(core2.length, 1)
  t.alike(await core.getUserData('hello'), null)
  t.alike(await core.getUserData('goodbye'), null)

  atomizer.exit()
  await t.execution(Promise.all(promises))

  t.is(core.length, 3)
  t.is(core2.length, 4)
  t.alike(await core.getUserData('hello'), b4a.from('world'))
  t.alike(await core2.getUserData('goodbye'), b4a.from('everybody'))

  draft.close()
  draft2.close()
})
