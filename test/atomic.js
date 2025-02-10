const test = require('brittle')
const b4a = require('b4a')

const Hypercore = require('../')
const { create, createStorage } = require('./helpers')

test('atomic - session', async function (t) {
  const core = await create(t)

  await core.append('hello')
  await core.append('world')

  const atom = core.state.storage.createAtom()

  const atomic = core.session({ atom })

  await atomic.append('edits!')

  t.alike(await atomic.get(0), b4a.from('hello'))
  t.alike(await atomic.get(1), b4a.from('world'))
  t.alike(await atomic.get(2), b4a.from('edits!'))
  t.alike(await atomic.seek(11), [2, 1])
  t.alike(atomic.byteLength, 16)
  t.alike(atomic.length, 3)

  await atomic.close()

  // nothing changed as it was atomic session
  t.alike(core.byteLength, 10)
  t.alike(core.length, 2)

  await core.close()
})

test('atomic - checkout session', async function (t) {
  const core = await create(t)

  await core.append('hello')
  await core.append('world')

  let truncates = 0
  let appends = 0

  core.on('append', () => appends++)
  core.on('truncate', () => truncates++)

  const atom = core.state.storage.createAtom()

  const atomic = core.session({ atom, checkout: 1 })
  await atomic.ready()

  await atomic.append('edits!')

  t.alike(await atomic.get(0), b4a.from('hello'))
  t.alike(await atomic.get(1), b4a.from('edits!'))
  t.alike(await atomic.seek(11), [2, 0])
  t.alike(atomic.byteLength, 11)
  t.alike(atomic.length, 2)

  // nothing changed as it was atomic session
  t.alike(core.byteLength, 10)
  t.alike(core.length, 2)

  t.is(appends, 0)
  t.is(truncates, 0)

  await atom.flush()

  t.alike(core.byteLength, 11)
  t.alike(core.length, 2)

  t.is(appends, 1)
  t.is(truncates, 1)

  await atomic.close()
  await core.close()
})

test('atomic - append', async function (t) {
  const core = await create(t)

  await core.append('hello')
  await core.append('world')

  const atom = core.state.storage.createAtom()

  const atomic = core.session({ atom })

  await atomic.append('edits!')

  t.alike(atomic.byteLength, 16)
  t.alike(atomic.length, 3)

  t.alike(core.byteLength, 10)
  t.alike(core.length, 2)

  await atom.flush()

  t.alike(core.byteLength, 16)
  t.alike(core.length, 3)

  await atomic.close()
  await core.close()
})

test('atomic - multiple flushes', async function (t) {
  const core = await create(t)

  await core.append('hello')
  await core.append('world')

  const atom = core.state.storage.createAtom()

  const atomic = core.session({ atom })

  await atomic.append('edits!')

  t.alike(atomic.byteLength, 16)
  t.alike(atomic.length, 3)

  t.alike(core.byteLength, 10)
  t.alike(core.length, 2)

  await atom.flush()

  t.alike(core.byteLength, 16)
  t.alike(core.length, 3)

  await atomic.append('more')

  t.alike(atomic.byteLength, 20)
  t.alike(atomic.length, 4)

  t.alike(core.byteLength, 16)
  t.alike(core.length, 3)

  await atom.flush()

  t.alike(core.byteLength, 20)
  t.alike(core.length, 4)

  await atomic.close()
  await core.close()
})

test('atomic - across cores', async function (t) {
  const core = await create(t)
  const core2 = await create(t)

  let appends = 0

  t.is(core.length, 0)
  t.is(core.writable, true)
  t.is(core.readable, true)

  core.on('append', function () {
    appends++
  })

  const atom = core.state.storage.createAtom()

  const a1 = core.session({ atom })
  const a2 = core2.session({ atom })

  await a1.append('1.1')
  await a1.append('1.2')
  await a2.append('2.2')

  t.is(a1.length, 2)
  t.is(a2.length, 1)

  t.is(core.length, 0)
  t.is(core2.length, 0)

  t.is(core.core.bitfield.get(0), false)
  t.is(core2.core.bitfield.get(0), false)

  t.is(appends, 0)

  await atom.flush()

  t.is(core.length, 2)
  t.is(core2.length, 1)

  t.is(core.core.bitfield.get(0), true)
  t.is(core2.core.bitfield.get(0), true)

  t.is(appends, 1)

  await a1.close()
  await a2.close()

  await core.close()
  await core2.close()
})

test('atomic - overwrite', async function (t) {
  const core = await create(t)
  const core2 = await create(t)

  await core.append('hello')
  await core.append('world')

  await core2.append('hello')

  t.is(core.length, 2)
  t.is(core2.length, 1)

  const draft = core.session({ name: 'writer' })
  const draft2 = core2.session({ name: 'writer' })

  await draft.append('all the way')

  await draft2.append('back')
  await draft2.append('to the')
  await draft2.append('beginning')

  const atom = core.state.storage.createAtom()

  const a1 = core.session({ atom })
  const a2 = core2.session({ atom })

  await a1.commit(draft, { treeLength: core.length })
  await a2.commit(draft2, { treeLength: core2.length })

  t.is(a1.length, 3)
  t.is(a2.length, 4)

  t.is(core.length, 2)
  t.is(core2.length, 1)

  await atom.flush()

  t.is(core.length, 3)
  t.is(core2.length, 4)

  await draft.close()
  await draft2.close()

  await a1.close()
  await a2.close()

  await core.close()
  await core2.close()
})

test('atomic - user data', async function (t) {
  const core = await create(t)

  await core.setUserData('hello', 'world')

  t.alike(await core.getUserData('hello'), b4a.from('world'))

  const atom = core.state.storage.createAtom()

  const atomic = core.session({ atom })
  await atomic.setUserData('hello', 'done')

  t.alike(await atomic.getUserData('hello'), b4a.from('done'))
  t.alike(await core.getUserData('hello'), b4a.from('world'))

  await atom.flush()

  t.alike(await core.getUserData('hello'), b4a.from('done'))

  await atomic.close()
  await core.close()
})

test('atomic - append and user data', async function (t) {
  const core = await create(t)

  await core.setUserData('hello', 'world')

  t.is(core.length, 0)
  t.alike(await core.getUserData('hello'), b4a.from('world'))

  const atom = core.state.storage.createAtom()

  const atomic = core.session({ atom })

  await atomic.setUserData('hello', 'done')
  await atomic.append('append')

  t.alike(await core.getUserData('hello'), b4a.from('world'))
  t.alike(await atomic.getUserData('hello'), b4a.from('done'))

  t.is(core.length, 0)
  t.is(atomic.length, 1)

  await atom.flush()

  t.is(core.length, 1)
  t.alike(await core.getUserData('hello'), b4a.from('done'))

  await atomic.close()
  await core.close()
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

  const draft = core.session({ name: 'writer' })
  const draft2 = core2.session({ name: 'writer' })

  await draft.append('all the way')

  await draft2.append('back')
  await draft2.append('to the')
  await draft2.append('beginning')

  const atom = core.state.storage.createAtom()

  const a1 = core.session({ atom })
  const a2 = core2.session({ atom })

  await a1.commit(draft, { treeLength: core.length, atom })
  await a2.commit(draft2, { treeLength: core2.length, atom })

  await a1.setUserData('hello', 'world', { atom })
  await a2.setUserData('goodbye', 'everybody', { atom })

  t.is(core.length, 2)
  t.is(core2.length, 1)

  t.is(a1.length, 3)
  t.is(a2.length, 4)

  t.alike(await core.getUserData('hello'), null)
  t.alike(await core.getUserData('goodbye'), null)

  t.alike(await a1.getUserData('hello'), b4a.from('world'))
  t.alike(await a2.getUserData('goodbye'), b4a.from('everybody'))

  await atom.flush()

  t.is(core.length, 3)
  t.is(core2.length, 4)

  t.alike(await core.getUserData('hello'), b4a.from('world'))
  t.alike(await core2.getUserData('goodbye'), b4a.from('everybody'))

  await a1.close()
  await a2.close()

  await draft.close()
  await draft2.close()

  await core.close()
  await core2.close()
})

test('atomic - truncate', async function (t) {
  const core = await create(t)

  await core.append('hello')
  await core.append('world')

  const atom = core.state.storage.createAtom()

  const atomic = core.session({ atom })

  await atomic.truncate(1)

  t.alike(core.byteLength, 10)
  t.alike(core.length, 2)

  t.alike(atomic.byteLength, 5)
  t.alike(atomic.length, 1)

  t.alike(await atomic.get(0), b4a.from('hello'))
  t.alike(await atomic.get(1, { wait: false }), null)
  t.alike(await atomic.seek(6, { wait: false }), null)

  await atom.flush()

  t.alike(core.byteLength, 5)
  t.alike(core.length, 1)

  await atomic.close()
  await core.close()
})

// not supported yet
test.skip('draft truncate then append', async function (t) {
  const core = await create(t)

  await core.append('hello')
  await core.append('world')

  const atom = core.state.storage.createAtom()

  const atomic = core.session({ atom })

  await atomic.truncate(1)
  await atomic.append('other')
  await atomic.append('data')

  t.alike(core.byteLength, 10)
  t.alike(core.length, 2)
  t.alike(await core.get(2, { wait: false }), null)

  t.alike(atomic.byteLength, 14)
  t.alike(atomic.length, 3)

  t.alike(await atomic.get(0), b4a.from('hello'))
  t.alike(await atomic.get(1), b4a.from('other'))
  t.alike(await atomic.get(2), b4a.from('data'))
  t.alike(await atomic.seek(11), [2, 1])

  await atom.flush()

  // nothing changed as it was a draft
  t.alike(core.byteLength, 14)
  t.alike(core.length, 3)
  t.alike(await core.get(2), b4a.from('data'))

  await atomic.close()
  await core.close()
})
