const { create } = require('./helpers')
const test = require('brittle')
const b4a = require('b4a')

test('draft', async function (t) {
  const core = await create(t)

  await core.append('hello')
  await core.append('world')

  const draft = core.session({ draft: true })

  await draft.append('edits!')

  t.alike(await draft.get(0), b4a.from('hello'))
  t.alike(await draft.get(1), b4a.from('world'))
  t.alike(await draft.get(2), b4a.from('edits!'))
  t.alike(await draft.seek(11), [2, 1])
  t.alike(draft.byteLength, 16)
  t.alike(draft.length, 3)

  await draft.close()

  // nothing changed as it was a draft
  t.alike(core.byteLength, 10)
  t.alike(core.length, 2)

  await core.close()
})

test('draft and then undraft', async function (t) {
  const core = await create(t)

  await core.append('hello')
  await core.append('world')

  const draft = core.session({ draft: true })

  await draft.append('edits!')

  await core.core.commit(draft.state, { treeLength: 2 })

  await draft.close()

  t.alike(core.byteLength, 16)
  t.alike(core.length, 3)

  await core.close()
})

test('draft truncate', async function (t) {
  const core = await create(t)

  await core.append('hello')
  await core.append('world')

  const sess = core.session({ name: 'batch' })

  await sess.append('some')
  await sess.append('value')

  // nothing changed as it was a draft
  t.alike(sess.byteLength, 19)
  t.alike(sess.length, 4)

  const draft = sess.session({ draft: true })

  await draft.truncate(2)

  t.alike(await draft.get(0), b4a.from('hello'))
  t.alike(await draft.get(1), b4a.from('world'))
  t.alike(await draft.get(2, { wait: false }), null)
  t.alike(await draft.seek(9), [1, 4])
  t.alike(draft.byteLength, 10)
  t.alike(draft.length, 2)

  t.unlike(await sess.state.overwrite(draft.state, { overwrite: true }), null)

  await draft.close()

  // nothing changed as it was a draft
  t.alike(sess.byteLength, 10)
  t.alike(sess.length, 2)

  await core.close()
})

test('draft truncate then append', async function (t) {
  const core = await create(t)

  await core.append('hello')
  await core.append('world')

  const sess = core.session({ name: 'batch' }) // only batch can be truncated

  await sess.append('some')
  await sess.append('value')

  const draft = sess.session({ draft: true })

  await draft.truncate(2)
  await draft.append('other data')

  t.alike(await draft.get(0), b4a.from('hello'))
  t.alike(await draft.get(1), b4a.from('world'))
  t.alike(await draft.get(2), b4a.from('other data'))
  t.alike(await draft.seek(11), [2, 1])
  t.alike(draft.byteLength, 20)
  t.alike(draft.length, 3)

  t.unlike(await sess.state.overwrite(draft.state), null)

  await draft.close()

  // nothing changed as it was a draft
  t.alike(sess.byteLength, 20)
  t.alike(sess.length, 3)

  await sess.close()
  await core.close()
})
