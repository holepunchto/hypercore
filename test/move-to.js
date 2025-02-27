const test = require('brittle')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const { create } = require('./helpers')

test('move - basic', async function (t) {
  t.plan(9)

  const core = await create(t)

  const sess = core.session({ name: 'session' })

  await sess.append('1')
  await sess.append('2')
  await sess.append('3')

  await core.commit(sess)

  t.is(core.length, 3)
  t.is(sess.length, 3)

  const keyPair = crypto.keyPair()

  const manifest = {
    prologue: {
      length: core.length,
      hash: core.state.hash()
    },
    signers: [{
      publicKey: keyPair.publicKey
    }]
  }

  const core2 = await create(t, { manifest, keyPair })
  await core2.core.copyPrologue(core.state)

  t.is(core2.length, 3)

  sess.once('migrate', key => { t.alike(key, core2.key) })

  await sess.state.moveTo(core2, core2.length)
  await sess.append('4')

  await core2.commit(sess)

  t.alike(await sess.get(0), b4a.from('1'))
  t.alike(await sess.get(1), b4a.from('2'))
  t.alike(await sess.get(2), b4a.from('3'))
  t.alike(await sess.get(3), b4a.from('4'))

  t.alike(await core2.get(3), b4a.from('4'))

  await core.close()
  await core2.close()
  await sess.close()
})

test('move - snapshots', async function (t) {
  const core = await create(t)

  await core.append('hello')
  await core.append('world')
  await core.append('again')

  const sess = core.session({ name: 'snapshot' })

  const snap = sess.snapshot()
  await snap.ready()

  await sess.close()
  await core.truncate(1)

  await core.append('break')

  t.is(snap.length, 3)
  t.is(core.length, 2)

  const keyPair = crypto.keyPair()

  const manifest = {
    prologue: {
      length: core.length,
      hash: core.state.hash()
    },
    signers: [{
      publicKey: keyPair.publicKey
    }]
  }

  const core2 = await create(t, { manifest, keyPair })
  await core2.core.copyPrologue(core.state)

  t.is(core2.length, 2)

  await snap.state.moveTo(core2, core2.length)

  t.is(snap.length, 3)

  t.alike(await snap.get(0), b4a.from('hello'))
  t.alike(await snap.get(1), b4a.from('world'))
  t.alike(await snap.get(2), b4a.from('again'))

  await snap.close()
  await core.close()
  await core2.close()
})
