const test = require('brittle')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const { create } = require('./helpers')

test('core - append', async function (t) {
  const core = await create(t)

  const sess = core.session({ name: 'session' })

  await sess.append('1')
  await sess.append('2')
  await sess.append('3')

  await core.core.commit(sess.state)

  t.is(core.length, 3)
  t.is(sess.length, 3)

  const keyPair = crypto.keyPair()

  const manifest = {
    prologue: {
      length: core.length,
      hash: core.state.tree.hash()
    },
    signers: [{
      publicKey: keyPair.publicKey 
    }]
  }

  const core2 = await create(t, { manifest, keyPair })
  await core2.core.copyPrologue(core.state)

  t.is(core2.length, 3)

  await sess.moveTo(core2.core)
  await sess.append('4')

  await core2.core.commit(sess.state)

  await core2.close()
  await sess.close()
})
