const test = require('brittle')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const { core: keys } = require('hypercore-storage/lib/keys.js')

const Hypercore = require('..')
const { createStorage } = require('./helpers')

test('basic purge', async function (t) {
  const dir = await t.tmp()

  const keyPair = crypto.keyPair()
  const core = new Hypercore(await createStorage(t, dir), { keyPair })
  await core.append(['a', 'b', 'c'])

  await core.purge()

  t.is(core.closed, true)

  const reopened = new Hypercore(await createStorage(t, dir), { key: keyPair.publicKey })
  await reopened.ready()

  t.is(reopened.length, 0, 'nothing remains')
  t.is(reopened.writable, false, 'auth is gone with it')

  await reopened.close()
})

test('purge closes all sessions', async function (t) {
  const dir = await t.tmp()
  const core = new Hypercore(await createStorage(t, dir))
  await core.append(['a', 'b', 'c'])
  const otherSession = core.session()
  await otherSession.ready()

  await core.purge()

  t.is(core.closed, true)
  t.is(otherSession.closed, true)
})

test('purge from another session', async function (t) {
  const dir = await t.tmp()
  const core = new Hypercore(await createStorage(t, dir))
  await core.append(['a', 'b', 'c'])
  const otherSession = core.session()

  await otherSession.purge()

  t.is(core.closed, true)
  t.is(otherSession.closed, true)
})

test('purge leaves other cores in the storage alone', async function (t) {
  const dir = await t.tmp()

  const keyPair = crypto.keyPair()
  const manifest = { signers: [{ publicKey: keyPair.publicKey }] }
  const key = Hypercore.key(manifest)

  const kept = new Hypercore(await createStorage(t, dir))
  await kept.append(['x', 'y'])
  const keptKey = kept.key
  await kept.close()

  const purged = new Hypercore(await createStorage(t, dir), { key, manifest, keyPair })
  await purged.append(['a', 'b', 'c'])
  await purged.purge()

  const a = new Hypercore(await createStorage(t, dir), { key: keptKey })
  await a.ready()
  t.is(a.length, 2, 'other core intact')
  t.alike(await a.get(1), Buffer.from('y'))
  await a.close()

  const b = new Hypercore(await createStorage(t, dir), { key })
  await b.ready()
  t.is(b.length, 0, 'purged core gone')
  await b.close()
})

test('purge deletes the rows, not just the record', async function (t) {
  const dir = await t.tmp()

  // a keyless core opens the storage's default core, so key both explicitly
  const keptPair = crypto.keyPair()
  const kept = new Hypercore(await createStorage(t, dir), {
    key: keptPair.publicKey,
    keyPair: keptPair
  })
  await kept.append(['x', 'y'])
  const keptPtr = kept.core.storage.core
  await kept.close()

  const keyPair = crypto.keyPair()
  const core = new Hypercore(await createStorage(t, dir), { key: keyPair.publicKey, keyPair })
  await core.append(['a', 'b', 'c'])
  await core.setUserData('hello', b4a.from('world'))
  const ptr = core.core.storage.core
  t.not(ptr.corePointer, keptPtr.corePointer)

  await core.purge()

  const storage = await createStorage(t, dir)
  t.is(
    await count(storage, keys.core(ptr.corePointer), keys.core(ptr.corePointer + 1)),
    0,
    'core rows'
  )
  t.is(
    await count(storage, keys.data(ptr.dataPointer), keys.data(ptr.dataPointer + 1)),
    0,
    'blocks, tree, bitfield and user data'
  )
  t.not(
    await count(storage, keys.core(keptPtr.corePointer), keys.core(keptPtr.corePointer + 1)),
    0,
    'other core rows untouched'
  )
  t.not(
    await count(storage, keys.data(keptPtr.dataPointer), keys.data(keptPtr.dataPointer + 1)),
    0,
    'other core data untouched'
  )
  await storage.close()
})

test('purge on a closed core fails clearly', async function (t) {
  const dir = await t.tmp()
  const core = new Hypercore(await createStorage(t, dir))
  await core.append(['a'])
  await core.close()

  await t.exception(core.purge(), /closed/)
})

test('a session that will not close aborts the purge and leaves the core intact', async function (t) {
  const dir = await t.tmp()
  const core = new Hypercore(await createStorage(t, dir))
  await core.append(['a', 'b'])
  const key = core.key

  const stuck = core.session()
  await stuck.ready()
  stuck.close = () => Promise.reject(new Error('busy'))

  await t.exception(core.purge(), /sessions are open/)

  t.is(core.core.closed, false, 'the core stays open for the session that refused')
  t.is(core.core.autoClose, true, 'autoClose restored')
  t.alike(await stuck.get(1), b4a.from('b'), 'still readable')

  delete stuck.close
  await stuck.close()
  t.is(core.core.closed, true, 'closing the last session closes the core again')

  const reopened = new Hypercore(await createStorage(t, dir), { key })
  await reopened.ready()
  t.is(reopened.length, 2, 'nothing was deleted')
  await reopened.close()
})

async function count(storage, gte, lt) {
  let n = 0
  for await (const _ of storage.db.iterator({ gte, lt })) n++
  return n
}
