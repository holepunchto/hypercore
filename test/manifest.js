const test = require('brittle')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const tmpDir = require('test-tmp')
const c = require('compact-encoding')

const Hypercore = require('../')
const Verifier = require('../lib/verifier')
const { assemble, partialSignature, signableLength } = require('../lib/multisig')
const { MerkleTree } = require('../lib/merkle-tree')
const caps = require('../lib/caps')
const enc = require('../lib/messages')

const { create, createStorage, createStored, replicate, unreplicate } = require('./helpers')

// TODO: move this to be actual tree batches instead - less future surprises
// for now this is just to get the tests to work as they test important things
class AssertionTreeBatch {
  constructor (hash, signable) {
    this._hash = hash
    this._signable = signable
    this.length = 1
  }

  hash () {
    return this._hash
  }

  signable (key) {
    return b4a.concat([key, this._signable])
  }

  signableCompat () {
    return this._signable
  }
}

test('create verifier - static signer', async function (t) {
  const treeHash = b4a.alloc(32, 1)

  const manifest = {
    quorum: 0,
    signers: [],
    prologue: {
      hash: treeHash,
      length: 1
    }
  }

  const verifier = Verifier.fromManifest(manifest)
  const batch = new AssertionTreeBatch(b4a.alloc(32, 1), null)

  t.ok(verifier.verify(batch))

  batch.length = 2
  t.absent(verifier.verify(batch))

  batch.length = 1
  batch._hash[0] ^= 0xff

  t.absent(verifier.verify(batch))
})

test('create verifier - single signer no sign (v0)', async function (t) {
  const keyPair = crypto.keyPair()

  const namespace = b4a.alloc(32, 2)

  const manifest = {
    version: 0,
    quorum: 1,
    signers: [{
      signature: 'ed25519',
      namespace,
      publicKey: keyPair.publicKey
    }]
  }

  const verifier = Verifier.fromManifest(manifest)

  const batch = new AssertionTreeBatch(null, b4a.alloc(32, 1))

  const signature = crypto.sign(batch.signable(namespace), keyPair.secretKey)

  t.ok(verifier.verify(batch, signature))

  signature[5] ^= 0xff

  t.absent(verifier.verify(batch, signature))
})

test('create verifier - single signer no sign', async function (t) {
  const keyPair = crypto.keyPair()

  const namespace = b4a.alloc(32, 2)

  const manifest = {
    quorum: 1,
    signers: [{
      signature: 'ed25519',
      namespace,
      publicKey: keyPair.publicKey
    }]
  }

  const verifier = Verifier.fromManifest(manifest)

  const batch = new AssertionTreeBatch(null, b4a.alloc(32, 1))

  const signature = assemble([{ signer: 0, signature: crypto.sign(batch.signable(verifier.manifestHash), keyPair.secretKey), patch: null }])

  t.ok(verifier.verify(batch, signature))

  signature[5] ^= 0xff

  t.absent(verifier.verify(batch, signature))
})

test('create verifier - single signer', async function (t) {
  const keyPair = crypto.keyPair()

  const namespace = b4a.alloc(32, 2)

  const manifest = {
    quorum: 1,
    signers: [{
      signature: 'ed25519',
      namespace,
      publicKey: keyPair.publicKey
    }]
  }

  const verifier = Verifier.fromManifest(manifest)

  const batch = new AssertionTreeBatch(null, b4a.alloc(32, 1))
  const signature = verifier.sign(batch, keyPair)

  t.ok(verifier.verify(batch, signature))

  signature[5] ^= 0xff

  t.absent(verifier.verify(batch, signature))
})

test('create verifier - multi signer', async function (t) {
  const a = crypto.keyPair()
  const b = crypto.keyPair()

  const signable = b4a.alloc(32, 1)
  const aEntropy = b4a.alloc(32, 2)
  const bEntropy = b4a.alloc(32, 3)

  const manifest = {
    allowPatch: false,
    quorum: 2,
    signers: [{
      publicKey: a.publicKey,
      namespace: aEntropy,
      signature: 'ed25519'
    }, {
      publicKey: b.publicKey,
      namespace: bEntropy,
      signature: 'ed25519'
    }]
  }

  const batch = new AssertionTreeBatch(null, signable)
  const verifier = Verifier.fromManifest(manifest)

  const asig = crypto.sign(batch.signable(verifier.manifestHash), a.secretKey)
  const bsig = crypto.sign(batch.signable(verifier.manifestHash), b.secretKey)

  const signature = assemble([{ signer: 0, signature: asig, patch: 0 }, { signer: 1, signature: bsig, patch: 0 }])
  const badSignature = assemble([{ signer: 0, signature: asig, patch: 0 }, { signer: 1, signature: asig, patch: 0 }])
  const secondBadSignature = assemble([{ signer: 0, signature: asig, patch: 0 }, { signer: 0, signature: asig, patch: 0 }])
  const thirdBadSignature = assemble([{ signer: 0, signature: asig, patch: 0 }])

  t.ok(verifier.verify(batch, signature))
  t.absent(verifier.verify(batch, badSignature))
  t.absent(verifier.verify(batch, secondBadSignature))
  t.absent(verifier.verify(batch, thirdBadSignature))
})

test('create verifier - defaults', async function (t) {
  const keyPair = crypto.keyPair()

  const manifest = Verifier.createManifest({
    quorum: 1,
    signers: [{
      signature: 'ed25519',
      publicKey: keyPair.publicKey
    }]
  })

  const verifier = Verifier.fromManifest(manifest)

  t.alike(Hypercore.key(manifest), Hypercore.key(keyPair.publicKey))

  const batch = new AssertionTreeBatch(null, b4a.alloc(32, 1))
  const signature = verifier.sign(batch, keyPair)

  t.ok(verifier.verify(batch, signature))

  signature[5] ^= 0xff

  t.absent(verifier.verify(batch, signature))
})

test('create verifier - unsupported curve', async function (t) {
  t.plan(2)

  const keyPair = crypto.keyPair()

  const manifest = {
    signers: [{
      signature: 'SECP_256K1',
      publicKey: keyPair.publicKey
    }]
  }

  try {
    Verifier.createManifest(manifest)
  } catch {
    t.pass('threw')
  }

  try {
    const v = Verifier.fromManifest(manifest)
    v.toString() // just to please standard
  } catch {
    t.pass('also threw')
  }
})

test('create verifier - compat signer', async function (t) {
  const keyPair = crypto.keyPair()

  const namespace = b4a.alloc(32, 2)

  const manifest = {
    quorum: 1,
    signers: [{
      signature: 'ed25519',
      namespace,
      publicKey: keyPair.publicKey
    }]
  }

  const verifier = Verifier.fromManifest(manifest, { compat: true })

  const batch = new AssertionTreeBatch(null, b4a.alloc(32, 1))

  const signature = crypto.sign(batch.signableCompat(), keyPair.secretKey)

  t.alike(verifier.sign(batch, keyPair), signature)
  t.ok(verifier.verify(batch, signature))
})

test('multisig - append', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(await create(t, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers, 0)

  let multisig = null

  const core = await create(t, { manifest })

  t.alike(Hypercore.key(manifest), core.key)

  await signers[0].append(b4a.from('0'))
  await signers[1].append(b4a.from('0'))

  const len = signableLength([signers[0].length, signers[1].length], 2)

  t.is(len, 1)

  const batch = core.session({ name: 'batch' })

  await batch.append(b4a.from('0'))

  const sigBatch = batch.state.createTreeBatch()

  const sig = await core.core.verifier.sign(sigBatch, signers[0].keyPair)
  const sig2 = await core.core.verifier.sign(sigBatch, signers[1].keyPair)

  const proof = await partialSignature(batch, 0, len, sigBatch.length, sig)
  const proof2 = await partialSignature(batch, 1, len, sigBatch.length, sig2)

  multisig = assemble([proof, proof2])

  await t.execution(core.append(b4a.from('0'), { signature: multisig }))

  t.is(core.length, 1)

  const core2 = await create(t, { manifest })

  const s1 = core.replicate(true)
  const s2 = core2.replicate(false)

  const p = new Promise((resolve, reject) => {
    s1.on('error', reject)
    s2.on('error', reject)

    core2.on('append', resolve)
  })

  s1.pipe(s2).pipe(s1)

  await t.execution(p)

  t.is(core2.length, core.length)

  await core2.download({ start: 0, end: core.length }).downloaded()

  t.alike(await core2.get(0), b4a.from('0'))

  await batch.close()
})

test('multisig - batch failed', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(await create(t, { compat: false }))

  await Promise.all(signers.map(s => s.ready()))

  let multisig = null

  const manifest = createMultiManifest(signers)

  const core = await create(t, { manifest })

  t.alike(Hypercore.key(manifest), core.key)

  await signers[0].append(b4a.from('0'))
  await signers[1].append(b4a.from('0'))

  const len = signableLength([signers[0].length, signers[1].length], 2)

  t.is(len, 1)

  const batch = await core.session({ name: 'batch' })
  batch.keyPair = null

  await batch.append(b4a.from('0'))

  const sigBatch = batch.state.createTreeBatch()

  const sig = await core.core.verifier.sign(sigBatch, signers[0].keyPair)
  const sig2 = await core.core.verifier.sign(sigBatch, signers[1].keyPair)

  const proof = await partialSignature(batch, 0, len, sigBatch.length, sig)
  const proof2 = await partialSignature(batch, 1, len, sigBatch.length, sig2)

  multisig = assemble([proof, proof2])

  await t.execution(core.append(b4a.from('hello'), { signature: multisig }))

  const core2 = await create(t, { manifest })

  const s1 = core.replicate(true)
  const s2 = core2.replicate(false)

  const p = new Promise((resolve, reject) => {
    core2.on('verification-error', reject)

    setTimeout(resolve, 100)
  })

  s1.pipe(s2).pipe(s1)

  await t.exception(p)

  t.is(core2.length, 0)

  await batch.close()
})

test('multisig - patches', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(await create(t, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null
  const core = await create(t, { manifest })

  await signers[0].append(b4a.from('0'))
  await signers[0].append(b4a.from('1'))
  await signers[0].append(b4a.from('2'))
  await signers[0].append(b4a.from('3'))
  await signers[0].append(b4a.from('4'))

  await signers[1].append(b4a.from('0'))

  const len = signableLength([signers[0].length, signers[1].length], 2)

  t.is(len, 1)

  const proof = await partialCoreSignature(core, signers[0], len)
  const proof2 = await partialCoreSignature(core, signers[1], len)

  multisig = assemble([proof, proof2])

  await t.execution(core.append(b4a.from('0'), { signature: multisig }))

  t.is(core.length, 1)

  const core2 = await create(t, { manifest })

  const s1 = core.replicate(true)
  const s2 = core2.replicate(false)

  const p = new Promise((resolve, reject) => {
    s1.on('error', reject)
    s2.on('error', reject)

    core2.on('append', resolve)
  })

  s1.pipe(s2).pipe(s1)

  await p
  await t.execution(p)

  t.is(core2.length, core.length)

  await core2.download({ start: 0, end: core.length }).downloaded()

  t.alike(await core2.get(0), b4a.from('0'))
})

test('multisig - batch append', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(await create(t, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null
  const core = await create(t, { manifest })

  await signers[0].append(b4a.from('0'))
  await signers[0].append(b4a.from('1'))
  await signers[0].append(b4a.from('2'))
  await signers[0].append(b4a.from('3'))

  await signers[1].append(b4a.from('0'))
  await signers[1].append(b4a.from('1'))
  await signers[1].append(b4a.from('2'))
  await signers[1].append(b4a.from('3'))

  const len = signableLength([signers[0].length, signers[1].length], 2)

  t.is(len, 4)

  const proof = await partialCoreSignature(core, signers[0], len)
  const proof2 = await partialCoreSignature(core, signers[1], len)

  multisig = assemble([proof, proof2])

  await t.execution(core.append([
    b4a.from('0'),
    b4a.from('1'),
    b4a.from('2'),
    b4a.from('3')
  ], {
    signature: multisig
  }))

  t.is(core.length, 4)

  const core2 = await create(t, { manifest })

  const s1 = core.replicate(true)
  const s2 = core2.replicate(false)

  const p = new Promise((resolve, reject) => {
    s1.on('error', reject)
    s2.on('error', reject)

    core2.on('append', resolve)
  })

  s1.pipe(s2).pipe(s1)

  await t.execution(p)

  t.is(core2.length, core.length)

  await core2.download({ start: 0, end: core.length }).downloaded()

  t.alike(await core2.get(0), b4a.from('0'))
  t.alike(await core2.get(1), b4a.from('1'))
  t.alike(await core2.get(2), b4a.from('2'))
  t.alike(await core2.get(3), b4a.from('3'))
})

test('multisig - batch append with patches', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(await create(t, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null
  const core = await create(t, { manifest })

  t.alike(Hypercore.key(manifest), core.key)

  await signers[0].append(b4a.from('0'))
  await signers[0].append(b4a.from('1'))
  await signers[0].append(b4a.from('2'))
  await signers[0].append(b4a.from('3'))
  await signers[0].append(b4a.from('4'))
  await signers[0].append(b4a.from('5'))

  await signers[1].append(b4a.from('0'))
  await signers[1].append(b4a.from('1'))
  await signers[1].append(b4a.from('2'))
  await signers[1].append(b4a.from('3'))

  const len = signableLength([signers[0].length, signers[1].length], 2)

  t.is(len, 4)

  const proof = await partialCoreSignature(core, signers[0], len)
  const proof2 = await partialCoreSignature(core, signers[1], len)

  multisig = assemble([proof, proof2])

  await t.execution(core.append([
    b4a.from('0'),
    b4a.from('1'),
    b4a.from('2'),
    b4a.from('3')
  ], {
    signature: multisig
  }))

  t.is(core.length, 4)

  const core2 = await create(t, { manifest })

  const s1 = core.replicate(true)
  const s2 = core2.replicate(false)

  const p = new Promise((resolve, reject) => {
    s1.on('error', reject)
    s2.on('error', reject)

    core2.on('append', resolve)
  })

  s1.pipe(s2).pipe(s1)

  await t.execution(p)

  t.is(core2.length, core.length)

  await core2.download({ start: 0, end: core.length }).downloaded()

  t.alike(await core2.get(0), b4a.from('0'))
  t.alike(await core2.get(1), b4a.from('1'))
  t.alike(await core2.get(2), b4a.from('2'))
  t.alike(await core2.get(3), b4a.from('3'))
})

test('multisig - cannot divide batch', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(await create(t, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null

  const core = await create(t, { manifest })

  await signers[0].append(b4a.from('0'))
  await signers[0].append(b4a.from('1'))
  await signers[0].append(b4a.from('2'))
  await signers[0].append(b4a.from('3'))

  await signers[1].append(b4a.from('0'))
  await signers[1].append(b4a.from('1'))
  await signers[1].append(b4a.from('2'))
  await signers[1].append(b4a.from('3'))

  const len = signableLength([signers[0].length, signers[1].length], 2)

  t.is(len, 4)

  const proof = await partialCoreSignature(core, signers[0], len)
  const proof2 = await partialCoreSignature(core, signers[1], len)

  multisig = assemble([proof, proof2])

  await t.execution(core.append([
    b4a.from('0'),
    b4a.from('1')
  ], {
    signature: multisig
  }))

  const core2 = await create(t, { manifest })

  const s1 = core.replicate(true)
  const s2 = core2.replicate(false)

  const p = new Promise((resolve, reject) => {
    core.once('verification-error', reject)
    core2.once('verification-error', reject)

    core2.on('append', resolve)
  })

  s1.pipe(s2).pipe(s1)

  await t.exception(p)

  t.is(core2.length, 0)
})

test('multisig - multiple appends', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(await create(t, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig1 = null
  let multisig2 = null

  const core = await create(t, { manifest })

  await signers[0].append(b4a.from('0'))
  await signers[0].append(b4a.from('1'))
  await signers[0].append(b4a.from('2'))
  await signers[0].append(b4a.from('3'))
  await signers[0].append(b4a.from('4'))
  await signers[0].append(b4a.from('5'))

  await signers[1].append(b4a.from('0'))
  await signers[1].append(b4a.from('1'))

  let len = signableLength([signers[0].length, signers[1].length], 2)

  t.is(len, 2)

  multisig1 = assemble([
    await partialCoreSignature(core, signers[0], len),
    await partialCoreSignature(core, signers[1], len)
  ])

  await signers[1].append(b4a.from('2'))
  await signers[1].append(b4a.from('3'))

  len = signableLength([signers[0].length, signers[1].length], 2)

  t.is(len, 4)

  multisig2 = assemble([
    await partialCoreSignature(core, signers[0], len),
    await partialCoreSignature(core, signers[1], len)
  ])

  const core2 = await create(t, { manifest })

  const s1 = core.replicate(true)
  const s2 = core2.replicate(false)

  s1.pipe(s2).pipe(s1)

  const p = new Promise((resolve, reject) => {
    s2.on('error', reject)
    core2.on('append', resolve)
  })

  core.append([
    b4a.from('0'),
    b4a.from('1')
  ], {
    signature: multisig1
  })

  await t.execution(p)

  t.is(core.length, 2)
  t.is(core2.length, 2)

  const p2 = new Promise((resolve, reject) => {
    s1.on('error', reject)
    core.on('append', resolve)
  })

  core2.append([
    b4a.from('2'),
    b4a.from('3')
  ], {
    signature: multisig2
  })

  await t.execution(p2)

  t.is(core.length, 4)
  t.is(core2.length, 4)
})

test('multisig - persist to disk', async function (t) {
  const dir = await tmpDir(t)
  const storage = await createStorage(t, dir)

  const signers = []
  for (let i = 0; i < 3; i++) signers.push(await create(t, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null

  const core = new Hypercore(storage, { manifest })
  await core.ready()

  await signers[0].append(b4a.from('0'))
  await signers[1].append(b4a.from('0'))

  const len = signableLength([signers[0].length, signers[1].length], 2)

  t.is(len, 1)

  const proof = await partialCoreSignature(core, signers[0], len)
  const proof2 = await partialCoreSignature(core, signers[1], len)

  multisig = assemble([proof, proof2])

  await t.execution(core.append(b4a.from('0'), { signature: multisig }))

  t.is(core.length, 1)

  await core.close()
  await storage.close()

  const reopened = new Hypercore(await createStorage(t, dir), { manifest })
  await t.execution(reopened.ready())

  const core2 = await create(t, { manifest })

  const s1 = reopened.replicate(true)
  const s2 = core2.replicate(false)

  const p = new Promise((resolve, reject) => {
    s1.on('error', reject)
    s2.on('error', reject)

    core2.on('append', resolve)
  })

  s1.pipe(s2).pipe(s1)

  await t.execution(p)

  t.is(core2.length, reopened.length)

  await core2.download({ start: 0, end: reopened.length }).downloaded()

  t.alike(await core2.get(0), b4a.from('0'))

  await reopened.close()
})

test('multisig - overlapping appends', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(await create(t, { compat: false }))

  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig1 = null
  let multisig2 = null

  const core = await create(t, { manifest })

  const core2 = await create(t, { manifest })
  await core.ready()

  await signers[0].append(b4a.from('0'))
  await signers[0].append(b4a.from('1'))
  await signers[0].append(b4a.from('2'))
  await signers[0].append(b4a.from('3'))
  await signers[0].append(b4a.from('4'))
  await signers[0].append(b4a.from('5'))

  await signers[1].append(b4a.from('0'))
  await signers[1].append(b4a.from('1'))

  await signers[2].append(b4a.from('0'))
  await signers[2].append(b4a.from('1'))
  await signers[2].append(b4a.from('2'))

  const len = signableLength([signers[0].length, signers[1].length, signers[2].length], 2)

  t.is(len, 3)

  multisig1 = assemble([
    await partialCoreSignature(core, signers[1], 2),
    await partialCoreSignature(core, signers[0], 2)
  ])

  multisig2 = assemble([
    await partialCoreSignature(core, signers[2], len),
    await partialCoreSignature(core, signers[0], len)
  ])

  await core.append([
    b4a.from('0'),
    b4a.from('1')
  ], {
    signature: multisig1
  })

  await core2.append([
    b4a.from('0'),
    b4a.from('1'),
    b4a.from('2')
  ], {
    signature: multisig2
  })

  t.is(core.length, 2)
  t.is(core2.length, 3)

  const s1 = core.replicate(true)
  const s2 = core2.replicate(false)

  s1.pipe(s2).pipe(s1)

  const p = new Promise((resolve, reject) => {
    s1.on('error', reject)
    s2.on('error', reject)
    core.on('append', resolve)
  })

  await t.execution(p)

  t.is(core.length, 3)
  t.is(core2.length, 3)
})

test('multisig - normal operating mode', async function (t) {
  const inputs = []

  for (let i = 0; i < 0xff; i++) inputs.push(b4a.from([i]))

  const signers = []
  signers.push(await create(t, { compat: false }))
  signers.push(await create(t, { compat: false }))
  signers.push(await create(t, { compat: false }))

  const [a, b, d] = signers

  await Promise.all(signers.map(s => s.ready()))
  const manifest = createMultiManifest(signers)

  const signer1 = signer(a, b)
  const signer2 = signer(b, d)

  const core = await create(t, { manifest })

  const core2 = await create(t, { manifest })
  await core.ready()

  let ai = 0
  let bi = 0
  let ci = 0

  const s1 = core.replicate(true)
  const s2 = core2.replicate(false)

  s1.pipe(s2).pipe(s1)

  t.teardown(() => {
    s1.destroy()
    s2.destroy()
  })

  s1.on('error', t.fail)
  s2.on('error', t.fail)

  while (true) {
    if (core.length === inputs.length && core2.length === inputs.length) break

    const as = Math.min(inputs.length, ai + 1 + Math.floor(Math.random() * 4))
    const bs = Math.min(inputs.length, bi + 1 + Math.floor(Math.random() * 4))
    const cs = Math.min(inputs.length, ci + 1 + Math.floor(Math.random() * 4))

    while (ai < as) await a.append(inputs[ai++])
    while (bi < bs) await b.append(inputs[bi++])
    while (ci < cs) await d.append(inputs[ci++])

    if (Math.random() < 0.5) {
      const m1s = Math.min(ai, bi)
      if (m1s <= core2.length) continue

      const p = new Promise(resolve => core2.once('append', resolve))

      core.append(inputs.slice(core.length, m1s), { signature: await signer1() })

      await p
    } else {
      const m2s = Math.min(bi, ci)
      if (m2s <= core.length) continue

      const p = new Promise(resolve => core.once('append', resolve))

      core2.append(inputs.slice(core2.length, m2s), { signature: await signer2() })

      await p
    }
  }

  t.is(core.length, inputs.length)
  t.is(core.length, core2.length)

  for (let i = 0; i < inputs.length; i++) {
    const l = await core.get(i)
    const r = await core2.get(i)

    if (!b4a.equals(l, r)) t.fail()
    if (l[0] !== i) t.fail()
  }

  t.pass()

  function signer (w1, w2) {
    return async (batch) => {
      const len = signableLength([w1.length, w2.length], 2)

      return assemble([
        await partialCoreSignature(core, w1, len),
        await partialCoreSignature(core, w2, len)
      ])
    }
  }
})

// Should take ~2s, but sometimes slow on CI machine, so lots of margin on timeout
test('multisig - large patches', { timeout: 120000 }, async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(await create(t, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null

  const core = await create(t, { manifest })

  for (let i = 0; i < 10000; i++) {
    await signers[0].append(b4a.from(i.toString(10)))
  }

  await signers[1].append(b4a.from('0'))

  let len = signableLength([signers[0].length, signers[1].length], 2)
  t.is(len, 1)

  const proof = await partialCoreSignature(core, signers[0], len)
  const proof2 = await partialCoreSignature(core, signers[1], len)

  multisig = assemble([proof, proof2])

  await t.execution(core.append(b4a.from('0'), { signature: multisig }))

  t.is(core.length, 1)

  const core2 = await create(t, { manifest })

  const s1 = core.replicate(true)
  const s2 = core2.replicate(false)

  const p = new Promise((resolve, reject) => {
    s1.on('error', reject)
    s2.on('error', reject)

    core2.on('append', resolve)
  })

  s1.pipe(s2).pipe(s1)

  await t.execution(p)

  t.is(core2.length, core.length)

  await core2.download({ start: 0, end: core.length }).downloaded()

  t.alike(await core2.get(0), b4a.from('0'))

  const batch = []
  for (let i = 1; i < 1000; i++) {
    batch.push(b4a.from(i.toString(10)))
    await signers[1].append(b4a.from(i.toString(10)))
  }

  for (let i = 0; i < 10000; i++) {
    await signers[0].append(b4a.from(i.toString(10)))
  }

  len = signableLength([signers[0].length, signers[1].length], 2)
  t.is(len, 1000)

  const proof3 = await partialCoreSignature(core, signers[0], len)
  const proof4 = await partialCoreSignature(core, signers[1], len)

  multisig = assemble([proof3, proof4])

  const p2 = new Promise((resolve, reject) => {
    s1.on('error', reject)
    s2.on('error', reject)

    core2.on('append', resolve)
  })

  await t.execution(core.append(batch, { signature: multisig }))

  t.is(core.length, 1000)

  await t.execution(p2)

  t.is(core2.length, core.length)
})

test('multisig - prologue', async function (t) {
  const signers = []
  for (let i = 0; i < 2; i++) signers.push(await create(t, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  await signers[0].append(b4a.from('0'))
  await signers[0].append(b4a.from('1'))

  const hash = b4a.from(signers[0].core.state.hash())

  const manifest = createMultiManifest(signers)
  const manifestWithPrologue = createMultiManifest(signers, { hash, length: 2 })

  let multisig = null

  const core = await create(t, { manifest })

  const prologued = await create(t, { manifest: manifestWithPrologue })
  await prologued.ready()

  await signers[1].append(b4a.from('0'))
  await signers[1].append(b4a.from('1'))

  const len = signableLength([signers[0].length, signers[1].length], 2)

  t.is(len, 2)

  {
    const proof = await partialCoreSignature(core, signers[0], 1)
    const proof2 = await partialCoreSignature(core, signers[1], 1)

    multisig = assemble([proof, proof2])
  }

  await t.execution(core.append(b4a.from('0'), { signature: multisig }))
  await t.exception(prologued.append(b4a.from('0'), { signature: multisig }))

  t.is(core.length, 1)
  t.is(prologued.length, 0)

  {
    const proof = await partialCoreSignature(core, signers[0], 2)
    const proof2 = await partialCoreSignature(core, signers[1], 2)

    multisig = assemble([proof, proof2])
  }

  await core.append(b4a.from('1'), { signature: multisig })
  await t.execution(prologued.append([b4a.from('0'), b4a.from('1')], { signature: multisig }))

  t.is(prologued.length, 2)
})

test('multisig - prologue replicate', async function (t) {
  const signers = []
  for (let i = 0; i < 2; i++) signers.push(await create(t, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  await signers[0].append(b4a.from('0'))
  await signers[0].append(b4a.from('1'))

  const hash = b4a.from(signers[0].core.state.hash())

  const manifest = createMultiManifest(signers, { hash, length: 2 })

  let multisig = null

  const core = await create(t, { manifest })

  const remote = await create(t, { manifest })
  await remote.ready()

  await signers[1].append(b4a.from('0'))
  await signers[1].append(b4a.from('1'))

  const proof = await partialCoreSignature(core, signers[0], 2)
  const proof2 = await partialCoreSignature(core, signers[1], 2)

  multisig = assemble([proof, proof2])

  await core.append([b4a.from('0'), b4a.from('1')], { signature: multisig })

  t.is(core.length, 2)
  t.is(remote.length, 0)

  const streams = replicate(core, remote, t)

  await new Promise((resolve, reject) => {
    streams[0].on('error', reject)
    streams[1].on('error', reject)

    remote.on('append', resolve)
  })

  t.is(remote.length, 2)
})

test('multisig - prologue verify hash', async function (t) {
  const signers = []
  for (let i = 0; i < 2; i++) signers.push(await create(t, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const s0 = signers[0]

  await s0.append(b4a.from('0'))
  await s0.append(b4a.from('1'))

  const hash = b4a.from(s0.core.state.hash())

  const manifest = createMultiManifest(signers, { hash, length: 2 })

  const core = await create(t, { manifest })

  t.is(core.length, 0)

  const batch = s0.core.storage.read()
  const p = await MerkleTree.proof(s0.state, batch, { upgrade: { start: 0, length: 2 } })
  batch.tryFlush()

  const proof = await p.settle()
  proof.upgrade.signature = null

  await t.execution(core.core.verify(proof))

  t.is(core.length, 2)

  const remote = await create(t, { manifest })
  await remote.ready()

  t.is(core.length, 2)
  t.is(remote.length, 0)

  const streams = replicate(core, remote, t)

  await new Promise((resolve, reject) => {
    streams[0].on('error', reject)
    streams[1].on('error', reject)

    remote.on('append', resolve)
  })

  t.is(remote.length, 2)
})

test('multisig - prologue morphs request', async function (t) {
  const signers = []

  let multisig = null

  for (let i = 0; i < 2; i++) signers.push(await create(t, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const [s0, s1] = signers

  await s0.append(b4a.from('0'))
  await s1.append(b4a.from('0'))

  await s0.append(b4a.from('1'))
  await s1.append(b4a.from('1'))

  await s0.append(b4a.from('2'))
  await s1.append(b4a.from('2'))

  await s0.append(b4a.from('3'))
  await s1.append(b4a.from('3'))

  const hash = b4a.from(s0.core.state.hash())
  const manifest = createMultiManifest(signers, { hash, length: 4 })

  const core = await create(t, { manifest })

  t.is(core.length, 0)

  const batch = s0.core.storage.read()
  const p = await MerkleTree.proof(s0.state, batch, { upgrade: { start: 0, length: 4 } })
  batch.tryFlush()

  const proof = await p.settle()
  proof.upgrade.signature = null

  await t.execution(core.core.verify(proof))

  t.is(core.length, 4)

  await s0.append(b4a.from('4'))
  await s1.append(b4a.from('4'))

  const proof2 = await partialCoreSignature(core, s0, 5)
  const proof3 = await partialCoreSignature(core, s1, 5)

  multisig = assemble([proof2, proof3])

  await core.append(b4a.from('4'), { signature: multisig })

  t.is(core.length, 5)

  const remote = await create(t, { manifest })
  await remote.ready()

  t.is(core.length, 5)
  t.is(remote.length, 0)

  const streams = replicate(core, remote, t)

  await new Promise((resolve, reject) => {
    streams[0].on('error', reject)
    streams[1].on('error', reject)

    remote.on('append', resolve)
  })

  unreplicate(streams)

  t.is(remote.length, 5)

  const rb = remote.core.storage.read()
  const rp = await MerkleTree.proof(remote.state, rb, { upgrade: { start: 0, length: 4 } })
  rb.tryFlush()

  await t.execution(rp.settle())
})

test('multisig - append/truncate before prologue', async function (t) {
  const signers = []
  for (let i = 0; i < 2; i++) signers.push(await create(t, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  await signers[0].append(b4a.from('0'))
  await signers[1].append(b4a.from('0'))

  await signers[0].append(b4a.from('1'))
  await signers[1].append(b4a.from('1'))

  const hash = b4a.from(signers[0].core.state.hash())
  const manifest = createMultiManifest(signers, { hash, length: 2 })

  let multisig = null
  let partialMultisig = null

  const core = await create(t, { manifest })

  const proof = await partialSignature(signers[0].core, 0, 2)
  const proof2 = await partialSignature(signers[1].core, 1, 2)

  multisig = assemble([proof, proof2])

  const partialProof = await partialSignature(signers[0].core, 0, 1)
  const partialProof2 = await partialSignature(signers[1].core, 1, 1)

  partialMultisig = assemble([partialProof, partialProof2])

  await t.exception(core.append([b4a.from('0')], { signature: partialMultisig }))
  await t.execution(core.append([b4a.from('0'), b4a.from('1')], { signature: multisig }))

  t.is(core.length, 2)

  await t.exception(core.truncate(1, { signature: partialMultisig }))
})

test('create verifier - default quorum', async function (t) {
  const keyPair = crypto.keyPair()
  const keyPair2 = crypto.keyPair()

  const namespace = b4a.alloc(32, 2)

  const manifest = {
    version: 0,
    signers: [{
      signature: 'ed25519',
      namespace,
      publicKey: keyPair.publicKey
    }]
  }

  // single v0
  t.is(Verifier.fromManifest(manifest).quorum, 1)

  // single v1
  manifest.version = 1
  t.is(Verifier.fromManifest(manifest).quorum, 1)

  manifest.signers.push({
    signature: 'ed25519',
    namespace,
    publicKey: keyPair2.publicKey
  })

  // multiple v0
  manifest.version = 0
  t.is(Verifier.fromManifest(manifest).quorum, 2)

  // multiple v1
  manifest.version = 1
  t.is(Verifier.fromManifest(manifest).quorum, 2)
})

test('manifest encoding', t => {
  const keyPair = crypto.keyPair()
  const keyPair2 = crypto.keyPair()

  const manifest = {
    version: 0,
    hash: 'blake2b',
    allowPatch: false,
    prologue: null,
    quorum: 1,
    signers: [{
      signature: 'ed25519',
      namespace: b4a.alloc(32, 1),
      publicKey: keyPair.publicKey
    }],
    linked: null,
    userData: null
  }

  t.alike(reencode(manifest), manifest)

  manifest.allowPatch = true
  t.alike(reencode(manifest), manifest)

  manifest.version = 1
  t.alike(reencode(manifest), manifest)

  manifest.allowPatch = false
  t.alike(reencode(manifest), manifest)

  // with prologue set
  manifest.prologue = { hash: b4a.alloc(32, 3), length: 4 }
  manifest.version = 1

  manifest.allowPatch = true
  t.alike(reencode(manifest), manifest)

  manifest.allowPatch = false
  t.alike(reencode(manifest), manifest)

  // add signer
  manifest.signers.push({
    signature: 'ed25519',
    namespace: b4a.alloc(32, 2),
    publicKey: keyPair2.publicKey
  })

  // reset
  manifest.version = 0
  manifest.prologue = null
  manifest.allowPatch = false
  manifest.quorum = 2

  t.alike(reencode(manifest), manifest)

  manifest.allowPatch = true
  t.alike(reencode(manifest), manifest)

  manifest.version = 1
  t.alike(reencode(manifest), manifest)

  manifest.allowPatch = false
  t.alike(reencode(manifest), manifest)

  // with prologue set
  manifest.prologue = { hash: b4a.alloc(32, 3), length: 4 }
  manifest.version = 1

  manifest.allowPatch = true
  t.alike(reencode(manifest), manifest)

  manifest.allowPatch = false
  t.alike(reencode(manifest), manifest)

  // now with partial quooum
  manifest.version = 0
  manifest.prologue = null
  manifest.quorum = 1

  t.alike(reencode(manifest), manifest)

  manifest.allowPatch = true
  t.alike(reencode(manifest), manifest)

  manifest.version = 1
  t.alike(reencode(manifest), manifest)

  manifest.allowPatch = false
  t.alike(reencode(manifest), manifest)

  // with prologue set
  manifest.prologue = { hash: b4a.alloc(32, 3), length: 4 }
  manifest.version = 1

  manifest.allowPatch = true
  t.alike(reencode(manifest), manifest)

  manifest.allowPatch = false
  t.alike(reencode(manifest), manifest)

  // with linked cores
  manifest.version = 2
  manifest.linked = [b4a.alloc(32, 4)]

  t.alike(reencode(manifest), manifest)

  manifest.userData = b4a.from([200])
  t.alike(reencode(manifest), manifest)

  function reencode (m) {
    return c.decode(enc.manifest, c.encode(enc.manifest, m))
  }
})

test('create verifier - open existing core with manifest', async function (t) {
  const keyPair = crypto.keyPair()

  const manifest = Verifier.createManifest({
    quorum: 1,
    signers: [{
      signature: 'ed25519',
      publicKey: keyPair.publicKey
    }]
  })

  const key = Verifier.manifestHash(manifest)

  const create = await createStored(t)
  const core = await create(key, { compat: false })
  await core.ready()

  t.is(core.manifest, null)
  t.is(core.core.header.manifest, null)
  t.alike(core.key, key)

  await core.close()

  manifest.signers[0].publicKey = b4a.alloc(32, 0)

  const wrongCore = await create(null, { manifest, compat: false })
  await t.exception(wrongCore.ready(), /STORAGE_CONFLICT/)

  manifest.signers[0].publicKey = keyPair.publicKey

  const manifestCore = await create(null, { manifest, compat: false })
  await manifestCore.ready()

  t.not(manifestCore.manifest, null)
  t.not(manifestCore.core.header.manifest, null)
  t.alike(manifestCore.key, key)

  await manifestCore.close()

  const compatCore = await create(null, { manifest, compat: true })
  await t.execution(compatCore.ready()) // compat flag is unset internally

  await compatCore.close()
})

function createMultiManifest (signers, prologue = null) {
  return {
    hash: 'blake2b',
    allowPatch: true,
    quorum: (signers.length >> 1) + 1,
    signers: signers.map(s => ({
      signature: 'ed25519',
      namespace: caps.DEFAULT_NAMESPACE,
      publicKey: s.manifest.signers[0].publicKey
    })),
    prologue,
    linked: []
  }
}

async function partialCoreSignature (core, s, len) {
  const sig = await core.core.verifier.sign(s.state.createTreeBatch(), s.keyPair)
  let index = 0
  for (; index < core.manifest.signers.length; index++) {
    if (b4a.equals(core.manifest.signers[index].publicKey, s.keyPair.publicKey)) break
  }
  const proof = await partialSignature(s.core, index, len, s.core.state.length, sig)
  return proof
}
