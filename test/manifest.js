const test = require('brittle')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const tmpDir = require('test-tmp')
const ram = require('random-access-memory')

const Hypercore = require('../')
const { assemble, partialSignature, signableLength } = require('../lib/multisig')
const { createVerifier, createManifest } = require('../lib/manifest')
const caps = require('../lib/caps')

// TODO: move this to be actual tree batches instead - less future surprises
// for now this is just to get the tests to work as they test important things
class AssertionTreeBatch {
  constructor (hash, signable) {
    this._hash = hash
    this._signable = signable
  }

  hash () {
    return this._hash
  }

  signable (ns) {
    return b4a.concat([ns, this._signable])
  }

  signableCompat () {
    return this._signable
  }
}

test('create verifier - static signer', async function (t) {
  const treeHash = b4a.alloc(32, 1)

  const manifest = {
    static: treeHash
  }

  const verifier = createVerifier(manifest)

  const batch = new AssertionTreeBatch(b4a.alloc(32, 1), null)

  t.ok(verifier.verify(batch))

  batch._hash[0] ^= 0xff

  t.absent(verifier.verify(batch))
})

test('create verifier - single signer no sign', async function (t) {
  const keyPair = crypto.keyPair()

  const namespace = b4a.alloc(32, 2)

  const manifest = {
    signer: {
      signature: 'ed25519',
      namespace,
      publicKey: keyPair.publicKey
    }
  }

  const verifier = createVerifier(manifest)

  const batch = new AssertionTreeBatch(null, b4a.alloc(32, 1))

  const signature = crypto.sign(batch.signable(namespace), keyPair.secretKey)

  t.ok(verifier.verify(batch, signature))

  signature[0] ^= 0xff

  t.absent(verifier.verify(batch, signature))
})

test('create verifier - single signer', async function (t) {
  const keyPair = crypto.keyPair()

  const namespace = b4a.alloc(32, 2)

  const manifest = {
    signer: {
      signature: 'ed25519',
      namespace,
      publicKey: keyPair.publicKey
    }
  }

  const verifier = createVerifier(manifest)

  const batch = new AssertionTreeBatch(null, b4a.alloc(32, 1))
  const signature = verifier.sign(batch, keyPair)

  t.ok(verifier.verify(batch, signature))

  signature[0] ^= 0xff

  t.absent(verifier.verify(batch, signature))
})

test('create verifier - multi signer', async function (t) {
  const a = crypto.keyPair()
  const b = crypto.keyPair()

  const signable = b4a.alloc(32, 1)
  const aEntropy = b4a.alloc(32, 2)
  const bEntropy = b4a.alloc(32, 3)

  const manifest = {
    multipleSigners: {
      allowPatched: false,
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
  }

  const batch = new AssertionTreeBatch(null, signable)

  const asig = crypto.sign(batch.signable(aEntropy), a.secretKey)
  const bsig = crypto.sign(batch.signable(bEntropy), b.secretKey)

  const signature = assemble([{ signer: 0, signature: asig }, { signer: 1, signature: bsig }])
  const badSignature = assemble([{ signer: 0, signature: asig }, { signer: 1, signature: asig }])
  const secondBadSignature = assemble([{ signer: 0, signature: asig }, { signer: 0, signature: asig }])
  const thirdBadSignature = assemble([{ signer: 0, signature: asig }])

  const verifier = createVerifier(manifest)

  t.ok(verifier.verify(batch, signature))
  t.absent(verifier.verify(batch, badSignature))
  t.absent(verifier.verify(batch, secondBadSignature))
  t.absent(verifier.verify(batch, thirdBadSignature))
})

test('create verifier - defaults', async function (t) {
  const keyPair = crypto.keyPair()

  const manifest = createManifest({
    signer: {
      signature: 'ed25519',
      publicKey: keyPair.publicKey
    }
  })

  const verifier = createVerifier(manifest)

  const batch = new AssertionTreeBatch(null, b4a.alloc(32, 1))
  const signature = verifier.sign(batch, keyPair)

  t.ok(verifier.verify(batch, signature))

  signature[0] ^= 0xff

  t.absent(verifier.verify(batch, signature))
})

test('create verifier - unsupported curve', async function (t) {
  t.plan(2)

  const keyPair = crypto.keyPair()

  const manifest = {
    signer: {
      signature: 'SECP_256K1',
      publicKey: keyPair.publicKey
    }
  }

  try {
    createManifest(manifest)
  } catch {
    t.pass('threw')
  }

  try {
    createVerifier(manifest)
  } catch {
    t.pass('also threw')
  }
})

test('create verifier - compat signer', async function (t) {
  const keyPair = crypto.keyPair()

  const namespace = b4a.alloc(32, 2)

  const manifest = {
    signer: {
      signature: 'ed25519',
      namespace,
      publicKey: keyPair.publicKey
    }
  }

  const verifier = createVerifier(manifest, { compat: true })

  const batch = new AssertionTreeBatch(null, b4a.alloc(32, 1))

  const signature = crypto.sign(batch.signableCompat(), keyPair.secretKey)

  t.alike(verifier.sign(batch, keyPair), signature)
  t.ok(verifier.verify(batch, signature))
})

test('multisig -  append', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null

  const core = new Hypercore(ram, { manifest })
  await core.ready()

  await signers[0].append(b4a.from('0'))
  await signers[1].append(b4a.from('0'))

  const len = signableLength([signers[0].length, signers[1].length], 2)

  t.is(len, 1)

  const proof = await partialSignature(signers[0].core.tree, 0, len)
  const proof2 = await partialSignature(signers[1].core.tree, 1, len)

  multisig = assemble([proof, proof2])

  await t.execution(core.append(b4a.from('0'), { signature: multisig }))

  t.is(core.length, 1)

  const core2 = new Hypercore(ram, { manifest })

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
})

test('multisig -  batch failed', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))

  await Promise.all(signers.map(s => s.ready()))

  let multisig = null

  const manifest = createMultiManifest(signers)

  const core = new Hypercore(ram, { manifest })
  await core.ready()

  await signers[0].append(b4a.from('0'))
  await signers[1].append(b4a.from('0'))

  const len = signableLength([signers[0].length, signers[1].length], 2)

  t.is(len, 1)

  const proof = await partialSignature(signers[0].core.tree, 0, len)
  const proof2 = await partialSignature(signers[1].core.tree, 1, len)

  multisig = assemble([proof, proof2])

  await t.execution(core.append(b4a.from('hello'), { signature: multisig }))

  const core2 = new Hypercore(ram, { manifest })

  const s1 = core.replicate(true)
  const s2 = core2.replicate(false)

  const p = new Promise((resolve, reject) => {
    s2.on('error', reject)

    setImmediate(resolve)
  })

  s1.pipe(s2).pipe(s1)

  await t.exception(p)

  t.is(core2.length, 0)
})

test('multisig -  patches', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null
  const core = new Hypercore(ram, { manifest })
  await core.ready()

  await signers[0].append(b4a.from('0'))
  await signers[0].append(b4a.from('1'))
  await signers[0].append(b4a.from('2'))
  await signers[0].append(b4a.from('3'))
  await signers[0].append(b4a.from('4'))

  await signers[1].append(b4a.from('0'))

  const len = signableLength([signers[0].length, signers[1].length], 2)

  t.is(len, 1)

  const proof = await partialSignature(signers[0].core.tree, 0, len)
  const proof2 = await partialSignature(signers[1].core.tree, 1, len)

  multisig = assemble([proof, proof2])

  await t.execution(core.append(b4a.from('0'), { signature: multisig }))

  t.is(core.length, 1)

  const core2 = new Hypercore(ram, { manifest })

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
})

test('multisig -  batch append', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null
  const core = new Hypercore(ram, { manifest })
  await core.ready()

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

  const proof = await partialSignature(signers[0].core.tree, 0, len)
  const proof2 = await partialSignature(signers[1].core.tree, 1, len)

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

  const core2 = new Hypercore(ram, { manifest })

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

test('multisig -  batch append with patches', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null
  const core = new Hypercore(ram, { manifest })
  await core.ready()

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

  const proof = await partialSignature(signers[0].core.tree, 0, len)
  const proof2 = await partialSignature(signers[1].core.tree, 1, len)

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

  const core2 = new Hypercore(ram, { manifest })

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

test('multisig -  cannot divide batch', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null

  const core = new Hypercore(ram, { manifest })
  await core.ready()

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

  const proof = await partialSignature(signers[0].core.tree, 0, len)
  const proof2 = await partialSignature(signers[1].core.tree, 1, len)

  multisig = assemble([proof, proof2])

  await t.execution(core.append([
    b4a.from('0'),
    b4a.from('1')
  ], {
    signature: multisig
  }))

  const core2 = new Hypercore(ram, { manifest })

  const s1 = core.replicate(true)
  const s2 = core2.replicate(false)

  const p = new Promise((resolve, reject) => {
    s1.on('error', reject)
    s2.on('error', reject)

    core2.on('append', resolve)
  })

  s1.pipe(s2).pipe(s1)

  await t.exception(p)

  t.is(core2.length, 0)
})

test('multisig -  multiple appends', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig1 = null
  let multisig2 = null

  const core = new Hypercore(ram, { manifest })
  await core.ready()

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
    await partialSignature(signers[0].core.tree, 0, len),
    await partialSignature(signers[1].core.tree, 1, len)
  ])

  await signers[1].append(b4a.from('2'))
  await signers[1].append(b4a.from('3'))

  len = signableLength([signers[0].length, signers[1].length], 2)

  t.is(len, 4)

  multisig2 = assemble([
    await partialSignature(signers[0].core.tree, 0, len),
    await partialSignature(signers[1].core.tree, 1, len)
  ])

  const core2 = new Hypercore(ram, { manifest })

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

test('multisig -  persist to disk', async function (t) {
  const storage = await tmpDir(t)

  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null

  const core = new Hypercore(storage, { manifest })
  await core.ready()

  await signers[0].append(b4a.from('0'))
  await signers[1].append(b4a.from('0'))

  const len = signableLength([signers[0].length, signers[1].length], 2)

  t.is(len, 1)

  const proof = await partialSignature(signers[0].core.tree, 0, len)
  const proof2 = await partialSignature(signers[1].core.tree, 1, len)

  multisig = assemble([proof, proof2])

  await t.execution(core.append(b4a.from('0'), { signature: multisig }))

  t.is(core.length, 1)

  await core.close()

  const clone = new Hypercore(storage, { manifest })
  await t.execution(clone.ready())

  const core2 = new Hypercore(ram, { manifest })

  const s1 = clone.replicate(true)
  const s2 = core2.replicate(false)

  const p = new Promise((resolve, reject) => {
    s1.on('error', reject)
    s2.on('error', reject)

    core2.on('append', resolve)
  })

  s1.pipe(s2).pipe(s1)

  await t.execution(p)

  t.is(core2.length, clone.length)

  await core2.download({ start: 0, end: clone.length }).downloaded()

  t.alike(await core2.get(0), b4a.from('0'))

  await clone.close()
})

test('multisig -  overlapping appends', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))

  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig1 = null
  let multisig2 = null

  const core = new Hypercore(ram, { manifest })
  await core.ready()

  const core2 = new Hypercore(ram, { manifest })
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
    await partialSignature(signers[1].core.tree, 0, 2),
    await partialSignature(signers[0].core.tree, 2, 2)
  ])

  multisig2 = assemble([
    await partialSignature(signers[2].core.tree, 2, len),
    await partialSignature(signers[0].core.tree, 0, len)
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
  signers.push(new Hypercore(ram, { compat: false }))
  signers.push(new Hypercore(ram, { compat: false }))
  signers.push(new Hypercore(ram, { compat: false }))

  const [a, b, d] = signers

  await Promise.all(signers.map(s => s.ready()))
  const manifest = createMultiManifest(signers)

  const signer1 = signer(a, b)
  const signer2 = signer(b, d)

  const core = new Hypercore(ram, { manifest, sign: signer1.sign })
  await core.ready()

  const core2 = new Hypercore(ram, { manifest, sign: signer2.sign })
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
    const a = signers.indexOf(w1)
    const b = signers.indexOf(w2)

    return async (batch) => {
      const len = signableLength([w1.length, w2.length], 2)

      return assemble([
        await partialSignature(w1.core.tree, a, len),
        await partialSignature(w2.core.tree, b, len)
      ])
    }
  }
})

test('multisig -  large patches', async function (t) {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null

  const core = new Hypercore(ram, { manifest })
  await core.ready()

  for (let i = 0; i < 10000; i++) {
    await signers[0].append(b4a.from(i.toString(10)))
  }

  await signers[1].append(b4a.from('0'))

  let len = signableLength([signers[0].length, signers[1].length], 2)
  t.is(len, 1)

  const proof = await partialSignature(signers[0].core.tree, 0, len)
  const proof2 = await partialSignature(signers[1].core.tree, 1, len)

  multisig = assemble([proof, proof2])

  await t.execution(core.append(b4a.from('0'), { signature: multisig }))

  t.is(core.length, 1)

  const core2 = new Hypercore(ram, { manifest })

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

  const proof3 = await partialSignature(signers[0].core.tree, 0, len)
  const proof4 = await partialSignature(signers[1].core.tree, 1, len)

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

function createMultiManifest (signers) {
  return {
    hash: 'blake2b',
    multipleSigners: {
      quorum: (signers.length >> 1) + 1,
      allowPatched: true,
      signers: signers.map(s => ({
        signature: 'ed25519',
        namespace: caps.DEFAULT_NAMESPACE,
        publicKey: s.manifest.signer.publicKey
      }))
    }
  }
}
