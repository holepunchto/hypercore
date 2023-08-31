const test = require('brittle')
const c = require('compact-encoding')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const tmpDir = require('test-tmp')
const ram = require('random-access-memory')

const aggregate = require('hypercore-crypto-multisig/lib/aggregate')

const Hypercore = require('../')
const { multisignature } = require('../lib/messages')
const createAuth = require('../lib/manifest')
const caps = require('../lib/caps')

test('create auth - static signer', async function (t) {
  const treeHash = b4a.alloc(32, 1)

  const manifest = {
    version: 0,
    // namespace: <0x...>,
    // hash: BLAKE_2B,
    static: treeHash
  }

  const auth = createAuth(manifest)

  t.ok(auth.verify)
  t.absent(auth.sign)

  const signable = b4a.alloc(32, 1)

  t.ok(auth.verify(signable))

  signable[0] ^= 0xff

  t.absent(auth.verify(signable))
})

test('create auth - single signer no sign', async function (t) {
  const keyPair = crypto.keyPair()

  const namespace = b4a.alloc(32, 2)

  const manifest = {
    version: 0,
    // hash: BLAKE_2B,
    signer: {
      signature: 'ed25519',
      namespace,
      publicKey: keyPair.publicKey
    }
  }

  const auth = createAuth(manifest)

  t.ok(auth.verify)
  t.absent(auth.sign)

  const signable = b4a.alloc(32, 1)
  const namespaced = b4a.concat([namespace, signable])
  const signature = crypto.sign(namespaced, keyPair.secretKey)

  t.ok(auth.verify(signable, signature))

  signature[0] ^= 0xff

  t.absent(auth.verify(signable, signature))
})

test('create auth - single signer', async function (t) {
  const keyPair = crypto.keyPair()

  const namespace = b4a.alloc(32, 2)

  const manifest = {
    version: 0,
    // hash: BLAKE_2B,
    signer: {
      signature: 'ed25519',
      namespace,
      publicKey: keyPair.publicKey
    }
  }

  const auth = createAuth(manifest, { keyPair })

  t.ok(auth.verify)
  t.ok(auth.sign)

  const signable = b4a.alloc(32, 1)
  const signature = auth.sign(signable)

  t.ok(auth.verify(signable, signature))

  signature[0] ^= 0xff

  t.absent(auth.verify(signable, signature))
})

test('create auth - multi signer', async function (t) {
  const a = crypto.keyPair()
  const b = crypto.keyPair()

  const signable = b4a.alloc(32, 1)
  const aEntropy = b4a.alloc(32, 2)
  const bEntropy = b4a.alloc(32, 3)

  const manifest = {
    version: 0,
    // hash: BLAKE_2B,
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

  const namespaced = entropy => b4a.concat([entropy, signable])

  const asig = crypto.sign(namespaced(aEntropy), a.secretKey)
  const bsig = crypto.sign(namespaced(bEntropy), b.secretKey)

  const enc = c.array(c.fixed64)

  const signature = c.encode(enc, [asig, bsig])
  const badSignature = c.encode(enc, [asig, asig])

  const auth = createAuth(manifest)

  t.ok(auth.verify)
  t.absent(auth.sign)

  t.ok(auth.verify(signable, signature))
  t.absent(auth.verify(signable, badSignature))
})

test('create auth - defaults', async function (t) {
  const keyPair = crypto.keyPair()

  const manifest = {
    version: 0,
    // hash: BLAKE_2B,
    signer: {
      signature: 'ed25519',
      publicKey: keyPair.publicKey
    }
  }

  const auth = createAuth(manifest, { keyPair })

  t.ok(auth.verify)
  t.ok(auth.sign)

  const signable = b4a.alloc(32, 1)
  const signature = auth.sign(signable)

  t.ok(auth.verify(signable, signature))

  signature[0] ^= 0xff

  t.absent(auth.verify(signable, signature))
})

test('create auth - invalid input', async function (t) {
  const keyPair = crypto.keyPair()
  const keyPair2 = crypto.keyPair()

  const manifest = {
    version: 0,
    // hash: BLAKE_2B,
    signer: {
      signature: 'ed25519',
      publicKey: keyPair.publicKey
    }
  }

  t.exception(() => createAuth(manifest, { keyPair: keyPair2 }))
})

test('create auth - unsupported curve', async function (t) {
  const keyPair = crypto.keyPair()

  const manifest = {
    version: 0,
    // hash: BLAKE_2B,
    signer: {
      signature: 'SECP_256K1',
      publicKey: keyPair.publicKey
    }
  }

  t.exception(() => createAuth(manifest))
})

test('create auth - compat signer', async function (t) {
  const keyPair = crypto.keyPair()

  const namespace = b4a.alloc(32, 2)

  const manifest = {
    version: 0,
    // hash: BLAKE_2B,
    signer: {
      signature: 'ed25519',
      namespace,
      publicKey: keyPair.publicKey
    }
  }

  const auth = createAuth(manifest, { keyPair, compat: true })

  t.ok(auth.verify)
  t.ok(auth.sign)

  const signable = b4a.alloc(32, 1)

  const signature = crypto.sign(signable, keyPair.secretKey)

  t.alike(auth.sign(signable), signature)
  t.ok(auth.verify(signable, signature))
})

test('multisig -  append', async t => {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null

  const core = new Hypercore(ram, { manifest, sign: () => multisig })
  await core.ready()

  await signers[0].append(b4a.from('0'))
  await signers[1].append(b4a.from('0'))

  const proof = await signature(signers[0], signers[1].length)
  const proof2 = await signature(signers[1])

  multisig = c.encode(multisignature, aggregate([proof, proof2], 2))

  await t.execution(core.append(b4a.from('0')))

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

test('multisig -  batch failed', async t => {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))

  await Promise.all(signers.map(s => s.ready()))

  let multisig = null

  const manifest = createMultiManifest(signers)

  const core = new Hypercore(ram, { manifest, sign: () => multisig })
  await core.ready()

  await signers[0].append(b4a.from('0'))
  await signers[1].append(b4a.from('0'))

  const proof = await signature(signers[0], signers[1].length)
  const proof2 = await signature(signers[1])

  multisig = c.encode(multisignature, aggregate([proof, proof2], 2))

  await t.execution(core.append(b4a.from('hello')))

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

test('multisig -  patches', async t => {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null
  const core = new Hypercore(ram, { manifest, sign: () => multisig })
  await core.ready()

  await signers[0].append(b4a.from('0'))
  await signers[0].append(b4a.from('1'))
  await signers[0].append(b4a.from('2'))
  await signers[0].append(b4a.from('3'))
  await signers[0].append(b4a.from('4'))

  await signers[1].append(b4a.from('0'))

  const proof = await signature(signers[0], signers[1].length)
  const proof2 = await signature(signers[1])

  multisig = c.encode(multisignature, aggregate([proof, proof2], 2))

  await t.execution(core.append(b4a.from('0')))

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

test('multisig -  batch append', async t => {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null
  const core = new Hypercore(ram, { manifest, sign: () => multisig })
  await core.ready()

  await signers[0].append(b4a.from('0'))
  await signers[0].append(b4a.from('1'))
  await signers[0].append(b4a.from('2'))
  await signers[0].append(b4a.from('3'))

  await signers[1].append(b4a.from('0'))
  await signers[1].append(b4a.from('1'))
  await signers[1].append(b4a.from('2'))
  await signers[1].append(b4a.from('3'))

  const proof = await signature(signers[0], signers[1].length)
  const proof2 = await signature(signers[1])

  multisig = c.encode(multisignature, aggregate([proof, proof2], 2))

  await t.execution(core.append([
    b4a.from('0'),
    b4a.from('1'),
    b4a.from('2'),
    b4a.from('3')
  ]))

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

test('multisig -  batch append with patches', async t => {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null
  const core = new Hypercore(ram, { manifest, sign: () => multisig })
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

  const proof = await signature(signers[0], signers[1].length)
  const proof2 = await signature(signers[1])

  multisig = c.encode(multisignature, aggregate([proof, proof2], 2))

  await t.execution(core.append([
    b4a.from('0'),
    b4a.from('1'),
    b4a.from('2'),
    b4a.from('3')
  ]))

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

test('multisig -  cannot divide batch', async t => {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null

  const core = new Hypercore(ram, { manifest, sign: () => multisig })
  await core.ready()

  await signers[0].append(b4a.from('0'))
  await signers[0].append(b4a.from('1'))
  await signers[0].append(b4a.from('2'))
  await signers[0].append(b4a.from('3'))

  await signers[1].append(b4a.from('0'))
  await signers[1].append(b4a.from('1'))
  await signers[1].append(b4a.from('2'))
  await signers[1].append(b4a.from('3'))

  const proof = await signature(signers[0], signers[1].length)
  const proof2 = await signature(signers[1])

  multisig = c.encode(multisignature, aggregate([proof, proof2], 2))

  await t.execution(core.append([
    b4a.from('0'),
    b4a.from('1')
  ]))

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

test('multisig -  multiple appends', async t => {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig1 = null
  let multisig2 = null

  const core = new Hypercore(ram, { manifest, sign: () => multisig1 })
  await core.ready()

  await signers[0].append(b4a.from('0'))
  await signers[0].append(b4a.from('1'))
  await signers[0].append(b4a.from('2'))
  await signers[0].append(b4a.from('3'))
  await signers[0].append(b4a.from('4'))
  await signers[0].append(b4a.from('5'))

  await signers[1].append(b4a.from('0'))
  await signers[1].append(b4a.from('1'))

  multisig1 = c.encode(multisignature, aggregate([
    await signature(signers[0], signers[1].length),
    await signature(signers[1])
  ], 2))

  await signers[1].append(b4a.from('2'))
  await signers[1].append(b4a.from('3'))

  multisig2 = c.encode(multisignature, aggregate([
    await signature(signers[0], signers[1].length),
    await signature(signers[1])
  ], 2))

  const core2 = new Hypercore(ram, { manifest, sign: () => multisig2 })

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
  ])

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
  ])

  await t.execution(p2)

  t.is(core.length, 4)
  t.is(core2.length, 4)
})

test('multisig -  persist to disk', async t => {
  const storage = await tmpDir(t)

  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null

  const core = new Hypercore(storage, { manifest, sign: () => multisig })
  await core.ready()

  await signers[0].append(b4a.from('0'))
  await signers[1].append(b4a.from('0'))

  const proof = await signature(signers[0], signers[1].length)
  const proof2 = await signature(signers[1])

  multisig = c.encode(multisignature, aggregate([proof, proof2], 2))

  await t.execution(core.append(b4a.from('0')))

  t.is(core.length, 1)

  await core.close()

  const clone = new Hypercore(storage, { manifest, sign: () => multisig })
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
})

test('multisig -  overlapping appends', async t => {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))

  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig1 = null
  let multisig2 = null

  const core = new Hypercore(ram, { manifest, sign: () => multisig1 })
  await core.ready()

  const core2 = new Hypercore(ram, { manifest, sign: () => multisig2 })
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

  multisig1 = c.encode(multisignature, aggregate([
    await signature(signers[1]),
    await signature(signers[0], signers[1].length)
  ], 2))

  multisig2 = c.encode(multisignature, aggregate([
    await signature(signers[2]),
    await signature(signers[0], signers[2].length)
  ], 2))

  await core.append([
    b4a.from('0'),
    b4a.from('1')
  ])

  await core2.append([
    b4a.from('0'),
    b4a.from('1'),
    b4a.from('2')
  ])

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

test('multisig - normal operating mode', async t => {
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

      await signer1.presign()
      core.append(inputs.slice(core.length, m1s))

      await p
    } else {
      const m2s = Math.min(bi, ci)
      if (m2s <= core.length) continue

      const p = new Promise(resolve => core.once('append', resolve))

      await signer2.presign()
      core2.append(inputs.slice(core2.length, m2s))

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
    let sig = null

    return {
      sign: () => sig,
      presign: async (batch) => {
        sig = c.encode(multisignature, aggregate([
          await signature(w1, w2.length),
          await signature(w2, w1.length)
        ], 2))
      }
    }
  }
})

test('multisig -  large patches', async t => {
  const signers = []
  for (let i = 0; i < 3; i++) signers.push(new Hypercore(ram, { compat: false }))
  await Promise.all(signers.map(s => s.ready()))

  const manifest = createMultiManifest(signers)

  let multisig = null

  const core = new Hypercore(ram, { manifest, sign: () => multisig })
  await core.ready()

  for (let i = 0; i < 10000; i++) {
    await signers[0].append(b4a.from(i.toString(10)))
  }

  await signers[1].append(b4a.from('0'))

  const proof = await signature(signers[0], signers[1].length)
  const proof2 = await signature(signers[1])

  multisig = c.encode(multisignature, aggregate([proof, proof2], 2))

  await t.execution(core.append(b4a.from('0')))

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

  const proof3 = await signature(signers[0], signers[1].length)
  const proof4 = await signature(signers[1])

  multisig = c.encode(multisignature, aggregate([proof3, proof4], 2))

  const p2 = new Promise((resolve, reject) => {
    s1.on('error', reject)
    s2.on('error', reject)

    core2.on('append', resolve)
  })

  await t.execution(core.append(batch))

  t.is(core.length, 1000)

  await t.execution(p2)

  t.is(core2.length, core.length)
})

async function signature (core, patch) {
  if (patch >= core.core.tree.length) patch = null

  return {
    length: core.core.tree.length,
    signature: b4a.from(core.core.tree.signature),
    patch: await upgrade(core, patch)
  }
}

async function upgrade (core, from) {
  if (!from && from !== 0) return null

  const tree = core.core.tree
  const p = await tree.proof({ upgrade: { start: from, length: tree.length - from } })
  return p.upgrade
}

function createMultiManifest (signers) {
  return {
    version: 0,
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
