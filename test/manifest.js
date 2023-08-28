const test = require('brittle')
const c = require('compact-encoding')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const tmpDir = require('test-tmp')
const ram = require('random-access-memory')

const Hypercore = require('../')
const createAuth = require('../lib/manifest')

test('create auth - static signer', async function (t) {
  const treeHash = b4a.alloc(32, 1)

  const manifest = {
    version: 0,
    // namespace: <0x...>,
    // hash: BLAKE_2B,
    type: 'STATIC',
    static: {
      treeHash
    }
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
  const entropy = b4a.alloc(32, 3)

  const manifest = {
    version: 0,
    namespace,
    // hash: BLAKE_2B,
    type: 'SIGNER',
    signer: {
      signature: 'ED_25519',
      entropy,
      publicKey: keyPair.publicKey
    }
  }

  const auth = createAuth(manifest)

  t.ok(auth.verify)
  t.absent(auth.sign)

  const signable = b4a.alloc(32, 1)
  const namespaced = b4a.concat([namespace, entropy, signable])
  const signature = crypto.sign(namespaced, keyPair.secretKey)

  t.ok(auth.verify(signable, signature))

  signature[0] ^= 0xff

  t.absent(auth.verify(signable, signature))
})

test('create auth - single signer', async function (t) {
  const keyPair = crypto.keyPair()

  const namespace = b4a.alloc(32, 2)
  const entropy = b4a.alloc(32, 3)

  const manifest = {
    version: 0,
    namespace,
    // hash: BLAKE_2B,
    type: 'SIGNER',
    signer: {
      signature: 'ED_25519',
      entropy,
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
  const namespace = b4a.alloc(32, 2)
  const aEntropy = b4a.alloc(32, 3)
  const bEntropy = b4a.alloc(32, 4)

  const manifest = {
    version: 0,
    namespace,
    // hash: BLAKE_2B,
    type: 'MULTI_SIGNERS',
    multiSigners: {
      allowPatched: false,
      quorum: 2,
      signers: [{
        publicKey: a.publicKey,
        entropy: aEntropy,
        signature: 'ED_25519'
      }, {
        publicKey: b.publicKey,
        entropy: bEntropy,
        signature: 'ED_25519'        
      }]
    }
  }

  const namespaced = entropy => b4a.concat([namespace, entropy, signable])

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

  const entropy = b4a.alloc(32, 0)

  const manifest = {
    version: 0,
    // namespace,
    // hash: BLAKE_2B,
    type: 'SIGNER',
    signer: {
      signature: 'ED_25519',
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
    // namespace,
    // hash: BLAKE_2B,
    type: 'SIGNER',
    signer: {
      signature: 'ED_25519',
      publicKey: keyPair.publicKey
    }
  }

  t.exception(() => createAuth(manifest, { keyPair: keyPair2 }))
})

test('create auth - unsupported curve', async function (t) {
  const keyPair = crypto.keyPair()

  const manifest = {
    version: 0,
    // namespace,
    // hash: BLAKE_2B,
    type: 'SIGNER',
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
  const entropy = b4a.alloc(32, 3)

  const manifest = {
    version: 0,
    namespace,
    // hash: BLAKE_2B,
    type: 'SIGNER',
    signer: {
      signature: 'ED_25519',
      entropy,
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
