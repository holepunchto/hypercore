const test = require('brittle')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')

const createAuth = require('../lib/auth')

test('create auth - single signer', async function (t) {
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

test('create auth - single signer', async function (t) {
  const keyPair = crypto.keyPair()

  const manifest = {
    version: 0,
    // namespace: <0x...>,
    // hash: BLAKE_2B,
    type: 'SIGNER',
    signer: {
      signature: 'ED_25519',
      // entropy: <0x...>,
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

test('create auth - mult signer', async function (t) {
  const a = crypto.keyPair()
  const b = crypto.keyPair()

  const manifest = {
    version: 0,
    // namespace: <0x...>,
    // hash: BLAKE_2B,
    type: 'MULTI_SIGNERS',
    multiSigners: {
      allowPatched: false,
      quorum: 2,
      signers: [{
        publicKey: a.publicKey,
        // entropy: <0x...>,
        signature: 'ED_25519'
      }, {
        publicKey: b.publicKey,
        // entropy: <0x...>,
        signature: 'ED_25519'        
      }]
    }
  }

  const signable = b4a.alloc(32, 1)

  const asig = crypto.sign(signable, a.secretKey)
  const bsig = crypto.sign(signable, b.secretKey)

  const signature = {
    proofs: [
      { signature: asig },
      { signature: bsig }
    ]
  }

  const badSignature = {
    proofs: [
      { signature: asig },
      { signature: asig }
    ]
  }

  const auth = createAuth(manifest)

  t.ok(auth.verify)
  t.absent(auth.sign)

  t.ok(auth.verify(signable, signature))
  t.absent(auth.verify(signable, badSignature))
})
